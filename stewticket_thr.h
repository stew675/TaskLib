// An Intel/AMD Hyper-threading friendly implementation of ticket locks with progressive back-off
// depending on where an acquirer is within the ticket queue

#include <immintrin.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <sys/time.h>
#include <errno.h>
#include <limits.h>
#include <sched.h>

#define TICKET_MAX_SPINS	200		// appears to be the best compromise

// Gather statistics about the number of calls, contentions, and spins.  Slows down locking by ~5-10%
//#define __TICKET_LOCK_STATISTICS

#define NUM_FUTEX_BITS	5
#define NUM_FUTEX_VARS (1 << NUM_FUTEX_BITS)
#define NUM_FUTEX_MASK (NUM_FUTEX_VARS - 1)
#define TKT_TO_FA(lk, idx)	&((lk)->futex_vars[(idx) & NUM_FUTEX_MASK])

typedef struct {
	struct __ticket {
		volatile uint32_t head; // On little-endian architectures, head must appear before tail
		uint32_t	  tail; // On big-endian architectures, tail must appear before head
	} tickets;
	volatile uint64_t	calls;
	volatile uint64_t	contentions;
	volatile uint64_t	spins;
	volatile uint64_t	futexes;
	uint64_t		padding[11];
	uint32_t		futex_vars[NUM_FUTEX_VARS];
} __attribute__ ((aligned(64))) ticketlock_t;

#define TICKET_LOCK_INITIALIZER (ticketlock_t){0}
#define ticket_init(x)	  	(*(x) = TICKET_LOCK_INITIALIZER)

static inline void ticket_lock(register ticketlock_t *lock)
{
	struct __ticket tkt = { .tail = 1 };
	register uint64_t spins = 0;
	register uint32_t *fa;
	register int32_t ns;

#ifdef __TICKET_LOCK_STATISTICS
	register uint64_t contentions = 0, futexes = 0;
#endif

	// Atomically adds tkt (with head = 0, tail = 1) to lock->tickets, and stores the result
	// back into tkt while generating an acquire style memory fence to the CPU bus
	asm __volatile__ ("lock xaddq %q0, %1\n" : "+r"(tkt), "+m"(lock->tickets) : : "memory", "cc");
	fa = TKT_TO_FA(lock, tkt.tail);

	while ((ns = (tkt.tail - tkt.head))) {
#ifdef __TICKET_LOCK_STATISTICS
		contentions++;
#endif
		// If we've spinlocked enough, wait on a futex instead
		// Also futex wait if we're not likely to get CPU soon
		if ((ns > 7) || (spins >= TICKET_MAX_SPINS)) {
#ifdef __TICKET_LOCK_STATISTICS
			futexes++;
#endif
			if ((syscall(SYS_futex, fa, (FUTEX_WAIT | FUTEX_PRIVATE_FLAG), 0, NULL, NULL, 0) == 0) ||
			    (errno == EAGAIN)) {
				if (tkt.tail == lock->tickets.head) {
					break;
				}
			}

			// We weren't the thread that needed to wake up.  Yield to
			// the scheduler so the needed thread can wake up quickly
			sched_yield();
		} else {
			if (ns > TICKET_MAX_SPINS) {
				ns = TICKET_MAX_SPINS;
			}

			spins += ns;
			for (_mm_pause(); --ns; _mm_pause());
		}
		tkt.head = lock->tickets.head;
	}
	// We have the lock.  First order of business is to set the futex addr
	// to zero to ensure any other threads contending on the same addr will
	// block within FUTEX_WAIT.  We don't acquire memory fences here because
	// the futex syscall does that for us, and will ensure synchronisation
	// for the times it's needed
	*fa = 0;

#ifdef __TICKET_LOCK_STATISTICS
	lock->calls++;
	lock->futexes += futexes;
	lock->contentions += contentions;
	lock->spins += spins;
#endif
}

static inline void ticket_unlock(register ticketlock_t *lock)
{
	struct __ticket tkt = { .head = 1 };
	register uint32_t new_head;

	// Atomically adds 1 to lock->tickets.head while generating an acquire style memory fence
	asm __volatile__ ("lock xaddq %q0, %1\n" : "+r"(tkt), "+m"(lock->tickets) : : "memory", "cc");

	new_head = tkt.head + 1;
	if (new_head != tkt.tail) {
		register uint32_t *fa = TKT_TO_FA(lock, new_head);

		// Another thread entered the lock before we released it.  Generate a futex
		// wakeup to unblock all threads waiting on the next futex addr.
		*fa = 1;
		syscall(SYS_futex, fa, (FUTEX_WAKE | FUTEX_PRIVATE_FLAG), INT_MAX, NULL, NULL, 0);
	}
}
