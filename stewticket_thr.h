// An Intel/AMD Hyper-threading friendly implementation of ticket locks with progressive back-off
// depending on where an acquirer is within the ticket queue, and futex based sleeping once an
// acquirer is far enough back in the queue to be unlikely to get the lock in a reasonable amount
// of time
//
// The ticket queue head and tail are updated with C11 atomics (<stdatomic.h>) instead of inline
// x86 assembly, so the implementation builds and runs on any architecture that provides atomic
// 32-bit integers.
//
// All ticket operations are seq_cst.  That is required for correctness of the futex handshake:
// a waiter bumps sleepers and then reads head, while the releaser bumps head and then reads
// sleepers.  With weaker orderings the two sides can each miss the other's update, leaving a
// waiter asleep with nobody left to wake it.
//
// Behaviour by load:
//
//   - Threads <= CPUs: the lock never sleeps and never issues a futex syscall.  Sleeping is
//     pointless when a core is free, as a sleep/wake round trip costs orders of magnitude more
//     than the few hundred nanoseconds a handoff takes.  In this regime the lock costs two
//     atomic read-modify-writes per lock/unlock pair when uncontended, and the same as a plain
//     spinning ticket lock when contended.
//
//   - Threads > CPUs: acquirers whose queue position cannot be reached by a running core go to
//     sleep on a futex word, which stops them from starving the lock holder of a core.  This
//     degrades gracefully instead of livelocking the way a pure spin lock does once the number
//     of acquirers exceeds the number of CPUs.

#include <stdint.h>
#include <stdatomic.h>
#include <stddef.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <sys/time.h>
#include <errno.h>
#include <limits.h>
#include <sched.h>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define	ticket_cpu_relax()	_mm_pause()
#else
#define	ticket_cpu_relax()	atomic_signal_fence(memory_order_seq_cst)
#endif

// Accumulated spin count after which an acquirer will sleep even though the machine is
// not oversubscribed.  This is only a safety valve for the case where the lock holder has
// been descheduled for an extended period - normal handoffs on a machine with CPUs to
// spare are handled by spinning, which is far cheaper than a sleep/wake round trip
#define TICKET_MAX_SPINS	20000

// Gather statistics about the number of calls, contentions, and spins.  Slows down locking by ~5-10%
//#define __TICKET_LOCK_STATISTICS

#define NUM_FUTEX_BITS	5
#define NUM_FUTEX_VARS (1 << NUM_FUTEX_BITS)
#define NUM_FUTEX_MASK (NUM_FUTEX_VARS - 1)
#define TKT_TO_FA(lk, idx)	&((lk)->futex_vars[(idx) & NUM_FUTEX_MASK])

typedef struct {
	struct __ticket {
		_Atomic uint32_t head;	// Ticket currently being served
		_Atomic uint32_t tail;	// Ticket to be handed to the next acquirer
	} tickets;
	volatile uint64_t	calls;
	volatile uint64_t	contentions;
	volatile uint64_t	spins;
	volatile uint64_t	futexes;
	uint64_t		pad0[3];
	// Number of acquirers that are asleep (or about to be) on a futex.  Kept on
	// its own cache line: it is read by every unlock but only written when an
	// acquirer actually sleeps, so it stays clean (and cheap to read) in the
	// common case where nobody ever sleeps
	_Atomic uint32_t	sleepers;
	uint32_t		pad1[15];
	_Atomic uint32_t	futex_vars[NUM_FUTEX_VARS];
} __attribute__ ((aligned(64))) ticketlock_t;

_Static_assert(offsetof(ticketlock_t, sleepers) == 64, "sleepers must be on its own cache line");
_Static_assert(offsetof(ticketlock_t, futex_vars) == 128, "futex_vars offset must not change");
_Static_assert(sizeof(ticketlock_t) == 256, "ticketlock_t size must not change");

#define TICKET_LOCK_INITIALIZER (ticketlock_t){0}
#define ticket_init(x)	  	(*(x) = TICKET_LOCK_INITIALIZER)

// Number of CPUs available to run lock acquirers, cached on first use.  A lost
// initialisation race just recomputes the same value
static inline int
ticket_num_cpus(void)
{
	static _Atomic int ncpu;
	int n = atomic_load_explicit(&ncpu, memory_order_relaxed);

	if (n == 0) {
		n = (int)sysconf(_SC_NPROCESSORS_ONLN);
		if (n < 1) {
			n = 1;
		}
		atomic_store_explicit(&ncpu, n, memory_order_relaxed);
	}
	return n;
}

static inline void ticket_lock(register ticketlock_t *lock)
{
	register uint32_t tkt_tail, tkt_head;
	register uint64_t spins = 0;
	register _Atomic uint32_t *fa;
	register int32_t ns;
	register int ncpu = ticket_num_cpus();

#ifdef __TICKET_LOCK_STATISTICS
	register uint64_t contentions = 0, futexes = 0;
#endif

	// Take the next ticket.  fetch_add returns the previous value of tail, which
	// is the ticket this acquirer now owns
	tkt_tail = atomic_fetch_add_explicit(&lock->tickets.tail, 1, memory_order_seq_cst);
	tkt_head = atomic_load_explicit(&lock->tickets.head, memory_order_seq_cst);

	// Sleep on the futex word associated with this ticket, so that the releaser
	// can find the right word to wake
	fa = TKT_TO_FA(lock, tkt_tail);

	while ((ns = (int32_t)(tkt_tail - tkt_head))) {
#ifdef __TICKET_LOCK_STATISTICS
		contentions++;
#endif
		// Only sleep when the queue ahead of us is deeper than the machine is
		// wide, i.e. when some of the acquirers ahead of us cannot be running
		// concurrently.  With CPUs to spare, a sleep/wake round trip costs
		// orders of magnitude more than spinning for the few hundred
		// nanoseconds it takes the current holder to finish.  spins is only a
		// safety valve for the case where the holder has genuinely been
		// descheduled, at which point spinning just burns a CPU it needs
		if ((ns > ncpu) || (spins >= TICKET_MAX_SPINS)) {
#ifdef __TICKET_LOCK_STATISTICS
			futexes++;
#endif
			// Advertise that we are (about to be) asleep so that the
			// releaser knows that a wakeup is needed.  Publish this
			// before re-reading head: a releaser that has already
			// advanced head past us must have seen sleepers != 0 and
			// signalled our futex word instead
			atomic_fetch_add_explicit(&lock->sleepers, 1, memory_order_seq_cst);
			if (tkt_tail != atomic_load_explicit(&lock->tickets.head, memory_order_seq_cst)) {
				(void)((syscall(SYS_futex, fa, (FUTEX_WAIT | FUTEX_PRIVATE_FLAG), 0, NULL, NULL, 0) == 0) ||
				       (errno == EAGAIN));
			}
			atomic_fetch_sub_explicit(&lock->sleepers, 1, memory_order_seq_cst);

			if (tkt_tail == atomic_load_explicit(&lock->tickets.head, memory_order_seq_cst)) {
				break;
			}

			// We weren't the thread that needed to wake up.  Yield to
			// the scheduler so the needed thread can wake up quickly
			sched_yield();
		} else {
			// Progressive back-off: cover roughly half the remaining
			// wait, leaving the cache line alone enough for the
			// holder to make progress.  A full ns pauses overshoots
			// badly on CPUs with a long PAUSE (AMD: ~64 cycles)
			ns /= 2;
			if (ns < 1) {
				ns = 1;
			}

			spins += (uint64_t)ns;
			for (ticket_cpu_relax(); --ns; ticket_cpu_relax());
		}
		tkt_head = atomic_load_explicit(&lock->tickets.head, memory_order_seq_cst);
	}
	// We have the lock.  Reset our futex word so that the releaser can signal
	// us, but only if it is actually set: writing it unconditionally bounces a
	// shared cache line on every single acquisition, which costs ~25% under
	// contention even when nobody has ever slept on this lock
	if (atomic_load_explicit(fa, memory_order_relaxed) != 0) {
		atomic_store_explicit(fa, 0, memory_order_relaxed);
	}

#ifdef __TICKET_LOCK_STATISTICS
	lock->calls++;
	lock->futexes += futexes;
	lock->contentions += contentions;
	lock->spins += spins;
#endif
}

static inline void ticket_unlock(register ticketlock_t *lock)
{
	register uint32_t new_head;

	new_head = atomic_fetch_add_explicit(&lock->tickets.head, 1, memory_order_seq_cst) + 1;

	// If nobody is queued behind us there is nothing to hand off and, crucially,
	// no reason to touch the futex word or issue a futex syscall
	if (new_head == atomic_load_explicit(&lock->tickets.tail, memory_order_seq_cst)) {
		return;
	}

	// Somebody is queued.  Only signal and wake if one of them is actually
	// asleep; otherwise the next holder is already spinning and will see head
	// when it next looks.  This is what keeps the lock syscall-free whenever
	// there are CPUs to spare
	if (atomic_load_explicit(&lock->sleepers, memory_order_seq_cst) != 0) {
		register _Atomic uint32_t *fa = TKT_TO_FA(lock, new_head);

		atomic_store_explicit(fa, 1, memory_order_relaxed);
		syscall(SYS_futex, fa, (FUTEX_WAKE | FUTEX_PRIVATE_FLAG), INT_MAX, NULL, NULL, 0);
	}
}
