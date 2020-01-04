// An Intel/AMD Hyper-threading friendly implementation of ticket locks with progressive back-off
// depending on where an acquirer is within the ticket queue
//
// Author: Stew Forster (stew675@gmail.com)
//
// Ideas present within derived from multiple sources and many...many hours of testing

#ifndef __TICKET_LOCK_H_
#define __TICKET_LOCK_H_

#include <stdint.h>
#include <immintrin.h>

// Gather statistics about the number of calls, contentions, and spins.  Slows down locking by ~10%
// #define __TICKET_LOCK_STATISTICS

typedef struct {
	struct __ticket {
		volatile uint32_t head; // On little-endian architectures, head must appear before tail
		uint32_t	  tail; // On big-endian architectures, tail must appear before head
	} tickets;
#ifdef __TICKET_LOCK_STATISTICS
	volatile uint64_t	calls;
	volatile uint64_t	contentions;
	volatile uint64_t	spins;
#endif
} ticketlock_t;

#define TICKET_LOCK_INITIALIZER (ticketlock_t){0}
#define ticket_init(x)	  	(*(x) = TICKET_LOCK_INITIALIZER)

static inline void ticket_lock(register ticketlock_t *lock)
{
	struct __ticket tkt = { .tail = 1 };
#ifdef __TICKET_LOCK_STATISTICS
	register uint64_t contentions = 0, spins = 0;
#endif

	// Atomically adds tkt (with head = 0, tail = 1) to lock->tickets, and stores the result
	// back into tkt while generating an acquire style memory fence to the CPU bus
	asm __volatile__ ("lock xaddq %q0, %1\n" : "+r"(tkt), "+m"(lock->tickets) : : "memory", "cc");
	while (tkt.tail - tkt.head) {
#ifdef __TICKET_LOCK_STATISTICS
		contentions = 1;
#endif
		// It's slightly faster for the uncontested path to calculate tkt.head here
		tkt.head = tkt.tail - tkt.head;

		// It's also slightly faster when contesting to use > 2 than > 1
		if (tkt.head > 2) {
#ifdef __TICKET_LOCK_STATISTICS
			spins += tkt.head;
#endif
			for (_mm_pause(); --tkt.head; _mm_pause());
		} else {
#ifdef __TICKET_LOCK_STATISTICS
			spins++;
#endif
			_mm_pause();
		}
		tkt.head = lock->tickets.head;
	}
#ifdef __TICKET_LOCK_STATISTICS
	lock->calls++;
	lock->contentions += contentions;
	lock->spins += spins;
#endif
}

static inline void ticket_unlock(register ticketlock_t *lock)
{
	// Atomically adds 1 to lock->tickets.head while generating a release style memory fence
	asm __volatile__ ("lock addl %1, %0\n" : "+m"(lock->tickets.head) : "ri"(1) : "memory", "cc");
}
#endif
