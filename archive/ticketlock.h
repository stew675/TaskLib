// An Intel/AMD Hyper-threading friendly implementation of ticket locks with progressive back-off
// depending on where an acquirer is within the ticket queue
//
// Author: Stew Forster (stew675@gmail.com)
//
// Ideas present within derived from multiple sources and many...many hours of testing
//
// The head and tail of the ticket queue are updated with C11 atomics (<stdatomic.h>) instead
// of inline x86 assembly, so the implementation builds and runs on any architecture that
// provides atomic 32-bit integers.  The operations used are seq_cst read-modify-writes and
// seq_cst loads: the latter are required to avoid a missed futex wakeup (the waiter bumps
// tail and then reads head, while the releaser bumps head and then reads tail).

#ifndef __TICKET_LOCK_H_
#define __TICKET_LOCK_H_

#include <stdint.h>
#include <stdatomic.h>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#define	ticket_cpu_relax()	_mm_pause()
#else
#define	ticket_cpu_relax()	atomic_signal_fence(memory_order_seq_cst)
#endif

// Gather statistics about the number of calls, contentions, and spins.  Slows down locking by ~10%
// #define __TICKET_LOCK_STATISTICS

typedef struct {
	struct __ticket {
		_Atomic uint32_t head;	// Ticket currently being served
		_Atomic uint32_t tail;	// Ticket to be handed to the next acquirer
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
	register uint32_t tkt_tail, tkt_head;
#ifdef __TICKET_LOCK_STATISTICS
	register uint64_t contentions = 0, spins = 0;
#endif

	// Take the next ticket.  fetch_add returns the previous value of tail, which
	// is the ticket this acquirer now owns
	tkt_tail = atomic_fetch_add_explicit(&lock->tickets.tail, 1, memory_order_seq_cst);
	tkt_head = atomic_load_explicit(&lock->tickets.head, memory_order_acquire);
	while (tkt_tail - tkt_head) {
#ifdef __TICKET_LOCK_STATISTICS
		contentions = 1;
#endif
		// It's slightly faster for the uncontested path to calculate the queue
		// distance here
		tkt_head = tkt_tail - tkt_head;

		// It's also slightly faster when contesting to use > 2 than > 1
		if (tkt_head > 2) {
#ifdef __TICKET_LOCK_STATISTICS
			spins += tkt_head;
#endif
			for (ticket_cpu_relax(); --tkt_head; ticket_cpu_relax());
		} else {
#ifdef __TICKET_LOCK_STATISTICS
			spins++;
#endif
			ticket_cpu_relax();
		}
		tkt_head = atomic_load_explicit(&lock->tickets.head, memory_order_acquire);
	}
#ifdef __TICKET_LOCK_STATISTICS
	lock->calls++;
	lock->contentions += contentions;
	lock->spins += spins;
#endif
}

static inline void ticket_unlock(register ticketlock_t *lock)
{
	// Hand the lock to the next ticket in the queue.  The release ordering
	// publishes everything done inside the critical section to the next owner,
	// which pairs with the acquire loads in ticket_lock()
	atomic_fetch_add_explicit(&lock->tickets.head, 1, memory_order_release);
}
#endif
