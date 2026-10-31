/*
 * Lightweight lock-free atomic primitives for TaskLib.
 *
 * TaskLib used to depend on Concurrency Kit for the handful of ck_pr
 * operations it needed.  Those operations are simple enough to express with
 * the compiler's __atomic builtins (supported by GCC >= 4.7 and Clang >= 3.1),
 * which removes the external dependency at no cost: the generated code is
 * identical because ck_pr is itself a thin header-only wrapper over these
 * same compiler/architecture primitives.
 *
 * Memory ordering deliberately mirrors ck_pr:
 *
 *   - load/store use acquire/release ordering.  Task state is published from
 *     one thread and consumed by another, so a plain relaxed access is not
 *     sufficient on weakly ordered architectures.
 *   - inc/dec/add and the bit operations are sequentially consistent
 *     read-modify-write operations (full memory barriers).
 *
 * The variables these operate on are plain (non-_Atomic) types, matching the
 * previous behaviour, so they may still be read directly in places where a
 * torn/stale read is acceptable.
 *
 * Author: Stew Forster (stew675@gmail.com)
 */

#ifndef __TASK_ATOMICS_H_
#define __TASK_ATOMICS_H_

#include <stdint.h>

/* Acquire/release atomic load/store.  Works with any integer or pointer type. */
#define	task_atomic_load(p)		__atomic_load_n((p), __ATOMIC_ACQUIRE)
#define	task_atomic_store(p, v)		__atomic_store_n((p), (v), __ATOMIC_RELEASE)

/* Full-barrier atomic read-modify-write operations. */
#define	task_atomic_inc(p)		((void)__atomic_add_fetch((p), 1, __ATOMIC_SEQ_CST))
#define	task_atomic_dec(p)		((void)__atomic_sub_fetch((p), 1, __ATOMIC_SEQ_CST))
#define	task_atomic_add(p, v)		((void)__atomic_add_fetch((p), (v), __ATOMIC_SEQ_CST))
#define	task_atomic_sub(p, v)		((void)__atomic_sub_fetch((p), (v), __ATOMIC_SEQ_CST))

/* Atomic bit set/clear/test-and-set.  These are used for the per-task state
 * words that may be touched from both a worker and an API caller. */
#define	task_atomic_or(p, v)		((void)__atomic_fetch_or((p), (v), __ATOMIC_SEQ_CST))
#define	task_atomic_and(p, v)		((void)__atomic_fetch_and((p), (v), __ATOMIC_SEQ_CST))
#define	task_atomic_fetch_or(p, v)	__atomic_fetch_or((p), (v), __ATOMIC_SEQ_CST)
#define	task_atomic_fetch_and(p, v)	__atomic_fetch_and((p), (v), __ATOMIC_SEQ_CST)

#endif	// __TASK_ATOMICS_H_
