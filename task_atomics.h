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
 *   - load/store are relaxed.  ck_pr's load and store are a plain access
 *     behind a compiler barrier, not acquire/release operations.
 *   - inc/dec/add are sequentially consistent read-modify-write operations,
 *     i.e. full memory barriers, as ck_pr's arithmetic operations are.
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

/* Relaxed atomic load/store.  Works with any integer or pointer type. */
#define	task_atomic_load(p)		__atomic_load_n((p), __ATOMIC_RELAXED)
#define	task_atomic_store(p, v)		__atomic_store_n((p), (v), __ATOMIC_RELAXED)

/* Full-barrier atomic read-modify-write operations. */
#define	task_atomic_inc(p)		((void)__atomic_add_fetch((p), 1, __ATOMIC_SEQ_CST))
#define	task_atomic_dec(p)		((void)__atomic_sub_fetch((p), 1, __ATOMIC_SEQ_CST))
#define	task_atomic_add(p, v)		((void)__atomic_add_fetch((p), (v), __ATOMIC_SEQ_CST))

#endif	// __TASK_ATOMICS_H_
