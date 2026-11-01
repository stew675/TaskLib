/* Regression: TASK_instance_destroy() used to free the instance while other
 * threads were still inside task APIs that held a task reference.  Their
 * task_unlock()/task_maybe_free() could then dereference freed instance
 * memory.  Destruction now clears instances[ti] and drains API entry
 * references before freeing. */
#include "task_lib.h"
#include <stdatomic.h>
#include <pthread.h>

#define N	64

static int32_t ti;
static _Atomic int64_t fds[N];
static _Atomic int stop;

static void *
user(void *arg)
{
	unsigned seed = (unsigned)(uintptr_t)arg;

	while (!atomic_load_explicit(&stop, memory_order_relaxed)) {
		int i = rand_r(&seed) % N;
		int64_t t = atomic_load_explicit(&fds[i], memory_order_relaxed);

		if (t > 0) {
			/* These may fail while destruction is in progress, but they
			 * must never touch freed instance state. */
			(void)TASK_socket_get_fd(t);
			(void)TASK_timeout_set(t, 1000000, NULL, NULL);
			(void)TASK_socket_shutdown(t, SHUT_RD);
		}
		sched_yield();
	}
	return NULL;
}

int
main(void)
{
	pthread_t th[4];

	ti = TASK_instance_create(4, 0, 2000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	for (int i = 0; i < N; i++) {
		fds[i] = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
		if (fds[i] < 0) return 1;
	}

	for (int i = 0; i < 4; i++) {
		pthread_create(&th[i], NULL, user, (void *)(uintptr_t)(i + 1));
	}

	usleep(50000);

	/* Destroy while the users are still calling task APIs. */
	if (TASK_instance_destroy(ti) < 0) {
		atomic_store_explicit(&stop, 1, memory_order_relaxed);
		for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);
		return 1;
	}

	/* Stale handles must now fail without touching the freed instance. */
	for (int i = 0; i < N; i++) {
		if (TASK_socket_get_fd(fds[i]) >= 0) {
			atomic_store_explicit(&stop, 1, memory_order_relaxed);
			for (int j = 0; j < 4; j++) pthread_join(th[j], NULL);
			return 1;
		}
	}

	atomic_store_explicit(&stop, 1, memory_order_relaxed);
	for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);
	return 0;
}
