/* Regression: migrating a task with an armed timeout used to publish the new
 * worker before detaching the timer node from the old worker's paired heap.
 * A concurrent timer update or close could then corrupt the heap.  This test
 * also stresses the rule that only one migration may be in flight per task. */
#include "task_lib.h"
#include <pthread.h>

#define N		128
#define RUN_SECONDS	3

static int32_t ti;
static int64_t fds[N];
static _Atomic int stop;
static _Atomic unsigned long migrations;

static void
tm_cb(int64_t tfd, int64_t late, void *data)
{
	(void)tfd;
	(void)late;
	(void)data;
}

static void *
mover(void *arg)
{
	unsigned seed = (unsigned)(uintptr_t)arg;

	while (!__atomic_load_n(&stop, __ATOMIC_RELAXED)) {
		int i = rand_r(&seed) % N;
		int j = rand_r(&seed) % N;
		int64_t from = __atomic_load_n(&fds[i], __ATOMIC_RELAXED);
		int64_t to = __atomic_load_n(&fds[j], __ATOMIC_RELAXED);

		if ((from > 0) && (to > 0) && (from != to)) {
			if (TASK_migrate(to, from) == 0) {
				__atomic_add_fetch(&migrations, 1, __ATOMIC_RELAXED);
			}
			(void)TASK_timeout_set(from, (seed & 1) ? 1000 : 10000000,
					       NULL, tm_cb);
			if (seed & 2) {
				(void)TASK_timeout_cancel(from);
			}
		}
	}
	return NULL;
}

static void *
closer(void *arg)
{
	unsigned seed = (unsigned)(uintptr_t)arg;

	while (!__atomic_load_n(&stop, __ATOMIC_RELAXED)) {
		int i = rand_r(&seed) % N;
		int64_t old = __atomic_exchange_n(&fds[i], -1, __ATOMIC_RELAXED);

		if (old > 0) {
			(void)TASK_close(old);
			int64_t nfd = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
			if (nfd > 0) {
				__atomic_store_n(&fds[i], nfd, __ATOMIC_RELAXED);
			}
		}
		usleep(20);
	}
	return NULL;
}

int
main(void)
{
	pthread_t th[4];

	ti = TASK_instance_create(8, 0, 4000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	for (int i = 0; i < N; i++) {
		fds[i] = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
		if (fds[i] < 0) return 1;
	}

	pthread_create(&th[0], NULL, mover, (void *)(uintptr_t)1);
	pthread_create(&th[1], NULL, mover, (void *)(uintptr_t)2);
	pthread_create(&th[2], NULL, closer, (void *)(uintptr_t)3);
	pthread_create(&th[3], NULL, closer, (void *)(uintptr_t)4);

	sleep(RUN_SECONDS);
	__atomic_store_n(&stop, 1, __ATOMIC_RELAXED);
	for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);

	for (int i = 0; i < N; i++) {
		int64_t t = __atomic_exchange_n(&fds[i], -1, __ATOMIC_RELAXED);
		if (t > 0) (void)TASK_close(t);
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);

	return (__atomic_load_n(&migrations, __ATOMIC_RELAXED) > 0) ? 0 : 1;
}
