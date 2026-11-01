/* Regression: APIs that take an instance index (TASK_socket_create,
 * TASK_timeout_create, TASK_socket_register, TASK_do_blocking_work) used to
 * race instance destruction and worker reaping.  They now take an instance
 * entry reference and shutdown drains those references before scanning or
 * stopping workers. */
#include "task_lib.h"
#include <pthread.h>
#include <stdatomic.h>

#define CREATORS	8
#define ROUNDS		300

static int32_t ti;
static _Atomic int stop;

static void *
creator(void *arg)
{
	(void)arg;

	while (!atomic_load_explicit(&stop, memory_order_relaxed)) {
		int64_t t = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);

		if (t > 0) {
			(void)TASK_close(t);
		}

		t = TASK_timeout_create(ti, 1000, NULL, NULL);
		if (t > 0) {
			(void)TASK_timeout_destroy(t);
		}
	}
	return NULL;
}

int
main(void)
{
	for (int round = 0; round < ROUNDS; round++) {
		pthread_t th[CREATORS];

		ti = TASK_instance_create(2, 0, 500, 131072, TASK_FLAGS_AFFINITY_DISABLE);
		if (ti < 0) return 1;
		if (TASK_instance_start(ti) < 0) return 1;

		atomic_store(&stop, 0);
		for (int i = 0; i < CREATORS; i++) {
			if (pthread_create(&th[i], NULL, creator, NULL) != 0) return 1;
		}

		usleep(50 + (round % 100));

		if (round & 1) {
			TASK_instance_shutdown(ti, NULL, NULL);
			if (TASK_instance_wait(ti) < 0) return 1;
			if (TASK_instance_destroy(ti) < 0) return 1;
		} else {
			if (TASK_instance_destroy(ti) < 0) return 1;
		}

		atomic_store(&stop, 1);
		for (int i = 0; i < CREATORS; i++) {
			if (pthread_join(th[i], NULL) != 0) return 1;
		}
	}
	return 0;
}
