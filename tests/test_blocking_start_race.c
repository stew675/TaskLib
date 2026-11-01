/* Regression: TASK_do_blocking_work() could insert a new created worker into
 * workers_created while TASK_instance_start() was walking that list.  The
 * worker could be missed, leaving its queued work permanently unstarted. */
#include "task_lib.h"
#include <pthread.h>
#include <stdatomic.h>

#define ROUNDS 120

static int32_t ti;
static _Atomic int worked;
static _Atomic int start_go;

static void
work(void *data)
{
	(void)data;
}

static void
work_cb(int32_t t, void *data)
{
	(void)t;
	(void)data;
	atomic_fetch_add(&worked, 1);
}

static void *
submitter(void *arg)
{
	(void)arg;
	while (!atomic_load(&start_go)) {
		sched_yield();
	}
	(void)TASK_do_blocking_work(ti, NULL, work, NULL, work_cb);
	return NULL;
}

int
main(void)
{
	for (int round = 0; round < ROUNDS; round++) {
		pthread_t th;

		ti = TASK_instance_create(2, 1, 1000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
		if (ti < 0) return 1;

		atomic_store(&worked, 0);
		atomic_store(&start_go, 0);
		if (pthread_create(&th, NULL, submitter, NULL) != 0) return 1;

		atomic_store(&start_go, 1);
		if (TASK_instance_start(ti) < 0) return 1;
		if (pthread_join(th, NULL) != 0) return 1;

		for (int i = 0; i < 300 && atomic_load(&worked) == 0; i++) {
			usleep(1000);
		}
		if (atomic_load(&worked) == 0) {
			fprintf(stderr, "round %d: queued blocking work never ran\n",
				round);
			return 1;
		}

		TASK_instance_shutdown(ti, NULL, NULL);
		if (TASK_instance_wait(ti) < 0) return 1;
		if (TASK_instance_destroy(ti) < 0) return 1;
	}
	return 0;
}
