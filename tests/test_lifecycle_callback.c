/* Regression: TASK_instance_destroy() held creation_lock across the worker
 * wait.  A callback on a worker of the instance being destroyed that tried to
 * create another instance deadlocked.  Instance creation now only uses the
 * global lock to reserve an empty slot. */
#include "task_lib.h"
#include <signal.h>
#include <stdatomic.h>

static _Atomic int entered;
static _Atomic int created_ti;
static int32_t ti_a;

static void
alarm_handler(int sig)
{
	(void)sig;
	_exit(2);
}

static void
timer_cb(int64_t tfd, int64_t late, void *data)
{
	(void)tfd;
	(void)late;
	(void)data;

	atomic_store(&entered, 1);
	/* Give the main thread time to enter TASK_instance_destroy() while this
	 * callback is still running on a worker of instance A. */
	usleep(300000);

	int32_t b = TASK_instance_create(2, 0, 500, 131072,
					 TASK_FLAGS_AFFINITY_DISABLE);
	atomic_store(&created_ti, b);
}

int
main(void)
{
	signal(SIGALRM, alarm_handler);
	alarm(20);

	ti_a = TASK_instance_create(2, 0, 1000, 131072,
				    TASK_FLAGS_AFFINITY_DISABLE);
	if (ti_a < 0) return 1;
	if (TASK_instance_start(ti_a) < 0) return 1;

	int64_t t = TASK_timeout_create(ti_a, 10000, NULL, timer_cb);
	if (t < 0) return 1;

	for (int i = 0; i < 500 && !atomic_load(&entered); i++) {
		usleep(10000);
	}
	if (!atomic_load(&entered)) return 1;

	if (TASK_instance_destroy(ti_a) < 0) return 1;

	int32_t ti_b = atomic_load(&created_ti);
	if (ti_b < 0) {
		fprintf(stderr, "instance creation from callback failed\n");
		return 1;
	}
	TASK_instance_shutdown(ti_b, NULL, NULL);
	if (TASK_instance_wait(ti_b) < 0) return 1;
	if (TASK_instance_destroy(ti_b) < 0) return 1;
	return 0;
}
