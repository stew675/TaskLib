/* Regression: lifecycle calls from the instance's own worker used to wait for
 * that same worker and deadlock.  They must return EDEADLK. */
#include "task_lib.h"
#include <signal.h>
#include <stdatomic.h>

static _Atomic int done;
static _Atomic int result;
static _Atomic int err;

static void
alarm_handler(int sig)
{
	(void)sig;
	_exit(2);
}

static void
timer_cb(int64_t tfd, int64_t late, void *data)
{
	int32_t ti = (int32_t)(intptr_t)data;

	(void)tfd;
	(void)late;
	errno = 0;
	int r = TASK_instance_destroy(ti);
	atomic_store(&result, r);
	atomic_store(&err, errno);
	atomic_store(&done, 1);
}

int
main(void)
{
	signal(SIGALRM, alarm_handler);
	alarm(20);

	int32_t ti = TASK_instance_create(2, 0, 1000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (TASK_timeout_create(ti, 10000, (void *)(intptr_t)ti, timer_cb) < 0) {
		return 1;
	}

	for (int i = 0; i < 500 && !atomic_load(&done); i++) {
		usleep(10000);
	}
	if (!atomic_load(&done)) return 1;
	if ((atomic_load(&result) != -1) || (atomic_load(&err) != EDEADLK)) {
		fprintf(stderr, "destroy from worker result=%d errno=%d\n",
			atomic_load(&result), atomic_load(&err));
		return 1;
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	if (TASK_instance_wait(ti) < 0) return 1;
	if (TASK_instance_destroy(ti) < 0) return 1;
	return 0;
}
