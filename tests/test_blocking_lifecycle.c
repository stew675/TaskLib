/* Regression: lifecycle calls from a blocking worker used to miss the
 * EDEADLK guard because __thr_current_worker is intentionally NULL on blocking
 * threads.  Destroying the instance from work_func() would wait for the worker
 * that is executing work_func() and deadlock. */
#include "task_lib.h"
#include <signal.h>
#include <stdatomic.h>

static _Atomic int work_result;
static _Atomic int work_errno;
static _Atomic int callback_seen;

static void
alarm_handler(int sig)
{
	(void)sig;
	_exit(2);
}

static void
work(void *data)
{
	int32_t ti = (int32_t)(intptr_t)data;

	errno = 0;
	int r = TASK_instance_destroy(ti);
	atomic_store(&work_result, r);
	atomic_store(&work_errno, errno);
}

static void
work_cb(int32_t ti, void *data)
{
	(void)ti;
	(void)data;
	atomic_store(&callback_seen, 1);
}

int
main(void)
{
	int32_t ti;

	signal(SIGALRM, alarm_handler);
	alarm(20);

	ti = TASK_instance_create(2, 1, 1000, 131072,
				  TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (TASK_do_blocking_work(ti, (void *)(intptr_t)ti, work, NULL,
				  work_cb) < 0) {
		return 1;
	}

	for (int i = 0; i < 1000 && !atomic_load(&callback_seen); i++) {
		usleep(10000);
	}
	if (!atomic_load(&callback_seen)) {
		fprintf(stderr, "blocking work callback did not run\n");
		return 1;
	}

	if ((atomic_load(&work_result) != -1) ||
	    (atomic_load(&work_errno) != EDEADLK)) {
		fprintf(stderr, "destroy from blocking worker result=%d errno=%d\n",
			atomic_load(&work_result), atomic_load(&work_errno));
		return 1;
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	if (TASK_instance_wait(ti) < 0) return 1;
	if (TASK_instance_destroy(ti) < 0) return 1;
	return 0;
}
