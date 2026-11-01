/* Regression: instance_shutdown_workers() used to move every worker to an
 * untracked local list and then drop the instance lock to notify them.  A
 * blocking worker finishing work in that window could rewrite stale list
 * links, stranding workers and hanging TASK_instance_wait(). */
#include "task_lib.h"
#include <pthread.h>
#include <stdatomic.h>
#include <signal.h>
#include <time.h>

#define WORKERS		64
#define ROUNDS		120

static _Atomic int64_t release_us;

static int64_t
now_us(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static void
work(void *data)
{
	int64_t release;

	(void)data;
	while (((release = atomic_load_explicit(&release_us, memory_order_relaxed)) == 0) ||
	       (now_us() < release)) {
	}
}

static void
work_cb(int32_t ti, void *data)
{
	(void)ti;
	(void)data;
}

static void
timeout_handler(int sig)
{
	(void)sig;
	_exit(2);
}

int
main(void)
{
	signal(SIGALRM, timeout_handler);
	alarm(120);

	for (int round = 0; round < ROUNDS; round++) {
		int32_t ti = TASK_instance_create(2, WORKERS, 2000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
		int n = 0;

		if (ti < 0) return 1;
		if (TASK_instance_start(ti) < 0) return 1;

		atomic_store(&release_us, 0);
		for (int i = 0; i < WORKERS; i++) {
			if (TASK_do_blocking_work(ti, NULL, work, NULL, work_cb) < 0) {
				break;
			}
			n++;
		}

		/* Let the workers finish somewhere during the shutdown walk. */
		atomic_store(&release_us, now_us() + (round % 400) + 1);

		TASK_instance_shutdown(ti, NULL, NULL);
		if (TASK_instance_wait(ti) < 0) return 1;
		if (TASK_instance_destroy(ti) < 0) return 1;

		if (n == 0) return 1;
	}
	return 0;
}
