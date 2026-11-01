/* Regression: a blocking-work callback queued before TASK_instance_start()
 * could be lost because the instance worker was assigned afterwards. */
#include "task_lib.h"

static int done;

static void work(void *d) { (void)d; }
static void work_cb(int32_t ti, void *d) { (void)ti; (void)d; task_atomic_store(&done, 1); }

int main(void)
{
	int32_t ti = TASK_instance_create(2, 1, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_do_blocking_work(ti, NULL, work, NULL, work_cb) < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	for (int i = 0; i < 200 && !task_atomic_load(&done); i++) usleep(10000);
	int ok = task_atomic_load(&done);

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return ok ? 0 : 1;
}
