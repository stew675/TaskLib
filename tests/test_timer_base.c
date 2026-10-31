/* Regression: the coarse clock used to be primed on the thread creating the
 * instance, so long timers armed from that thread could fire immediately. */
#include "task_lib.h"

static int32_t ti;
static int64_t start;

static void fired(int64_t tfd, int64_t late, void *ud)
{
	(void)tfd; (void)late; (void)ud;
	TASK_instance_shutdown(ti, NULL, NULL);
}

int main(void)
{
	ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	sleep(7);
	start = TASK_get_us_time(NULL, true);
	if (TASK_timeout_create(ti, 6 * 1000000, NULL, fired) < 0) return 1;

	TASK_instance_wait(ti);
	int64_t elapsed = TASK_get_us_time(NULL, true) - start;
	TASK_instance_destroy(ti);
	return (elapsed >= 5 * 1000000) ? 0 : 1;
}
