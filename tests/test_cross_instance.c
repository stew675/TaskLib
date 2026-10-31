/* Regression: an API call for instance B from a worker of instance A used to
 * clobber the worker's instance pointer and corrupt memory. */
#include "task_lib.h"

static int32_t ti0, ti1;

static void tick(int64_t tfd, int64_t late, void *ud)
{
	(void)late; (void)ud;
	int64_t x = TASK_socket_create(ti1, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (x < 0) {
		TASK_instance_shutdown(ti0, NULL, NULL);
		return;
	}
	TASK_close(x);
	TASK_timeout_set(tfd, 1000, NULL, tick);
}

int main(void)
{
	ti0 = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	ti1 = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti0 < 0 || ti1 < 0) return 1;
	if (TASK_instance_start(ti0) < 0 || TASK_instance_start(ti1) < 0) return 1;

	int64_t t = TASK_timeout_create(ti0, 1000, NULL, tick);
	if (t < 0) return 1;

	sleep(1);
	TASK_instance_shutdown(ti0, NULL, NULL);
	TASK_instance_shutdown(ti1, NULL, NULL);
	TASK_instance_wait(ti0);
	TASK_instance_wait(ti1);
	TASK_instance_destroy(ti0);
	TASK_instance_destroy(ti1);
	return 0;
}
