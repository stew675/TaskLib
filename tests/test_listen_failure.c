/* Regression: a failed TASK_socket_listen() used to leave the task marked as
 * a listener, making close cleanup and retry unsafe. */
#include "task_lib.h"

int main(void)
{
	int32_t ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	int64_t t = TASK_socket_create(ti, AF_INET, SOCK_DGRAM, 0, NULL, NULL);
	if (t < 0) return 1;

	errno = 0;
	if (TASK_socket_listen(t, NULL, NULL) != -1) return 1;

	errno = 0;
	if (TASK_socket_listen(t, NULL, NULL) != -1) return 1;
	if (errno == EBUSY) return 1;	/* state was not rolled back */

	if (TASK_close(t) < 0) return 1;

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
