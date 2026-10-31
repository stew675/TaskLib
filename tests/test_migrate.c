/* Regression: TASK_migrate() used to accept a to_tfd from another instance
 * or a stale handle and select whatever task shared the pool index. */
#include "task_lib.h"

int main(void)
{
	int32_t ti0 = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	int32_t ti1 = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti0 < 0 || ti1 < 0) return 1;
	TASK_instance_start(ti0);
	TASK_instance_start(ti1);

	int64_t a = TASK_socket_create(ti0, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	int64_t b = TASK_socket_create(ti1, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	int64_t c = TASK_socket_create(ti0, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (a < 0 || b < 0 || c < 0) return 1;

	errno = 0;
	if (TASK_migrate(b, a) == 0 || errno != EINVAL) return 1;

	TASK_close(c);
	errno = 0;
	if (TASK_migrate(c, a) == 0 || errno != EBADF) return 1;

	TASK_close(a);
	TASK_close(b);
	TASK_instance_shutdown(ti0, NULL, NULL);
	TASK_instance_shutdown(ti1, NULL, NULL);
	TASK_instance_wait(ti0);
	TASK_instance_wait(ti1);
	TASK_instance_destroy(ti0);
	TASK_instance_destroy(ti1);
	return 0;
}
