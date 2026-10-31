/* Regression: APIs that fail validation from a non-owner thread used to
 * overflow the locked-action array and abort (or corrupt state with NDEBUG). */
#include "task_lib.h"

int main(void)
{
	int32_t ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	int64_t tfd = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (tfd < 0) return 1;

	for (int i = 0; i < 40; i++) {
		if (TASK_socket_get_fd(tfd) < 0) return 1;
	}
	for (int i = 0; i < 40; i++) {
		TASK_socket_read(tfd, NULL, 0, TASK_TIMEOUT_NEVER, NULL, NULL);
	}

	TASK_close(tfd);
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
