/* Exercise the actual migration path, including the target-worker completion
 * notification, rather than only TASK_migrate() argument validation. */
#include "task_lib.h"

int main(void)
{
	int32_t ti = TASK_instance_create(4, 0, 2000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	int64_t fds[8];
	int n = 0, migrated = 0;
	for (; n < 8; n++) {
		fds[n] = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
		if (fds[n] < 0) break;
	}

	for (int i = 0; i < n && !migrated; i++) {
		for (int j = 0; j < n; j++) {
			if (i == j) continue;
			errno = 0;
			if (TASK_migrate(fds[j], fds[i]) == 0) {
				migrated = 1;
				break;
			}
			if (errno != EALREADY) return 1;
		}
	}
	if (n > 1 && !migrated) return 1;

	usleep(100000);
	for (int i = 0; i < n; i++) TASK_close(fds[i]);
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
