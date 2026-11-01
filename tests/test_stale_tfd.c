/* Regression: tfd generation counters used to live in the task slots, so
 * destroying an instance and creating a new one at the same ti restarted the
 * generation at 1.  A stale handle from the old instance could then alias a
 * task in the replacement instance. */
#include "task_lib.h"

int
main(void)
{
	int32_t ti0, ti1;
	int64_t old_tfd, new_tfd;

	ti0 = TASK_instance_create(1, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti0 < 0) return 1;
	if (TASK_instance_start(ti0) < 0) return 1;

	old_tfd = TASK_socket_create(ti0, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (old_tfd < 0) return 1;

	TASK_instance_shutdown(ti0, NULL, NULL);
	if (TASK_instance_wait(ti0) < 0) return 1;
	if (TASK_instance_destroy(ti0) < 0) return 1;

	ti1 = TASK_instance_create(1, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti1 != ti0) return 1;	// The test relies on the instance slot being reused
	if (TASK_instance_start(ti1) < 0) return 1;

	new_tfd = TASK_socket_create(ti1, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (new_tfd < 0) return 1;

	/* The stale handle must be rejected even if the new task happens to share
	 * the same pool index (which it does in the deterministic allocator). */
	errno = 0;
	if (TASK_socket_get_fd(old_tfd) >= 0) {
		TASK_instance_shutdown(ti1, NULL, NULL);
		TASK_instance_wait(ti1);
		TASK_instance_destroy(ti1);
		return 1;
	}

	TASK_close(new_tfd);
	TASK_instance_shutdown(ti1, NULL, NULL);
	TASK_instance_wait(ti1);
	TASK_instance_destroy(ti1);
	return 0;
}
