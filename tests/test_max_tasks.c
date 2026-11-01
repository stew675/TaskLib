/* Regression: max_tasks sizing arithmetic could wrap uint32_t and silently
 * create a tiny pool for an absurd request. */
#include "task_lib.h"
#include <stdint.h>

int
main(void)
{
	int32_t ti;

	errno = 0;
	ti = TASK_instance_create(1, 0, UINT32_MAX, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti >= 0) {
		TASK_instance_shutdown(ti, NULL, NULL);
		TASK_instance_wait(ti);
		TASK_instance_destroy(ti);
		fprintf(stderr, "UINT32_MAX max_tasks was accepted\n");
		return 1;
	}
	if ((errno != EINVAL) && (errno != ENOMEM) && (errno != EOVERFLOW)) {
		fprintf(stderr, "unexpected errno %d\n", errno);
		return 1;
	}

	errno = 0;
	if (TASK_instance_create(1, 0, 0, 131072, TASK_FLAGS_AFFINITY_DISABLE) >= 0) {
		return 1;
	}
	if (errno != EINVAL) return 1;

	return 0;
}
