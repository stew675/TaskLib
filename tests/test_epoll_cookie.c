/* White-box regression: epoll used to store raw struct task pointers, so a
 * stale event could be delivered to a recycled slot.  Epoll cookies are now
 * the task's generation-tagged TFD and are validated against the slot before
 * dispatch. */
#include "task_internal.h"

static int
wait_recycled(struct instance *i, uint32_t idx, int64_t old_tfd)
{
	for (int n = 0; n < 1000; n++) {
		struct task *t = i->tfd_pool + idx;
		if ((task_atomic_load(&t->tfd) != old_tfd) ||
		    (task_atomic_load(&t->state) == TASK_STATE_UNUSED) ||
		    (task_atomic_load(&t->state) == TASK_STATE_FREEING)) {
			return 0;
		}
		usleep(1000);
	}
	return -1;
}

int
main(void)
{
	int32_t ti = TASK_instance_create(2, 0, 1000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
	struct instance *i;
	int64_t old_tfd, new_tfd;
	uint32_t old_tfdi;
	struct task *t;

	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	old_tfd = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (old_tfd < 0) return 1;
	old_tfdi = TFD_TO_INDEX(old_tfd);
	i = instances[ti];
	if (i == NULL) return 1;

	/* A live cookie must resolve to the task. */
	t = worker_event_task(i, (uint64_t)old_tfd);
	if (t == NULL) return 1;
	task_unref(t);

	if (TASK_close(old_tfd) < 0) return 1;
	if (wait_recycled(i, old_tfdi, old_tfd) < 0) {
		fprintf(stderr, "task slot was not recycled\n");
		return 1;
	}

	/* The stale cookie must now be dropped. */
	t = worker_event_task(i, (uint64_t)old_tfd);
	if (t != NULL) {
		task_unref(t);
		fprintf(stderr, "stale cookie resolved after recycle\n");
		return 1;
	}

	/* Force the same slot to be reused and check its new cookie resolves. */
	new_tfd = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (new_tfd < 0) return 1;
	if (TFD_TO_INDEX(new_tfd) == old_tfdi) {
		t = worker_event_task(i, (uint64_t)new_tfd);
		if (t == NULL) {
			fprintf(stderr, "new generation cookie did not resolve\n");
			return 1;
		}
		task_unref(t);
		t = worker_event_task(i, (uint64_t)old_tfd);
		if (t != NULL) {
			task_unref(t);
			fprintf(stderr, "old generation cookie resolved to new task\n");
			return 1;
		}
	}

	(void)TASK_close(new_tfd);
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
