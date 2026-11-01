/* Regression: close_cb must not run while an API reference is still held.
 * A foreign IO call holds such a reference until after its own callback, so a
 * close racing that call must defer its close callback until the API has
 * finished.  This is the white-box half of the callback-ordering contract. */
#include "task_internal.h"
#include <stdatomic.h>

static _Atomic int closed;

static void
close_cb(int64_t tfd, void *data)
{
	(void)tfd;
	(void)data;
	atomic_store(&closed, 1);
}

static int
wait_closed(void)
{
	for (int i = 0; i < 500; i++) {
		if (atomic_load(&closed)) {
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
	int64_t tfd;
	struct task *t;

	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	tfd = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, close_cb);
	if (tfd < 0) return 1;

	/* Simulate an in-flight API call: task_lookup takes the same API
	 * reference that TASK_socket_write()/read()/connect() take.  The extra
	 * task_ref() models the reference those APIs keep across their own
	 * callback so close_cb cannot overtake the callback. */
	t = task_lookup(tfd, FLG_WR);
	if (t == NULL) return 1;
	task_ref(t);

	if (TASK_close(tfd) < 0) return 1;

	usleep(30000);
	if (atomic_load(&closed)) {
		fprintf(stderr, "close_cb ran while an API reference was held\n");
		return 1;
	}

	/* Dropping the lookup reference is not enough: the API is still inside
	 * its callback.  This is the step that used to allow close_cb to run
	 * before a foreign API's error callback. */
	task_unlock(t, FLG_WR);
	usleep(30000);
	if (atomic_load(&closed)) {
		fprintf(stderr, "close_cb ran before the API callback reference\n");
		return 1;
	}

	task_unref(t);
	if (wait_closed() < 0) {
		fprintf(stderr, "close_cb did not run after all API references drained\n");
		return 1;
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
