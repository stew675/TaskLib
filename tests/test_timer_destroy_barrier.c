/* Regression: a timer callback could run after TASK_timeout_destroy()
 * returned (and even after the close callback had run).  Destroy is an
 * asynchronous request; the close callback is the barrier after which no
 * further timer callback for that task may start. */
#include "task_lib.h"
#include <stdatomic.h>

#define ROUNDS 5000

struct state {
	_Atomic int timeout_calls;
	_Atomic int close_calls;
	_Atomic int closed;
};

static void
timeout_cb(int64_t tfd, int64_t late, void *data)
{
	struct state *s = data;

	(void)tfd;
	(void)late;
	atomic_fetch_add(&s->timeout_calls, 1);
}

static void
close_cb(int64_t tfd, void *data)
{
	struct state *s = data;

	(void)tfd;
	atomic_fetch_add(&s->close_calls, 1);
	atomic_store(&s->closed, 1);
}

int
main(void)
{
	int32_t ti = TASK_instance_create(4, 0, 2000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	for (int i = 0; i < ROUNDS; i++) {
		struct state *s = calloc(1, sizeof(*s));
		int64_t t;

		if (s == NULL) return 1;
		t = TASK_timeout_create(ti, 0, s, timeout_cb);
		if (t < 0) return 1;
		if (TASK_socket_set_close_cb(t, s, close_cb) < 0) return 1;

		/* Destroy immediately.  The timer may already be running on the
		 * worker, but close_cb must be the last callback. */
		if (TASK_timeout_destroy(t) < 0) return 1;

		for (int n = 0; n < 500 && !atomic_load(&s->closed); n++) {
			usleep(1000);
		}
		if (!atomic_load(&s->closed)) {
			fprintf(stderr, "close barrier did not run at round %d\n", i);
			return 1;
		}

		int before = atomic_load(&s->timeout_calls);
		usleep(2000);
		int after = atomic_load(&s->timeout_calls);
		if (after != before) {
			fprintf(stderr, "timer fired after close barrier: %d -> %d\n",
				before, after);
			return 1;
		}
		if (atomic_load(&s->close_calls) != 1) {
			fprintf(stderr, "close callback ran %d times\n",
				atomic_load(&s->close_calls));
			return 1;
		}
		free(s);
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
