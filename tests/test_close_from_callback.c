/* Regression coverage for closing a task from every callback type.  Close must
 * be deferred out of the event/callback dispatch, run the close callback once,
 * and never tear a task down inline while an event batch still references it. */
#include "task_lib.h"
#include <stdatomic.h>

static _Atomic int close_count;
static _Atomic int done;
static _Atomic int expected_closes;

static void
close_cb(int64_t tfd, void *data)
{
	(void)tfd;
	(void)data;
	atomic_fetch_add(&close_count, 1);
	if (atomic_load(&close_count) >= atomic_load(&expected_closes)) {
		atomic_store(&done, 1);
	}
}

static void
read_cb(int64_t tfd, void *buf, ssize_t result, void *data)
{
	(void)buf;
	(void)data;
	if (result > 0) {
		(void)TASK_close(tfd);
	}
}

static void
write_cb(int64_t tfd, const void *buf, ssize_t result, void *data)
{
	(void)buf;
	(void)data;
	if (result > 0) {
		(void)TASK_close(tfd);
	}
}

static void
accept_cb(int64_t tfd, void *data)
{
	if (tfd >= 0) {
		int64_t listener = (int64_t)(intptr_t)data;

		/* Accepted tasks no longer inherit the listener's close callback. */
		(void)TASK_socket_set_close_cb(tfd, NULL, close_cb);
		(void)TASK_close(tfd);
		(void)TASK_close(listener);
	}
}

static void
timer_cb(int64_t tfd, int64_t late, void *data)
{
	(void)late;
	(void)data;
	(void)TASK_close(tfd);
}

static int
wait_done(void)
{
	for (int i = 0; i < 500; i++) {
		if (atomic_load(&done)) return 0;
		usleep(10000);
	}
	return -1;
}

static void
reset(int expected)
{
	atomic_store(&close_count, 0);
	atomic_store(&done, 0);
	atomic_store(&expected_closes, expected);
}

static int32_t
new_inst(void)
{
	int32_t ti = TASK_instance_create(2, 0, 1000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
	if (ti >= 0 && TASK_instance_start(ti) < 0) return -1;
	return ti;
}

static void
destroy_inst(int32_t ti)
{
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
}

int
main(void)
{
	/* Read callback closes its own task. */
	{
		int32_t ti = new_inst();
		int sv[2];
		int64_t tfd;
		if (ti < 0) return 1;
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
		tfd = TASK_socket_register(ti, sv[0], NULL, close_cb);
		if (tfd < 0) return 1;
		reset(1);
		TASK_socket_read(tfd, (char[16]){0}, 16, TASK_TIMEOUT_ONE_SEC,
				 NULL, read_cb);
		if (write(sv[1], "x", 1) != 1) return 1;
		if (wait_done() < 0) return 1;
		usleep(50000);
		close(sv[0]);
		close(sv[1]);
		destroy_inst(ti);
	}

	/* Write callback closes its own task. */
	{
		int32_t ti = new_inst();
		int sv[2];
		int64_t tfd;
		char out[8] = "write";
		if (ti < 0) return 1;
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
		tfd = TASK_socket_register(ti, sv[0], NULL, close_cb);
		if (tfd < 0) return 1;
		reset(1);
		TASK_socket_write(tfd, out, sizeof(out), TASK_TIMEOUT_ONE_SEC,
				  NULL, write_cb);
		if (wait_done() < 0) return 1;
		usleep(50000);
		close(sv[0]);
		close(sv[1]);
		destroy_inst(ti);
	}

	/* Timer callback closes its own standalone timer task. */
	{
		int32_t ti = new_inst();
		if (ti < 0) return 1;
		reset(1);
		int64_t t = TASK_timeout_create(ti, 1000, NULL, timer_cb);
		if (t < 0) return 1;
		if (TASK_socket_set_close_cb(t, NULL, close_cb) < 0) return 1;
		if (wait_done() < 0) return 1;
		usleep(50000);
		destroy_inst(ti);
	}

	/* Accept callback closes the accepted child and the parent listener. */
	{
		int32_t ti = new_inst();
		int64_t listener;
		struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = 0 };
		a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

		if (ti < 0) return 1;
		listener = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0,
					      NULL, close_cb);
		if (listener < 0) return 1;
		if (TASK_socket_bind(listener, (struct sockaddr *)&a, sizeof(a)) < 0) {
			return 1;
		}
		if (TASK_socket_listen(listener, (void *)(intptr_t)listener,
				       accept_cb) < 0) {
			return 1;
		}

		int cfd = socket(AF_INET, SOCK_STREAM, 0);
		if (cfd < 0) return 1;
		socklen_t alen = sizeof(a);
		if (TASK_socket_get_fd(listener) < 0) return 1;
		if (getsockname(TASK_socket_get_fd(listener),
				(struct sockaddr *)&a, &alen) < 0) return 1;
		if (connect(cfd, (struct sockaddr *)&a, sizeof(a)) < 0) return 1;

		/* Parent close plus child close callbacks. */
		reset(2);
		if (wait_done() < 0) return 1;
		usleep(100000);
		close(cfd);
		destroy_inst(ti);
	}

	return 0;
}
