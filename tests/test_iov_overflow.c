/* Regression: the total length of an iovec was accumulated in size_t without
 * an overflow check, so an overflowing vector could be handed to writev() as
 * a tiny/mismatched length after task state had already been mutated. */
#include "task_lib.h"
#include <stdint.h>
#include <stdatomic.h>

static int32_t ti;
static _Atomic int done;
static _Atomic ssize_t result;
static _Atomic int err;

static void
io_cb(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t r, void *data)
{
	(void)tfd; (void)iov; (void)iovcnt; (void)data;
	atomic_store(&result, r);
	atomic_store(&err, errno);
	atomic_store(&done, 1);
}

static void
buf_cb(int64_t tfd, const void *buf, ssize_t r, void *data)
{
	(void)tfd; (void)buf; (void)data;
	atomic_store(&result, r);
	atomic_store(&err, errno);
	atomic_store(&done, 1);
}

static int
wait_done(void)
{
	for (int i = 0; i < 200; i++) {
		if (atomic_load(&done)) return 0;
		usleep(10000);
	}
	return -1;
}

int
main(void)
{
	int sv[2];
	int64_t tfd;
	static char b[8];
	struct iovec iov[2];

	ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
	tfd = TASK_socket_register(ti, sv[0], NULL, NULL);
	if (tfd < 0) return 1;

	iov[0].iov_base = b;
	iov[0].iov_len = SIZE_MAX;
	iov[1].iov_base = b;
	iov[1].iov_len = SIZE_MAX;

	atomic_store(&done, 0);
	errno = 0;
	TASK_socket_writev(tfd, iov, 2, TASK_TIMEOUT_ONE_SEC, NULL, io_cb);
	if (wait_done() < 0) return 1;
	if ((atomic_load(&result) != -1) ||
	    ((atomic_load(&err) != EINVAL) && (atomic_load(&err) != EOVERFLOW))) {
		fprintf(stderr, "writev overflow result=%zd errno=%d\n",
			atomic_load(&result), atomic_load(&err));
		return 1;
	}

	/* The failed call must not have poisoned the task. */
	if (TASK_socket_get_fd(tfd) < 0) return 1;

	atomic_store(&done, 0);
	errno = 0;
	TASK_socket_readv(tfd, iov, 2, TASK_TIMEOUT_ONE_SEC, NULL, io_cb);
	if (wait_done() < 0) return 1;
	if ((atomic_load(&result) != -1) ||
	    ((atomic_load(&err) != EINVAL) && (atomic_load(&err) != EOVERFLOW))) {
		fprintf(stderr, "readv overflow result=%zd errno=%d\n",
			atomic_load(&result), atomic_load(&err));
		return 1;
	}

	/* A normal operation still works after the rejected ones. */
	{
		static char out[5] = "ping";
		atomic_store(&done, 0);
		TASK_socket_write(tfd, out, 4, TASK_TIMEOUT_ONE_SEC, NULL, buf_cb);
		if (wait_done() < 0) return 1;
		if (atomic_load(&result) != 4) return 1;
	}

	(void)TASK_close(tfd);
	close(sv[0]);
	close(sv[1]);
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
