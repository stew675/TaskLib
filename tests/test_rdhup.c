/* Regression: EPOLLRDHUP/EPOLLHUP used to pre-set rd_shut before trying the
 * read, so data already queued by a peer that half-closed or closed was
 * discarded and reported as EPIPE. */
#include "task_lib.h"
#include <stdatomic.h>

static int32_t ti;
static _Atomic ssize_t last_result;
static _Atomic int last_errno;
static _Atomic int done;
static char buf[64];

static void
read_cb(int64_t tfd, void *b, ssize_t result, void *data)
{
	(void)tfd;
	(void)b;
	(void)data;
	atomic_store(&last_result, result);
	atomic_store(&last_errno, errno);
	atomic_store(&done, 1);
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

static int
make_tcp_pair(int *managed, int *peer)
{
	int ls, cfd, sfd;
	struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = 0 };
	socklen_t alen = sizeof(a);

	a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if ((ls = socket(AF_INET, SOCK_STREAM, 0)) < 0) return -1;
	if (bind(ls, (struct sockaddr *)&a, sizeof(a)) < 0) goto fail;
	if (listen(ls, 8) < 0) goto fail;
	if (getsockname(ls, (struct sockaddr *)&a, &alen) < 0) goto fail;
	if ((cfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) goto fail;
	if (connect(cfd, (struct sockaddr *)&a, sizeof(a)) < 0) goto fail_cfd;
	if ((sfd = accept(ls, NULL, NULL)) < 0) goto fail_cfd;
	close(ls);
	*managed = cfd;
	*peer = sfd;
	return 0;

fail_cfd:
	close(cfd);
fail:
	close(ls);
	return -1;
}

static int
run_case(int close_peer, int prefill)
{
	int managed, peer;
	int64_t tfd;
	ssize_t r;
	int e;

	if (make_tcp_pair(&managed, &peer) < 0) return -1;
	tfd = TASK_socket_register(ti, managed, NULL, NULL);
	if (tfd < 0) return -1;

	atomic_store(&done, 0);
	atomic_store(&last_result, -2);
	atomic_store(&last_errno, 0);
	memset(buf, 0, sizeof(buf));

	if (prefill) {
		if (write(peer, "hello", 5) != 5) return -1;
		if (close_peer) close(peer);
		else shutdown(peer, SHUT_WR);
		usleep(50000);
	}

	TASK_socket_read(tfd, buf, sizeof(buf), TASK_TIMEOUT_ONE_SEC, NULL, read_cb);

	if (!prefill) {
		/* Let the worker arm EPOLLIN and block in epoll before the data
		 * and FIN arrive, so the RDHUP/HUP event path is exercised. */
		usleep(50000);
		if (write(peer, "hello", 5) != 5) return -1;
		if (close_peer) close(peer);
		else shutdown(peer, SHUT_WR);
	}

	if (wait_done() < 0) return -1;
	r = atomic_load(&last_result);
	e = atomic_load(&last_errno);
	if ((r != 5) || (memcmp(buf, "hello", 5) != 0)) {
		fprintf(stderr, "first read result=%zd errno=%d (expected 5)\n", r, e);
		return -1;
	}

	/* The EOF must be reported by the next read, after draining the data. */
	atomic_store(&done, 0);
	atomic_store(&last_result, -2);
	TASK_socket_read(tfd, buf, sizeof(buf), TASK_TIMEOUT_ONE_SEC, NULL, read_cb);
	if (wait_done() < 0) return -1;
	r = atomic_load(&last_result);
	e = atomic_load(&last_errno);
	if ((r != -1) || (e != EPIPE)) {
		fprintf(stderr, "eof read result=%zd errno=%d (expected -1/EPIPE)\n", r, e);
		return -1;
	}

	(void)TASK_close(tfd);
	usleep(50000);
	if (!prefill) close(peer);
	close(managed);
	return 0;
}

int
main(void)
{
	ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (run_case(0, 0) < 0) return 1;	/* pending read, half-close */
	if (run_case(1, 0) < 0) return 1;	/* pending read, full close */
	if (run_case(0, 1) < 0) return 1;	/* data+FIN already queued */
	if (run_case(1, 1) < 0) return 1;	/* data+close already queued */

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
