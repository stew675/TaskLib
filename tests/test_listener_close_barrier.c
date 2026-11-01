/* Regression: a listener's close callback ran before its per-worker child
 * listeners had stopped accepting.  A connection delivered by a child after
 * the parent close_cb therefore invoked the shared accept callback against
 * user data that the parent callback may already have freed.
 *
 * The test keeps a client connecting continuously while the listener is
 * closed, and asserts that no accept callback starts after the listener's own
 * close callback has run. */
#define _GNU_SOURCE
#include "task_lib.h"
#include <pthread.h>
#include <stdatomic.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define ROUNDS 120

struct listener_ctx {
	_Atomic int64_t listener_tfd;
	_Atomic int close_seen;
	_Atomic int late_accepts;
	_Atomic int accepts;
};

static int32_t ti;
static _Atomic int current_port;
static _Atomic int stop_client;

static void
close_cb(int64_t tfd, void *data)
{
	struct listener_ctx *ctx = data;

	/* Accepted connections inherit this callback.  Only the parent listener
	 * close defines the barrier being tested. */
	if (tfd == atomic_load(&ctx->listener_tfd)) {
		atomic_store(&ctx->close_seen, 1);
	}
}

static void
accept_cb(int64_t tfd, void *data)
{
	struct listener_ctx *ctx = data;

	if (tfd < 0) {
		return;
	}
	atomic_fetch_add(&ctx->accepts, 1);
	if (atomic_load(&ctx->close_seen)) {
		atomic_fetch_add(&ctx->late_accepts, 1);
	}
	(void)TASK_close(tfd);
}

static void *
client(void *arg)
{
	(void)arg;

	while (!atomic_load(&stop_client)) {
		int port = atomic_load(&current_port);
		struct sockaddr_in a;
		int fd;

		if (port <= 0) {
			usleep(10);
			continue;
		}
		fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0) {
			break;
		}
		memset(&a, 0, sizeof(a));
		a.sin_family = AF_INET;
		a.sin_port = htons((uint16_t)port);
		a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		if (connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0) {
			usleep(100);
		}
		close(fd);
	}
	return NULL;
}

int
main(void)
{
	pthread_t client_thr;
	int total_late = 0;

	ti = TASK_instance_create(4, 0, 2000, 131072,
				  TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (pthread_create(&client_thr, NULL, client, NULL) != 0) return 1;

	for (int round = 0; round < ROUNDS; round++) {
		struct listener_ctx ctx;
		struct sockaddr_in a;
		int64_t listener;
		socklen_t alen;
		int fd;

		memset(&ctx, 0, sizeof(ctx));
		memset(&a, 0, sizeof(a));
		a.sin_family = AF_INET;
		a.sin_port = 0;
		a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

		listener = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0,
					      &ctx, close_cb);
		if (listener < 0) return 1;
		atomic_store(&ctx.listener_tfd, listener);
		if (TASK_socket_bind(listener, (struct sockaddr *)&a,
				     sizeof(a)) < 0) {
			return 1;
		}
		fd = TASK_socket_get_fd(listener);
		alen = sizeof(a);
		if (getsockname(fd, (struct sockaddr *)&a, &alen) < 0) return 1;
		atomic_store(&current_port, (int)ntohs(a.sin_port));

		if (TASK_socket_listen(listener, &ctx, accept_cb) < 0) return 1;

		/* Give the client time to queue connections on several workers. */
		usleep(200);
		if (TASK_close(listener) < 0) return 1;

		for (int i = 0; i < 1000 && !atomic_load(&ctx.close_seen); i++) {
			usleep(50);
		}
		if (!atomic_load(&ctx.close_seen)) {
			fprintf(stderr, "listener close callback did not run\n");
			return 1;
		}

		/* Child teardown should already be complete when close_cb runs.
		 * Allow a short settling period, but any accept after close_seen
		 * is a contract violation. */
		usleep(2000);
		total_late += atomic_load(&ctx.late_accepts);
		atomic_store(&current_port, 0);
		usleep(50);
	}

	atomic_store(&stop_client, 1);
	pthread_join(client_thr, NULL);

	if (total_late != 0) {
		fprintf(stderr, "%d accepts started after listener close_cb\n",
			total_late);
		return 1;
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
