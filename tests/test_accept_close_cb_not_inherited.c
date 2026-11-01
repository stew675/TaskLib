/* Accepted connections must not inherit the listener's close callback.  The
 * accepted task can outlive the listener, so sharing the listener's user data
 * would make the listener close callback unsafe. */
#include "task_lib.h"
#include <stdatomic.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static _Atomic int close_count;
static _Atomic int accepted;

static void
close_cb(int64_t tfd, void *data)
{
	(void)tfd;
	(void)data;
	atomic_fetch_add(&close_count, 1);
}

static void
accept_cb(int64_t tfd, void *data)
{
	(void)data;
	if (tfd > 0) {
		atomic_store(&accepted, 1);
		/* Close without installing a per-connection close callback. */
		(void)TASK_close(tfd);
	}
}

int
main(void)
{
	int32_t ti = TASK_instance_create(2, 0, 1000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
	struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = 0 };
	int64_t listener;
	int cfd;
	socklen_t alen = sizeof(a);

	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	listener = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0,
				      NULL, close_cb);
	if (listener < 0) return 1;
	if (TASK_socket_bind(listener, (struct sockaddr *)&a, sizeof(a)) < 0) {
		return 1;
	}
	if (TASK_socket_listen(listener, NULL, accept_cb) < 0) return 1;
	if (getsockname(TASK_socket_get_fd(listener),
			(struct sockaddr *)&a, &alen) < 0) return 1;

	cfd = socket(AF_INET, SOCK_STREAM, 0);
	if (cfd < 0) return 1;
	if (connect(cfd, (struct sockaddr *)&a, sizeof(a)) < 0) return 1;

	for (int i = 0; i < 500 && !atomic_load(&accepted); i++) {
		usleep(1000);
	}
	if (!atomic_load(&accepted)) return 1;

	/* Let the accepted task close.  Only the listener close callback may run. */
	usleep(50000);
	(void)TASK_close(listener);
	for (int i = 0; i < 500 && atomic_load(&close_count) == 0; i++) {
		usleep(1000);
	}
	usleep(50000);
	close(cfd);

	if (atomic_load(&close_count) != 1) {
		fprintf(stderr, "listener close callback ran %d times\n",
			atomic_load(&close_count));
		return 1;
	}

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
