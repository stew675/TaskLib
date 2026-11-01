/* Stress the listener arming path: clients connect while a listener is being
 * created and after it is armed.  A listener arming from a non-owner thread
 * used to publish epoll registration before in_epoll/events, allowing the
 * owning worker to drop the one-shot initial accept event and wedge the
 * listener.  This test is probabilistic but cheap and ASan-friendly. */
#include "task_lib.h"
#include <pthread.h>
#include <stdatomic.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define ROUNDS 250

static int32_t ti;
static _Atomic int current_port;
static _Atomic int stop_client;

static _Atomic int accepts;

static void
accept_cb(int64_t tfd, void *data)
{
	(void)data;
	if (tfd > 0) {
		atomic_fetch_add(&accepts, 1);
		(void)TASK_close(tfd);
	}
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
		if (fd < 0) break;
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
	pthread_t th;
	int total_accepts = 0;

	ti = TASK_instance_create(4, 0, 2000, 131072,
				  TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (pthread_create(&th, NULL, client, NULL) != 0) return 1;

	for (int round = 0; round < ROUNDS; round++) {
		struct sockaddr_in a;
		int64_t listener;
		socklen_t alen;
		int fd;

		memset(&a, 0, sizeof(a));
		a.sin_family = AF_INET;
		a.sin_port = 0;
		a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

		listener = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0,
					      NULL, NULL);
		if (listener < 0) return 1;
		if (TASK_socket_bind(listener, (struct sockaddr *)&a,
				     sizeof(a)) < 0) {
			return 1;
		}
		fd = TASK_socket_get_fd(listener);
		alen = sizeof(a);
		if (getsockname(fd, (struct sockaddr *)&a, &alen) < 0) return 1;

		atomic_store(&accepts, 0);
		atomic_store(&current_port, (int)ntohs(a.sin_port));
		if (TASK_socket_listen(listener, NULL, accept_cb) < 0) return 1;

		/* If the initial event was lost, no later connection can re-arm
		 * the one-shot registration and this wait times out. */
		for (int i = 0; i < 200 && atomic_load(&accepts) == 0; i++) {
			usleep(500);
		}
		if (atomic_load(&accepts) == 0) {
			fprintf(stderr, "round %d: listener accepted nothing\n", round);
			return 1;
		}
		total_accepts += atomic_load(&accepts);
		atomic_store(&current_port, 0);
		TASK_close(listener);
		usleep(200);
	}

	atomic_store(&stop_client, 1);
	pthread_join(th, NULL);

	if (total_accepts == 0) return 1;

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
