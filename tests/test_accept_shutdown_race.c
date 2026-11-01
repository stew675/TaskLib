/* Stress accepted-task creation against instance shutdown.  Accepted tasks are
 * created directly on a worker rather than through a creation-style API, so
 * they take their own instance reference while being published.  This test is
 * primarily an ASan/TSan target for that window. */
#include "task_lib.h"
#include <pthread.h>
#include <stdatomic.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define CLIENTS 4

static _Atomic int current_port;
static _Atomic int stop_client;

static void
accept_cb(int64_t tfd, void *data)
{
	(void)data;
	if (tfd > 0) {
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
	pthread_t th[CLIENTS];
	struct sockaddr_in a;
	int64_t listener;
	socklen_t alen;
	int fd;

	for (int round = 0; round < 80; round++) {
		int32_t ti = TASK_instance_create(4, 0, 2000, 131072,
						  TASK_FLAGS_AFFINITY_DISABLE);
		if (ti < 0) return 1;
		if (TASK_instance_start(ti) < 0) return 1;

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

		atomic_store(&current_port, (int)ntohs(a.sin_port));
		atomic_store(&stop_client, 0);
		if (TASK_socket_listen(listener, NULL, accept_cb) < 0) return 1;

		for (int i = 0; i < CLIENTS; i++) {
			if (pthread_create(&th[i], NULL, client, NULL) != 0) {
				return 1;
			}
		}

		usleep(500 + (round % 20) * 50);
		(void)TASK_instance_shutdown(ti, NULL, NULL);
		atomic_store(&stop_client, 1);
		for (int i = 0; i < CLIENTS; i++) {
			pthread_join(th[i], NULL);
		}
		if (TASK_instance_wait(ti) < 0) return 1;
		if (TASK_instance_destroy(ti) < 0) return 1;
	}

	return 0;
}
