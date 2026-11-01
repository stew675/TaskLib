/* Regression: TASK_socket_listen() captured the local address with
 * getsockname() before listen().  For an unbound socket the kernel assigns the
 * port during listen(), so the balanced child listeners were bound to port 0
 * and received different random ports.  All listener clones must now share the
 * parent's assigned port. */
#define _GNU_SOURCE
#include "task_lib.h"
#include <dirent.h>

static int
listener_port(int fd)
{
	struct sockaddr_in a;
	socklen_t alen = sizeof(a);

	if (getsockname(fd, (struct sockaddr *)&a, &alen) < 0) {
		return -1;
	}
	if (a.sin_family != AF_INET) {
		return -1;
	}
	return (int)ntohs(a.sin_port);
}

static int
is_listening(int fd)
{
	int accepting = 0;
	socklen_t alen = sizeof(accepting);

	if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &accepting, &alen) < 0 ||
	    accepting != SOCK_STREAM) {
		return 0;
	}
	alen = sizeof(accepting);
	if (getsockopt(fd, SOL_SOCKET, SO_ACCEPTCONN, &accepting, &alen) < 0) {
		return 0;
	}
	return accepting != 0;
}

int
main(void)
{
	int32_t ti = TASK_instance_create(3, 0, 1000, 131072,
					  TASK_FLAGS_AFFINITY_DISABLE);
	int64_t listener;
	int parent_fd;
	int parent_port;
	int found = 0;
	DIR *d;
	struct dirent *de;

	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	listener = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (listener < 0) return 1;

	/* Deliberately do not bind. */
	if (TASK_socket_listen(listener, NULL, NULL) < 0) return 1;

	parent_fd = TASK_socket_get_fd(listener);
	parent_port = listener_port(parent_fd);
	if (parent_port <= 0) return 1;

	d = opendir("/proc/self/fd");
	if (d == NULL) return 1;
	while ((de = readdir(d)) != NULL) {
		int fd = atoi(de->d_name);
		int port;

		if (!is_listening(fd)) {
			continue;
		}
		port = listener_port(fd);
		if (port <= 0) {
			continue;
		}
		found++;
		if (port != parent_port) {
			fprintf(stderr, "listener fd %d uses port %d, parent uses %d\n",
				fd, port, parent_port);
			closedir(d);
			return 1;
		}
	}
	closedir(d);

	if (found < 2) {
		fprintf(stderr, "expected balanced listener clones, found %d\n", found);
		return 1;
	}

	TASK_close(listener);
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
