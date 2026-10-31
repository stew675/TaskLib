/* Regression: closing a parent listener used to unlock the child TFD lock
 * through c->worker even for unused pool slots, dereferencing NULL. */
#include "task_lib.h"

int main(void)
{
	int32_t ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	int64_t t = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
	if (t < 0) return 1;

	struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = 0 };
	a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (TASK_socket_bind(t, (struct sockaddr *)&a, sizeof(a)) < 0) return 1;
	if (TASK_socket_listen(t, NULL, NULL) < 0) return 1;

	if (TASK_close(t) < 0) return 1;

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
