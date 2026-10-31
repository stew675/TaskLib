/* Regression: task_maybe_free()/task_lookup() could race with task_init()
 * and touch a recycled slot via t->worker, or a stale handle could crash
 * instead of returning EBADF. */
#include "task_lib.h"
#include <pthread.h>

static int32_t ti;
static int32_t stop;
static int64_t shared_tfd;

static void tm_cb(int64_t tfd, int64_t late, void *data)
{
	(void)tfd; (void)late; (void)data;
}

static void *creator(void *arg)
{
	(void)arg;
	while (!task_atomic_load(&stop)) {
		int64_t t = TASK_timeout_create(ti, TASK_TIMEOUT_ONE_SEC, NULL, tm_cb);
		if (t >= 0) {
			task_atomic_store(&shared_tfd, t);
			TASK_timeout_cancel(t);
			TASK_timeout_destroy(t);
		}
	}
	return NULL;
}

static void *stale_user(void *arg)
{
	(void)arg;
	while (!task_atomic_load(&stop)) {
		int64_t t = task_atomic_load(&shared_tfd);
		if (t > 0) {
			/* These may legitimately fail, but must never crash. */
			(void)TASK_socket_get_fd(t);
			(void)TASK_socket_shutdown(t, SHUT_RD);
		}
	}
	return NULL;
}

int main(void)
{
	pthread_t th[4];

	ti = TASK_instance_create(2, 0, 2000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	for (int i = 0; i < 2; i++) pthread_create(&th[i], NULL, creator, NULL);
	for (int i = 2; i < 4; i++) pthread_create(&th[i], NULL, stale_user, NULL);

	sleep(2);
	task_atomic_store(&stop, 1);
	for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);

	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	return 0;
}
