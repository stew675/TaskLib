/*
 * TaskLib microbenchmarks.
 *
 * Modes:
 *   alloc   task_get_free_task() + task_free() pair
 *   getfd   TASK_socket_get_fd() (locked meta lookup/unlock path)
 *   read    TASK_socket_read() with a zero-length buffer; it fails EINVAL
 *           after the lock-free FLG_RD lookup/reservation path, so this
 *           measures lookup/unlock overhead without doing any IO.
 *
 * The alloc mode uses TaskLib internals deliberately: it isolates the task
 * allocator/free-list from socket syscalls.  The other modes use the public API.
 *
 * Build through run_micro.sh; do not link this file with the normal examples.
 */
#include "task_internal.h"
#include <pthread.h>

static int32_t ti;
static struct instance *inst;
static int nthreads = 1;
static long iters = 1000000;
static const char *mode;
static int64_t *tfds;
static int num_cpus;

static void
pin_cpu(int cpu)
{
	cpu_set_t cs;

	CPU_ZERO(&cs);
	CPU_SET(cpu % num_cpus, &cs);
	(void)pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
}

static void *
alloc_worker(void *arg)
{
	long id = (long)arg;

	pin_cpu((int)id);
	for (long i = 0; i < iters; i++) {
		struct task *t = task_get_free_task(inst);
		if (t == NULL) abort();
		task_free(inst, t, false);
	}
	return NULL;
}

static void *
getfd_worker(void *arg)
{
	long id = (long)arg;
	int64_t tfd = tfds[id];

	pin_cpu((int)id);
	for (long i = 0; i < iters; i++) {
		if (TASK_socket_get_fd(tfd) < 0) abort();
	}
	return NULL;
}

static void *
read_lookup_worker(void *arg)
{
	long id = (long)arg;
	int64_t tfd = tfds[id];
	char b[1];

	pin_cpu((int)id);
	for (long i = 0; i < iters; i++) {
		TASK_socket_read(tfd, b, 0, TASK_TIMEOUT_NEVER, NULL, NULL);
	}
	return NULL;
}

int
main(int argc, char **argv)
{
	if (argc < 4) {
		fprintf(stderr, "usage: %s <alloc|getfd|read> <threads> <iters>\n", argv[0]);
		return 2;
	}

	mode = argv[1];
	nthreads = atoi(argv[2]);
	iters = atol(argv[3]);
	if ((nthreads < 1) || (nthreads > 64) || (iters < 1)) {
		return 2;
	}
	num_cpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
	if (num_cpus < 1) num_cpus = 1;

	int workers = nthreads < 4 ? nthreads : 4;
	ti = TASK_instance_create(workers, 0, 300000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) {
		perror("TASK_instance_create");
		return 1;
	}
	inst = instances[ti];

	tfds = calloc((size_t)nthreads, sizeof(*tfds));
	pthread_t th[64];
	struct timespec a, b;

	if (tfds == NULL) return 1;

	if (!strcmp(mode, "getfd") || !strcmp(mode, "read")) {
		for (int i = 0; i < nthreads; i++) {
			tfds[i] = TASK_socket_create(ti, AF_INET, SOCK_STREAM, 0, NULL, NULL);
			if (tfds[i] < 0) return 1;
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &a);
	for (long i = 0; i < nthreads; i++) {
		void *(*fn)(void *) = alloc_worker;

		if (!strcmp(mode, "getfd")) fn = getfd_worker;
		else if (!strcmp(mode, "read")) fn = read_lookup_worker;
		pthread_create(&th[i], NULL, fn, (void *)i);
	}
	for (long i = 0; i < nthreads; i++) pthread_join(th[i], NULL);
	clock_gettime(CLOCK_MONOTONIC, &b);

	double sec = (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) / 1e9;
	double total = (double)nthreads * (double)iters;
	printf("%s threads=%d iters=%ld sec=%.6f ns/op=%.2f Mops=%.3f\n",
	       mode, nthreads, iters, sec, sec * 1e9 / total, total / sec / 1e6);

	if (!strcmp(mode, "getfd") || !strcmp(mode, "read")) {
		for (int i = 0; i < nthreads; i++) (void)TASK_close(tfds[i]);
	}
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);
	free(tfds);
	return 0;
}
