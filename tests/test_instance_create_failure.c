/* Regression: a failed TASK_instance_create() left evfd at its zero-initialised
 * value and instance_destroy() then closed fd 0 (stdin).  Oversized sizing is
 * rejected before publishing, and every fd field starts at -1. */
#include "task_lib.h"
#include <fcntl.h>
#include <sys/resource.h>
#include <stdint.h>

#if defined(__SANITIZE_ADDRESS__)
#define TEST_SKIP_SANITIZER 1
#elif defined(__SANITIZE_THREAD__)
#define TEST_SKIP_SANITIZER 1
#endif

static int
fd_is_open(int fd)
{
	return fcntl(fd, F_GETFD) != -1;
}

int
main(void)
{
	struct rlimit old, lim;
	int32_t ti;

#ifdef TEST_SKIP_SANITIZER
	/* Sanitizers reserve large virtual address ranges; RLIMIT_AS cannot be
	 * used to force this allocation failure reliably. */
	return 0;
#endif

	/* Make sure the standard descriptors are actually open before we test
	 * that a failed constructor did not close them. */
	for (int fd = 0; fd < 3; fd++) {
		if (!fd_is_open(fd)) {
			int nfd = open("/dev/null", O_RDWR);
			if (nfd < 0) return 0;
			if (nfd != fd) {
				dup2(nfd, fd);
				close(nfd);
			}
		}
	}

	if (getrlimit(RLIMIT_AS, &old) < 0) return 0;
	lim = old;
	lim.rlim_cur = 1024UL * 1024 * 1024;
	if ((old.rlim_cur != RLIM_INFINITY) && (old.rlim_cur < lim.rlim_cur)) {
		lim.rlim_cur = old.rlim_cur;
	}
	if (setrlimit(RLIMIT_AS, &lim) < 0) return 0;

	errno = 0;
	ti = TASK_instance_create(1, 0, 100000000u, 131072,
				  TASK_FLAGS_AFFINITY_DISABLE);

	if (ti >= 0) {
		/* Not the failure this test wants to exercise, but do not leak. */
		TASK_instance_shutdown(ti, NULL, NULL);
		TASK_instance_wait(ti);
		TASK_instance_destroy(ti);
		setrlimit(RLIMIT_AS, &old);
		return 0;
	}

	(void)setrlimit(RLIMIT_AS, &old);

	if (!fd_is_open(0) || !fd_is_open(1) || !fd_is_open(2)) {
		fprintf(stderr, "failed create closed a standard fd: 0=%d 1=%d 2=%d\n",
			fd_is_open(0), fd_is_open(1), fd_is_open(2));
		return 1;
	}
	return 0;
}
