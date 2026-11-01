/* Exercise vector IO across the TASK_MAX_IO_UNIT fairness boundary.  This
 * covers the stack-local iovec windowing used by readv/writev and the partial
 * readv result handling. */
#include "task_lib.h"

#define IOVS		3
#define CHUNK		40000
#define TOTAL		(IOVS * CHUNK)

static int32_t ti;
static int64_t wr_tfd, rd_tfd;

static char wr_buf[IOVS][CHUNK];
static char rd_buf[IOVS][CHUNK];
static struct iovec wr_iov[IOVS];
static struct iovec rd_iov[IOVS];

static struct iovec active_rd[IOVS];
static size_t rd_consumed;
static size_t wr_result;
static _Atomic int rd_done;
static _Atomic int wr_done;
static _Atomic int failed;

static void readv_cb(int64_t tfd, const struct iovec *iov, int iovcnt,
		     ssize_t result, void *data);

static void
issue_read(void)
{
	size_t skip = rd_consumed;
	int n = 0, cnt = 0;

	while ((n < IOVS) && (skip >= rd_iov[n].iov_len)) {
		skip -= rd_iov[n].iov_len;
		n++;
	}

	for (; n < IOVS; n++) {
		active_rd[cnt].iov_base = ((char *)rd_iov[n].iov_base) + skip;
		active_rd[cnt].iov_len = rd_iov[n].iov_len - skip;
		cnt++;
		skip = 0;
	}

	if (cnt == 0) {
		__atomic_store_n(&rd_done, 1, __ATOMIC_RELEASE);
		return;
	}

	TASK_socket_readv(rd_tfd, active_rd, cnt, TASK_TIMEOUT_ONE_SEC, NULL,
			  readv_cb);
}

static void
readv_cb(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result,
	 void *data)
{
	(void)tfd;
	(void)iov;
	(void)iovcnt;
	(void)data;

	if (result < 0) {
		__atomic_store_n(&failed, 1, __ATOMIC_RELEASE);
		__atomic_store_n(&rd_done, 1, __ATOMIC_RELEASE);
		return;
	}

	rd_consumed += (size_t)result;
	if (rd_consumed >= TOTAL) {
		__atomic_store_n(&rd_done, 1, __ATOMIC_RELEASE);
		return;
	}
	issue_read();
}

static void
writev_cb(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result,
	  void *data)
{
	(void)tfd;
	(void)iov;
	(void)iovcnt;
	(void)data;

	if (result < 0) {
		__atomic_store_n(&failed, 1, __ATOMIC_RELEASE);
	} else {
		wr_result = (size_t)result;
		__atomic_store_n(&wr_done, 1, __ATOMIC_RELEASE);
	}
}

int
main(void)
{
	int sv[2];

	ti = TASK_instance_create(2, 0, 1000, 131072, TASK_FLAGS_AFFINITY_DISABLE);
	if (ti < 0) return 1;
	if (TASK_instance_start(ti) < 0) return 1;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;

	rd_tfd = TASK_socket_register(ti, sv[0], NULL, NULL);
	wr_tfd = TASK_socket_register(ti, sv[1], NULL, NULL);
	if ((rd_tfd < 0) || (wr_tfd < 0)) return 1;

	for (int i = 0; i < IOVS; i++) {
		wr_iov[i].iov_base = wr_buf[i];
		wr_iov[i].iov_len = CHUNK;
		rd_iov[i].iov_base = rd_buf[i];
		rd_iov[i].iov_len = CHUNK;
		for (int j = 0; j < CHUNK; j++) {
			wr_buf[i][j] = (char)((i * CHUNK + j) & 0xff);
		}
	}

	/* The readv callback is invoked once per fairness slice, so drive it
	 * with a wrapper.  The library API callback parameter cannot be used
	 * directly because we need to re-arm with the advanced iovec. */
	TASK_socket_readv(rd_tfd, rd_iov, IOVS, TASK_TIMEOUT_ONE_SEC, NULL, readv_cb);
	TASK_socket_writev(wr_tfd, wr_iov, IOVS, TASK_TIMEOUT_ONE_SEC, NULL, writev_cb);

	for (int i = 0; i < 500; i++) {
		if (__atomic_load_n(&rd_done, __ATOMIC_ACQUIRE) &&
		    __atomic_load_n(&wr_done, __ATOMIC_ACQUIRE)) {
			break;
		}
		usleep(10000);
	}

	int failed_flag = __atomic_load_n(&failed, __ATOMIC_ACQUIRE);
	int done = __atomic_load_n(&rd_done, __ATOMIC_ACQUIRE) &&
		   __atomic_load_n(&wr_done, __ATOMIC_ACQUIRE);
	int ok = 0;

	if (!failed_flag && done && (rd_consumed == TOTAL) && (wr_result == TOTAL)) {
		ok = 1;
		for (int i = 0; i < IOVS; i++) {
			if (memcmp(wr_buf[i], rd_buf[i], CHUNK) != 0) {
				ok = 0;
				break;
			}
		}
	}

	(void)TASK_close(rd_tfd);
	(void)TASK_close(wr_tfd);
	TASK_instance_shutdown(ti, NULL, NULL);
	TASK_instance_wait(ti);
	TASK_instance_destroy(ti);

	return ok ? 0 : 1;
}
