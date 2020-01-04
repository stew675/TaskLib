#include "task_lib.h"


#define	BUFLEN		32768
#define	MAX_ADDRS	100
#define	MAX_TCP_MEM	2147483648		// 2GB at most outstanding
#define	MAX_OUTSTANDING	209715			// Writes cannot be ahead of reads by more than 2MB

struct loadgen {
	int64_t		tfd;
	int		buflen;
	uint64_t	received;
	uint64_t	delivered;
	intptr_t	ti;
	int		done;
	struct sockaddr_in *addr;
	char		*wr_buf;
	char		rd_buf[BUFLEN];
	uint64_t	num_wr_calls;
	uint64_t	num_rd_calls;
	bool		wr_blocked;
};

static intptr_t task_instance;

struct sockaddr_storage conn_storage[MAX_ADDRS];
struct sockaddr *	conn_addrs[MAX_ADDRS];
uint64_t		num_addrs = 0, num_connects = 0, num_conns = 0;
char wr_buf[BUFLEN];

int		maxgens = 10;
uint64_t	numgens = 0;
uint64_t	deliver_size = 10000000000;	// 10GB
uint64_t	total_deliver = 0;
uint64_t	total_received = 0;
uint64_t	run_time = 0;
uint64_t	max_outstanding = (MAX_TCP_MEM / 100);
uint64_t	run_in = 0, run_out = 0;

static void loadgen_start_rd(struct loadgen *lg);
static void loadgen_start_wr(struct loadgen *lg);

static pthread_spinlock_t deliver_lock[1];
static pthread_spinlock_t receive_lock[1];


static void
loadgen_close_cb(int64_t tfd, void *user_data)
{
	struct loadgen *lg = (struct loadgen *)user_data;

	(void)tfd;
	if (lg) {
//		fprintf(stderr, "loadgen close_cb called: Received/Delivered %lu/%lu bytes, Read/Write Calls: %u/%u\n", lg->received, lg->delivered, lg->num_rd_calls, lg->num_wr_calls);
		free(lg);
		ck_pr_dec_64(&numgens);
	}
} // loadgen_close_cb


static void
loadgen_read_cb(int64_t tfd, void *buf, ssize_t result, void *user_data)
{
	struct loadgen *lg = (struct loadgen *)user_data;

	(void)tfd;
	(void)buf;

	if (result == 0) {
		return;		// It was queued, just return
	}

	if (result < 0) {
		if (errno == ETIMEDOUT) {
			fprintf(stderr, "Reads starving on tfd=%ld.  Shutting connection down\n", (tfd & 0xffffffff));
		} else {
			if (errno != EOWNERDEAD) {
				perror("loadgen_read");
			}
		}
		TASK_close(tfd);
		return;
	}

	ck_pr_add_64(&total_received, result);
	lg->received += result;
	if (lg->done) {
		if (lg->delivered == lg->received) {
			// We're done
			TASK_close(lg->tfd);
			return;
		}
	}

	// Go back to the beginning
	loadgen_start_rd(lg);
} // loadgen_read_cb


static void
loadgen_start_rd(struct loadgen *lg)
{
	ssize_t result;

	lg->num_rd_calls++;
	result = TASK_socket_read(lg->tfd, lg->rd_buf, BUFLEN, TASK_TIMEOUT_ONE_SEC * 15, lg, loadgen_read_cb);
	loadgen_read_cb(lg->tfd, lg->rd_buf, result, lg);
} // loadgen_start_rd


static void
loadgen_write_cb(int64_t tfd, const void *buf, ssize_t result, void *user_data)
{
	struct loadgen *lg = (struct loadgen *)user_data;

	(void)tfd;
	(void)buf;

	if (result == 0) {
		return;		// It was queued, just return
	}
	if (result < 0) {
		if (errno == ETIMEDOUT) {
			int fd, in, out;

			fd = TASK_socket_get_fd(tfd);
			ioctl(fd, TIOCINQ, &in);
			ioctl(fd, TIOCOUTQ, &out);
			fprintf(stderr, "Writes blocked on tfd=%ld.  Shutting connection down.  InQ=%d, OutQ=%d\n", (tfd & 0xffffffff), in, out);
		} else {
			if (errno != EOWNERDEAD) {
				perror("loadgen_write");
				if (errno == EINPROGRESS) {
					assert(0);
				}
			}
		}
		TASK_close(tfd);
		return;
	}

	ck_pr_add_64(&total_deliver, result);
	lg->delivered += result;
	loadgen_start_wr(lg);
} // loadgen_write_cb


static void
loadgen_start_wr(struct loadgen *lg)
{
	ssize_t result;
	size_t wr_size = 0;

	wr_size = deliver_size - lg->delivered;
	if (wr_size > BUFLEN) {
		wr_size = BUFLEN;
	} else if (wr_size == 0) {
		lg->done = 1;
		return;
	}

	lg->num_wr_calls++;
	result = TASK_socket_write(lg->tfd, lg->wr_buf, wr_size, TASK_TIMEOUT_ONE_SEC * 15, lg, loadgen_write_cb);
	loadgen_write_cb(lg->tfd, lg->wr_buf, result, lg);
} // loadgen_start_wr


static void
connect_complete_cb(int64_t tfd, int result, void *user_data)
{
	struct loadgen *lg = (struct loadgen *)user_data;

	(void)tfd;

	if (result == 0) {
		return;		// It was queued, wait for the real result
	}
	if (result < 0) {
		perror("connect_complete_cb->TASK_socket_connect");
		TASK_close(tfd);
		return;
	}
//	fprintf(stderr, "Connected to: tfd=%d %s %d\n", tfd, inet_ntoa(lg->addr->sin_addr), (int)ntohs(lg->addr->sin_port));
	loadgen_start_wr(lg);
	loadgen_start_rd(lg);
} // connect_complete_cb


static void
loadgen_connect(struct loadgen *lg)
{
	struct sockaddr_in lca;
	int res;

	lca.sin_family = AF_INET;
	lca.sin_addr.s_addr = INADDR_ANY;
	lca.sin_port = 0;

//	fprintf(stderr, "Attempting to connect to: %s %d\n", inet_ntoa(lg->addr->sin_addr), (int)ntohs(lg->addr->sin_port));
	// We've got an address to connect to.  create, bind, connect
	if ((lg->tfd = TASK_socket_create(task_instance, AF_INET, SOCK_STREAM, 0, (void *)lg, loadgen_close_cb)) < 0) {
		perror("loadgen_connect->TASK_socket_create");
		ck_pr_dec_64(&numgens);
		free(lg);
		return;
	}

	if (TASK_socket_bind(lg->tfd, (struct sockaddr *)&lca, sizeof(struct sockaddr_in)) < 0) {
		if (errno != EADDRINUSE) {
			perror("loadgen_connect->TASK_socket_bind");
		}
		TASK_close(lg->tfd);
		return;
	}

	res = TASK_socket_connect(lg->tfd, (struct sockaddr *)lg->addr, sizeof(struct sockaddr_in), TASK_TIMEOUT_ONE_SEC * 15, (void *)lg, connect_complete_cb);
	connect_complete_cb(lg->tfd, res, lg);
}


#define RESPAWN_PERIOD_US	20000		// 20000us = 20ms
#define MAX_SPAWN_PER_LOOP	10		// (1000ms / 20ms) * 10 => 500TPS maximum

static void
loadgen_start(int64_t tfd, int64_t lateness_us, void *user_data)
{
	int64_t next_duration;
	int i, num_to_spawn;

	(void)user_data;

	// Set up to call outselves again every RESPAWN_PERIOD microseconds
	next_duration = (next_duration = (RESPAWN_PERIOD_US - lateness_us)) < 0 ? 0 : next_duration;
	TASK_timeout_set(tfd, next_duration, NULL, loadgen_start);

	// We're called every 10 millisecond, so basically max 2000/sec
	num_to_spawn = maxgens - numgens;
	if (num_to_spawn > MAX_SPAWN_PER_LOOP) {
		num_to_spawn = MAX_SPAWN_PER_LOOP;
	}

	for (i = 0; i < num_to_spawn; i++) {
		struct loadgen *lg;
		int ap;

		if ((lg = calloc(1, sizeof(struct loadgen))) == NULL) {
			continue;
		}

		lg->wr_buf = wr_buf;

		ck_pr_inc_64(&num_connects);
		ck_pr_inc_64(&numgens);

		max_outstanding = MAX_TCP_MEM / (numgens + 100);
		ap = ck_pr_load_64(&num_connects) % num_addrs;
		lg->addr = (struct sockaddr_in *)conn_addrs[ap];
		loadgen_connect(lg);
	}

} // loadgen_start


#define TICKER_PERIOD_US	1000000		// 1s
static void
ticker_cb(int64_t tfd, int64_t lateness_us, void *user_data)
{
	static int64_t count = 0;
	int interval = 1;
	int smooth = 10;

	TASK_timeout_set(tfd, ((TICKER_PERIOD_US * interval) - lateness_us), user_data, ticker_cb);
	if (run_in) {
		run_in = ((run_in * (smooth - 1)) + total_received) / smooth;
	} else {
		run_in = total_received;
	}
	total_received = 0;
	if (run_out) {
		run_out = ((run_out * (smooth - 1)) + total_deliver) / smooth;
	} else {
		run_out = total_deliver;
	}
	total_deliver = 0;

	fprintf(stderr, "T+%-6ld: IN MB/sec=%lu OUT MB/sec=%lu\n", count, run_in / (1024 * 1024), run_out / (1024 * 1024));
	count += interval;
} // ticker_cb


static void
done_hammer_cb(int64_t tfd, int64_t lateness_us, void *user_data)
{
	(void)tfd;
	(void)lateness_us;
	(void)user_data;

	fprintf(stderr, "RUN TIME REACHED!  SHUTTING DOWN!\n");
	TASK_instance_shutdown(task_instance, NULL, NULL);
	exit(0);
} // done_hammer_cb


// Parses the command line to set up the configuration for the load generator
#define WORKER_STATE_SPEC_LEN		1024
static int
parse_acceptor_args(int argc, const char *argv[])
{
	bool is_connect = false, is_runtime = false;
	bool is_numgen = false, is_sendsize = false;

	for(int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-c") == 0) {
			is_connect = true;
			continue;
		}
		if (strcmp(argv[i], "-n") == 0) {
			is_numgen = true;
			continue;
		}
		if (strcmp(argv[i], "-s") == 0) {
			is_sendsize = true;
			continue;
		}
		if (strcmp(argv[i], "-t") == 0) {
			is_runtime = true;
			continue;
		}

		if (is_runtime) {
			is_runtime = false;
			run_time = atoi(argv[i]);
			run_time = run_time * 1000000;
			continue;
		}

		if (is_numgen) {
			is_numgen = false;
			maxgens = atoi(argv[i]);
			continue;
		}

		if (is_sendsize) {
			is_sendsize = false;
			deliver_size = atoi(argv[i]);
			deliver_size *= 1024;
			deliver_size *= 1024;
fprintf(stderr, "Setting deliver_size to %lu\n", deliver_size);
			continue;
		}

		if (is_connect) {
			char worker_spec[WORKER_STATE_SPEC_LEN], *port_spec;
			struct sockaddr_storage a[1];
			struct sockaddr_in *addr = (struct sockaddr_in *)a;

			is_connect = false;

			memcpy(worker_spec, argv[i], strlen(argv[i]) + 1);
			worker_spec[WORKER_STATE_SPEC_LEN - 1] = '\0';
			port_spec = strchr(worker_spec, ':');

			if (port_spec && (port_spec != worker_spec)) {
				*port_spec++ = '\0';
				inet_aton(worker_spec, &addr->sin_addr);
			} else {
				port_spec = worker_spec;
				inet_aton("127.0.0.1", &addr->sin_addr);
			}

			addr->sin_family = AF_INET;
			addr->sin_port = htons((in_port_t)atoi(port_spec));

			memcpy(conn_addrs[num_addrs], addr, sizeof(struct sockaddr_in));
			num_addrs++;
		}
	}

	return 0;
} // parse_acceptor_args


static void
usage(const char *name)
{
	fprintf(stderr, "Usage: %s [-n num_conns] [-s size_mb] [-l <[local_ip]:local_port>...] [-c <[local_ip]:local_port>]\n", name);
} // usage


// Where it all begins!
int
main(int argc, const char *argv[])
{
	pthread_spin_init(deliver_lock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(receive_lock, PTHREAD_PROCESS_PRIVATE);

	for (int i = 0; i < MAX_ADDRS; i++) {
		conn_addrs[i] = (struct sockaddr *)conn_storage + i;
	}

	if (parse_acceptor_args(argc, argv) < 0) {
		usage(argv[0]);
	}
	if (num_addrs == 0) {
		usage(argv[0]);
		return 1;
	}

	for (int n = 0; n < BUFLEN; n++) {
		wr_buf[n] = 'a' + n%26;
	}

	fprintf(stderr, "Generating Load for %u clients\n", maxgens);

	if ((task_instance = TASK_instance_create((get_nprocs_conf() + 2) / 4, 0, maxgens, 98304, TAKS_FLAGS_AFFINITY_FORCE)) < 0) {
		perror("TASK_instance_create");
		return 1;
	}

	// Our entry function.  Make it start as soon as the instance starts
	if (TASK_timeout_create(task_instance, 0, NULL, loadgen_start) < 0) {
		perror("TASK_timeout_set");
	}

	// Create data rate ticker
	if (TASK_timeout_create(task_instance, 0, NULL, ticker_cb) < 0) {
		perror("TASK_timeout_set");
	}

	// Create big hammer timeout
	if (run_time > 0) {
		if (TASK_timeout_create(task_instance, run_time, NULL, done_hammer_cb) < 0) {
			perror("TASK_timeout_set");
		}
	}

	// Set it off
	TASK_instance_start(task_instance);

	// Wait for it to shutdown
	TASK_instance_wait(task_instance);

	// Destroy the instance
	TASK_instance_destroy(task_instance);
	return 0;
} // main
