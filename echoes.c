#include "task_lib.h"

#define BUFLEN		32768
#define	MAX_ADDRS	32

struct acceptor {
	int64_t		tfd;
};

struct echo {
	int64_t		tfd;
	char		buffer[BUFLEN];

	uint64_t	received;
	uint64_t	delivered;
};

static void start_read(struct echo *e);

static int32_t task_instance = 0;
static uint32_t	num_conns = 0, total_conns = 0, max_conns = 100;

struct sockaddr_storage acpt_storage[MAX_ADDRS];
struct sockaddr_in *    acpt_addrs[MAX_ADDRS];
int	num_acpt = 0;


static void
close_cb(int64_t tfd, void *user_data)
{
	struct echo *e = (struct echo *)user_data;

	(void)tfd;

	if (e) {
//		fprintf(stderr, "Connection Completed: tfd=%lu, Received/Delivered %lu/%lu bytes\n", tfd, e->received, e->delivered);
		free(e);
	}
	ck_pr_dec_32(&num_conns);
} // close_cb


static void
write_done_cb(int64_t tfd, const void *buf, ssize_t result, void *user_data)
{
	struct echo *e = (struct echo *)user_data;

	(void)tfd;
	(void)buf;

	if (result == 0) {
		return;		// Write was queued, just return
	}

	if (result < 0) {
		if (errno == ETIMEDOUT) {
			fprintf(stderr, "Blocked writes on tfd=%ld.  Shutting it down\n", (tfd & 0xffffffff));
		} else {
//			perror("TASK_socket_write");
		}
		if (TASK_close(tfd) < 0) {
			perror("TASK_close");
		}
		return;
	}

	e->delivered += result;
	start_read(e);
} // write_done_cb


static void
read_done_cb(int64_t tfd, void *buf, ssize_t result, void *user_data)
{
	struct echo *e = (struct echo *)user_data;

	(void)buf;
	(void)tfd;

	if (result < 0) {
		if (errno == ETIMEDOUT) {
			fprintf(stderr, "Idle client timeout on tfd=%ld.  Shutting it down\n", (tfd & 0xffffffff));
		} else {
//			perror("TASK_socket_read");
		}
		if (TASK_close(tfd) < 0) {
			perror("TASK_close");
		}
		return;
	} else if (result == 0) {
		return;		// It's queued.  Just return
	}

	e->received += result;

	// Now just write it straight back!
	result = TASK_socket_write(e->tfd, e->buffer, result, TASK_TIMEOUT_ONE_SEC * 10, e, write_done_cb);
	write_done_cb(e->tfd, e->buffer, result, e);
} // read_done_cb


static void
start_read(struct echo *e)
{
	ssize_t result;

	result = TASK_socket_read(e->tfd, e->buffer, BUFLEN, TASK_TIMEOUT_ONE_MINUTE, e, read_done_cb);
	read_done_cb(e->tfd, e->buffer, result, e);
} // start_read


static void
accept_cb(int64_t new_tfd, void *data)
{
	struct acceptor *a = (struct acceptor *)data;
	struct echo *e;

	(void)a;

	if (new_tfd < 0) {
		// There was a new connection but the task library ran out
		// of resources to handle it.  It's just letting us know
		// that it's getting overloaded
		return;
	}

//	fprintf(stderr, "Got a new connection on new TFD = %ld\n", (new_tfd & 0xffffffff));

	// We got a new connection!
	if ((e = calloc(1, sizeof(struct echo))) == NULL) {
		perror("start_echo->calloc");
		TASK_close(new_tfd);
		return;
	}

	ck_pr_inc_32(&total_conns);
	e->tfd = new_tfd;
	TASK_socket_set_close_cb(new_tfd, e, close_cb);
	ck_pr_inc_32(&num_conns);
	start_read(e);
} // accept_cb


static void
accept_close_cb(int64_t tfd, void *data)
{
	struct acceptor *a = (struct acceptor *)data;

	if (a && (a->tfd == tfd)) {
		fprintf(stderr, "Accept Socket Close\n");
		TASK_instance_shutdown(task_instance, NULL, NULL);
	} else {
		fprintf(stderr, "Got a close on a nascent accepted connection\n");
		ck_pr_dec_32(&num_conns);
	}
} // accept_close_cb


static void
ticker_cb(int64_t tfd, int64_t lateness_us, void *user_data)
{
	static int64_t count = 0;
	int interval = 1;

	// Set next timeout, adjuted for the lateness of delivery of this one
	TASK_timeout_set(tfd, ((TASK_TIMEOUT_ONE_SEC * interval) - lateness_us), user_data, ticker_cb);

	fprintf(stderr, "T+%-6ld: Num Connections = %u, Total Connections = %u, tfd=%ld, late by %ldus\n", count, num_conns, total_conns, (tfd & 0xffffffff), lateness_us);
	count += interval;
} // ticker_cb


static void
echoes_start(int64_t tfd, int64_t lateness_us, void *user_data)
{
	int n;

	(void)lateness_us;
	(void)user_data;

	for (n = 0; n < num_acpt; n++) {
		struct acceptor *acpt = NULL;

		if ((acpt = calloc(1, sizeof(struct acceptor))) == NULL) {
			perror("parse_acceptor_args->calloc");
			continue;
		}

		fprintf(stderr, "Attempting to listen on: %s %d\n", inet_ntoa(acpt_addrs[n]->sin_addr), (int)ntohs(acpt_addrs[n]->sin_port));
		// We've got an address to listen on.  create, bind, listen
		// TASK_FLAG_INHERIT_WORKER,
		if ((acpt->tfd = TASK_socket_create(task_instance, AF_INET, SOCK_STREAM, 0, (void *)acpt, accept_close_cb)) < 0) {
			perror("parse_acceptor_args->TASK_socket_create");
			free(acpt);
			continue;
		}
		if (TASK_socket_bind(acpt->tfd, (struct sockaddr *)(acpt_addrs[n]), sizeof(struct sockaddr_in)) < 0) {
			perror("parse_acceptor_args->TASK_socket_bind");
			close(acpt->tfd);
			continue;
		}
		if (TASK_socket_listen(acpt->tfd, (void *)acpt, accept_cb) < 0) {
			perror("parse_acceptor_args->TASK_socket_listen");
			close(acpt->tfd);
			continue;
		}
		fprintf(stderr, "Listening on: %s %d\n", inet_ntoa(acpt_addrs[n]->sin_addr), (int)ntohs(acpt_addrs[n]->sin_port));
	}

	// Free up the initial start timer
	TASK_close(tfd);
} // echoes_start


// Parses the command line to set up the configuration for the acceptor
#define WORKER_STATE_SPEC_LEN		1024
static void
parse_acceptor_args(int argc, const char *argv[])
{
	int is_listen = false, is_numclients = false;

	for(int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-l") == 0) {
			is_listen = true;
			continue;
		}

		if (strcmp(argv[i], "-n") == 0) {
			is_numclients = true;
			continue;
		}

		if (is_numclients) {
			is_numclients = false;
			max_conns = (uint32_t)atoi(argv[i]);
			fprintf(stderr, "Setting maximum connections to %u\n", max_conns);
			continue;
		}

		if (is_listen) {
			char worker_spec[WORKER_STATE_SPEC_LEN], *port_spec;
			struct sockaddr_storage a[1];
			struct sockaddr_in *addr = (struct sockaddr_in *)a;

			is_listen = false;

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
			memcpy(acpt_addrs[num_acpt], addr, sizeof(struct sockaddr_in));
			num_acpt++;
			continue;
		}
	}
} // parse_acceptor_args


static void
usage(const char *name)
{
	fprintf(stderr, "Usage: %s [-n max_conns] (-l <[local_ip]:local_port>...)\n", name);
} // usage


// Where it all begins!
int
main(int argc, const char *argv[])
{
	for (int n = 0; n < MAX_ADDRS; n++) {
		memset(acpt_storage + n, 0, sizeof(*acpt_storage));
		acpt_addrs[n] = (struct sockaddr_in *)acpt_storage + n;
	}

	parse_acceptor_args(argc, argv);
	if (num_acpt == 0) {
		usage(argv[0]);
		TASK_instance_destroy(task_instance);
		return 1;
	}

	uint64_t tcp_mem = 335544320 / max_conns;	// 320M
	if ((task_instance = TASK_instance_create((get_nprocs_conf() / 2), 0, max_conns, tcp_mem)) < 0) {
		perror("TASK_instance_create");
		return 1;
	}

	// Our entry function.  Make it start as soon as the instance starts
	if (TASK_timeout_create(task_instance, 0, NULL, echoes_start) < 0) {
		perror("TASK_timeout_set");
	}

	// Current status ticker
	if (TASK_timeout_create(task_instance, 0, NULL, ticker_cb) < 0) {
		perror("TASK_timeout_set");
	}

	// Set it off
	TASK_instance_start(task_instance);

	// Wait for it to shutdown
	TASK_instance_wait(task_instance);

	// Destroy the instance
	TASK_instance_destroy(task_instance);
	return 0;
} // main
