// TCP Server Foundation Framework written in C
//
// Author: Stew Forster (stew675@gmail.com)
//
// A simple but powerful foundational framework intended as a launching pad for most any
// load capacity variant of TCP client/server project.
//
// Features:
// - A fully event driven multi-threaded-IO MP-safe framework
// - A full set of socket connect+IO calls with automated timeout handling
// - Fully distributed connection acceptance for better acceptance serving
// - A light-weight but fast and powerful timer queue mechanism built atop my paired heap
//   library here: git@github.com:stew675/Paired-Heap-Library.git
// - Ability to bind to and listen for new connections on any number of ip:port tuples
// - Ability to create multiple worker threads to distribute load across multiple CPUs
// - Automated detection of the number of CPUs in the system, and binding the affinity of
//   worker threads to specific CPUs based upon operational patterns seen
// - Automated load balancing of tasks across CPUs and worker threads
// - Adaptive task migration.  If one task is continually operating on another task (such
//   as a proxy might, with one task reading from a service, and another task responding
//   to a client), then both tasks will automatically migrate to the same worker thread
//   for lower latency operations and better CPU cache coherency.
// - Self-contained task, worker, instance definitions for easy extensions
// - Thread local storage of important variables
// - Easily accessible micro-second resolution timing
// - Fully instanced self-contained configuration allows for multiple services to use the
//   library within the same process independently of each other

#include "ph.h"
#include "task_lib.h"

// Uncomment to turn on poll() instead of epoll(). poll() mode is about 15% slower than
// epoll() and burns way more CPU on array set management.  Use epoll() if you can
// #define USE_POLL		// DO NOT ENABLE - DOES NOT WORK

// Uncomment to turn on either EPOLLET/EPOLLONESHOT style epolling (doesn't apply to poll() mode)
#define USE_EPOLLET
#define USE_EPOLLONESHOT

// Uncomment the following to turn on using pthread spinlocks for TFD table and worker locking
//#define USE_PTHREAD_SPINLOCKS

#define TASK_MAX_IO_DEPTH	2				// Max depth IO nested callbacks can be before queueing
#define TASK_MAX_IO_UNIT	32768				// The maximum amount that may be read/written in one go
#define TASK_MAX_EVENTS		1024				// More than this impacts CPU cache lines negatively
#define TASK_LISTEN_BACKLOG	((int)1024)			// System auto-truncates it to system limit anyway
#define	TASK_MAX_INSTANCES	16				// Maximum number of Task library instances allowed at once
#define	TASK_MAX_TFD_LOCKS	253				// Number of TFD spinlocks in an instance's lock pool

// I highly recommend NOT fiddling with the COLT1 value unless you understand what it will impact
#define	WORKER_TIME_COLT1	4500000				// 4s (expressed in microseconds)
#define	WORKER_TIME_COLT2	(WORKER_TIME_COLT1 * 1.8)	// COLT1 * 1.8 (expressed in microseconds)
#define TASK_MAX_EPOLL_WAIT_MS	(WORKER_TIME_COLT1 / 5000)	// 1/5th that of COLT1 (expressed in milliseconds)

// Handy time unit conversion macros
#define TASK_MS_TO_US(a)	(((int64_t)a) * 1000)
#define TASK_US_TO_MS(a)	(((int64_t)a) / 1000)
#define TASK_S_TO_US(a)		(((int64_t)a) * 1000000)
#define TASK_NS_TO_US(a)	(((int64_t)a) / 1000)
#define TASK_US_TO_S(a)		(((int64_t)a) / 1000000)

#define TFD_NONE 		(int64_t)(0xffffffffffffffff)
#define TFDU_NONE 		(uint64_t)(0xffffffffffffffff)

// Branch prediction optimisation macros
#if __GNUC__ >= 3
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

// Intel MP Hyper-threading friendly implementation of Spinlocks
#include <immintrin.h>
typedef struct {
	int64_t lock[8];
} __attribute__ ((aligned(64))) spin_lock_t;

static inline void spin_lock2(register int64_t volatile *p)
{
	while(unlikely(__sync_lock_test_and_set(p, 1))) {
		do { _mm_pause(); } while (*p);
	}
}

static inline void spin_unlock2(register int64_t volatile *p)
{
	__sync_lock_release(p);
}

#define spin_init(lk)   (lk).lock[0] = 0;
#define spin_lock(lk)   spin_lock2((lk).lock);
#define spin_unlock(lk) spin_unlock2((lk).lock);

// Returns 1 if the task was freed, 0 otherwise

//-------------------------------------------------------------------------------------------

TAILQ_HEAD(worker_list, worker);
TAILQ_HEAD(task_list, task);
TAILQ_HEAD(ntfyq_list, ntfyq);

typedef enum {
	TASK_TIME_PRECISE,
	TASK_TIME_COARSE
} time_precision_t;

// The ordering of these tasks states is important
typedef enum {
	TASK_STATE_CREATED,		// Just created
	TASK_STATE_ACTIVE,		// In the FD table
	TASK_STATE_DESTROY,		// Waiting for worker to kill it
	TASK_STATE_UNUSED = INT32_MAX	// Not attached to anything
} task_state_t;

typedef enum {
	TASK_TYPE_IO = 0,
	TASK_TYPE_TIMER = 1,
	TASK_TYPE_CONNECT = 2,
	TASK_TYPE_ACCEPT_PARENT = 3,
	TASK_TYPE_ACCEPT_CHILD = 4,
	TASK_TYPE_NONE = INT32_MAX
} task_type_t;

typedef enum {
	TASK_READ_STATE_VECTOR = 0,
	TASK_READ_STATE_BUFFER = 1,
	TASK_READ_STATE_IDLE = INT32_MAX
} task_rd_state_t;

typedef enum {
	TASK_WRITE_STATE_VECTOR = 0,
	TASK_WRITE_STATE_BUFFER = 1,
	TASK_WRITE_STATE_IDLE = INT32_MAX
} task_wr_state_t;

enum {
	TIMER_TIME_DESTROY = -2,
	TIMER_TIME_CANCEL = -1
};

// The below are activity reference bits. The system works a bit like reference counting
// except there's a limit to the number of references, and each reference is exclusive.
// Doing it this way, protected by spinlocks, is way faster than using atomic counters
typedef enum {
	FLG_NONE =	0x00000000,		// Special no flag for certain operations
	FLG_RD	=	0x00000001,		// A read for a TFD is active in the library
	FLG_WR	=	0x00000002,		// A write for a TFD is active
	FLG_RT	=	0x00000004,		// A Read Timeout Update is active
	FLG_WT	=	0x00000008,		// A Write Timeout Update is active
	FLG_PW	=	0x00000010,		// Poll Table Entry Reference
	FLG_LU	=	0x00000020,		// Lookup Lock
	FLG_CO	=	0x00000040,		// A Connect Task is active
	FLG_LI	=	0x00000080,		// A Listen Task is active
	FLG_TM	=	0x00000100,		// A Timer Task is active
	FLG_MG	=	0x00000200,		// A Migration Event is active
	FLG_CL	=	0x00000400,		// A Close Event is active
	FLG_BND	=	0x00000800,		// A Bind operation is in progress
	FLG_PI	=	0x00001000,		// Task is waiting for a POLLIN event
	FLG_PO	=	0x00002000,		// Task is waiting for a POLLOUT event
	FLG_DBG	=	0x00004000,		// Task Debug API in progress
	FLG_CCB =	0x00008000,		// A Close CallBack API is in progress
	FLG_TC	=	0x00010000,		// A Timer Cancel API is in progress
	FLG_TD	=	0x00020000,		// A Timer Destroy API is in progress
	FLG_TS	=	0x00040000,		// A Timer Set API is in progress
	FLG_GFD	=	0x00080000,		// A Get FD operation is already in progress
	FLG_SD =	0x00100000,		// A Shutdown operation is in progress
} task_action_flag_t;

struct ntfyq {
	TAILQ_ENTRY(ntfyq)		list;
	uint64_t			tfd;
	task_action_flag_t		action;
	bool				locked;
#ifdef USE_POLL
	uint32_t			fd;
#endif
};

struct task_timer {
	int64_t			tfd;		// TFD this timer is associated with
	int64_t			expiry_us;	// The expiry time
	void			*node;		// The paired heap node
	struct task_timer	*next;		// Next cool-off list entry
	struct task_timer	*prev;		// Previous cool-off list entry
};

struct task {
	// General task information
	// 64 bytes of "hot" items before notifyqlen_locked
__attribute__ ((aligned(64))) task_action_flag_t active_flags;	// Which flags are active
	task_type_t			type;			// Type of task
	task_state_t			state;			// Operational state of the task
	int64_t				tfd;			// Task File Descriptor that identifies this task
	uint32_t			tfd_index;		// The node index in the table
	int32_t				fd;			// The actual system socket FD we're working on
	int32_t				cb_errno;		// Errno we want to propagate on callbacks
	uint32_t			io_depth;		// How many direct calls to allow before queueing
	struct epoll_event		ev;			// The current epoll events we're waiting on
	struct worker			*worker;		// The current io worker task is bound to
	uint32_t			notifyqlen;		// Number of notifyq entries this task has
	uint32_t			committed_events;	// Events verifiably committed via epoll_ctl
	int64_t				age;			// Time this task was created
	uint32_t			migrations;		// Number of times the task has migrated
	int32_t				tfd_iteration;		// The iteration on the node
	struct worker			*preferred_worker;	// To initiate task io worker migration

	//----------------------------------------------------------------------------------------------//
	//=================================    TASK CLOSURE FIELDS    ==================================//
	//----------------------------------------------------------------------------------------------//

	// The close cb to inform use to close the task.  It is automatically inherited by newly
	// accepted tasks until the user sets it to something else.
	void				*close_cb_data;		//  User data to pass to the close callback
	void				(*close_cb)(int64_t tfd, void *close_cb_data);
	bool				forward_close;		// Temporarily allow close action forwarding
	bool				registered_fd;		// If the FD was registered by the user

	//----------------------------------------------------------------------------------------------//
	//=================================    TIMER CONTROL FIELDS    =================================//
	//----------------------------------------------------------------------------------------------//

	// Timer Task Information
	struct task_timer		tm_tt;
	void				*tm_cb_data;
	void				(*tm_cb)(int64_t tfd, int64_t lateness_us, void *tm_cb_data);

	//----------------------------------------------------------------------------------------------//
	//================================    WRITE DIRECTION FIELDS   =================================//
	//----------------------------------------------------------------------------------------------//

	// Connect is treated as a special form of write
	void				*connect_cb_data;
	void				(*connect_cb)(int64_t tfd, int result, void *connect_cb_data);

	// General Write Fields
	struct task_timer		wr_tt;
	bool				wr_shut;		// If Write side is shutdown
	task_wr_state_t			wr_state;
	size_t				wr_total;
	void				*wr_cb_data;

	// Data write fields
	size_t				wr_buflen;
	size_t				wr_bufpos;
	const char			*wr_buf;
	void				(*wr_cb)(int64_t tfd, const void *buf, ssize_t result, void *wr_cb_data);

	// Data writev fields
	size_t				wrv_buflen;
	size_t				wrv_bufpos;
	const struct iovec		*wrv_iov;
	int				wrv_iovcnt;
	void				(*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data);

	//----------------------------------------------------------------------------------------------//
	//================================    READ DIRECTION FIELDS    =================================//
	//----------------------------------------------------------------------------------------------//

	// Accept is a treated as a special form of reading
	void				*accept_cb_data;
	void				(*accept_cb)(int64_t tfd, void *accept_cb_data);
	struct ntfyq_list		accept_children;

	// General Read Fields
	struct task_timer		rd_tt;
	bool				rd_shut;		// If Read side is shutdown
	task_rd_state_t			rd_state;
	size_t				rd_total;
	void				*rd_cb_data;

	// Data read fields
	size_t				rd_buflen;
	size_t				rd_bufpos;
	char				*rd_buf;
	void				(*rd_cb)(int64_t tfd, void *buf, ssize_t result, void *rd_cb_data);

	// Data readv fields
	size_t				rdv_buflen;
	size_t				rdv_bufpos;
	const struct iovec		*rdv_iov;
	int				rdv_iovcnt;
	void				(*rdv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data);

	// notifyqlen_locked is updated by an atomic count operation.  We want to keep it on its own
	// CPU cache line (64 bytes width). We stick the end of the structure for this reason
	__attribute__ ((aligned(64))) uint64_t	notifyqlen_locked;	// Number of locked notifyq entries this task has

	// For TASK_TYPE_ACCEPT tasks, addr refers to the local address we're listening on
	// For TASK_TYPE_IO/CONNECT tasks, addr refers to the remote communication address
	struct sockaddr_storage		addr;			// An IPv4/6 address
	socklen_t			addrlen;		// The valid length of the data in .addr

} __attribute__ ((aligned(64)));	// Round up to the nearest 64 byte boundary

typedef enum {
	WORKER_STATE_LIMBO = 0,
	WORKER_STATE_CREATED,
	WORKER_STATE_RUNNING,
	WORKER_STATE_BLOCKING,
	WORKER_STATE_IDLE,
	WORKER_STATE_NOTIFYING,
	WORKER_STATE_SHUTTING_DOWN,
	WORKER_STATE_DEAD
} worker_state_t;

typedef enum {
	WORKER_TYPE_IO,
	WORKER_TYPE_ACCEPT,
	WORKER_TYPE_BLOCKING
} worker_type_t;

struct instance;

struct worker {
	uint32_t			magic;
#define WORKER_MAGIC	0xa3b6c9e1

	void				*timer_queue;
	int64_t				curtime_us;

	worker_type_t			type;
	worker_state_t			state;
	worker_state_t			old_state;
	pthread_t			thr;
	uint64_t			num_tasks;
	int				evfd;
	int				epfd;
	int				notified;
	int				affined_cpu;
	uint64_t			processed_total;
	uint64_t			processed_tc;

	struct ntfyq_list		notifyq;
	struct ntfyq_list		freeq;
	uint32_t			notifyqlen;

	struct ntfyq_list		notifyq_locked;
	struct ntfyq_list		freeq_locked;
	uint32_t			notifyqlen_locked;

	// Timer cool of list stuff
	int64_t				colt_next;
	struct task_timer		*colt1;
	struct task_timer		*colt2;

#ifdef USE_POLL
	struct pollfd 			*poll_events;
	int32_t				*poll_tfds;
	uint32_t			nfds;

	struct pollfd			*poll_event_set1;
	int32_t				*poll_tfd_set1;
	struct pollfd			*poll_event_set2;
	int32_t				*poll_tfd_set2;
#endif

	// Blocking worker call info
	void				(*work_func)(void *work_data);
	void				*work_data;
	void				(*work_cb_func)(int32_t ti, void *work_cb_data);
	void				*work_cb_data;

	struct instance			*instance;
	TAILQ_ENTRY(worker)		list;

	// Put this all by itself at the end
#ifdef USE_PTHREAD_SPINLOCKS
	pthread_spinlock_t		spinlock;
#else
__attribute__ ((aligned(64))) spin_lock_t lock[1];
#endif
}  __attribute__ ((aligned(64)));

// The ordering of these tasks states is important
typedef enum {
	INSTANCE_STATE_FREE = 0,
	INSTANCE_STATE_CREATED,
	INSTANCE_STATE_RUNNING,
	INSTANCE_STATE_SHUTTING_DOWN
} instance_state_t;

struct cpuinfo {
	bool				seen;
	struct worker			**workers;
	int				worker_pos;
	int				num_workers;
};

struct instance {
	uint32_t			magic;
#define INSTANCE_MAGIC	0x487a3d67

	int64_t				curtime_us;
	int64_t				worker_idle_empty_time_us;
	int64_t				worker_idle_empty_reaped;

	bool				is_client;		// If we're observed to connect()
	bool				is_server;		// If we're observed to listen()
	bool				flip;			// Affects CPU affinity flipping
	bool				disable_affinity;	// Disables CPU affinity system

	instance_state_t		state;
	pthread_t			thr;
	pthread_mutex_t			lock;
	int				evfd;

	// TFD->task pool
	struct task			*tfd_pool;		// The total pool of tasks we can work with
	uint64_t			tfd_pool_used;		// The total number of active tasks in the pool
	uint32_t			tfd_pool_size;
#ifdef USE_PTHREAD_SPINLOCKS
__attribute__ ((aligned(64))) pthread_spinlock_t tfd_locks[TASK_MAX_TFD_LOCKS];
#else
__attribute__ ((aligned(64))) spin_lock_t tfd_locks[TASK_MAX_TFD_LOCKS];
#endif

	struct worker			*instance_worker;
	struct worker			**io_workers;
	uint32_t			cur_io_worker;
	uint32_t			num_workers_io;

	uint64_t			max_tcp_mem;
	uint64_t			per_task_sndbuf;

	// CPU/Worker Affinity Fields
	pthread_spinlock_t		cpuspin;
	int				worker_offset;
	bool				all_cpus_seen;
	bool				all_io_workers_affined;
	int				num_io_workers_affined;
	int				num_cpu_rows;
	int				num_cpus_seen;
	int				num_cpus;
	struct cpuinfo			*cpus;

	struct worker_list		workers_created;
	struct worker_list		workers_running;
	struct worker_list		workers_blocking;
	struct worker_list		workers_idle;
	struct worker_list		workers_notify;
	struct worker_list		workers_shutdown;
	struct worker_list		workers_dead;

	int				max_blocking_workers;
	int				num_blocking_workers;
	int				num_blocking_idle;

	void				(*shutdown_cb)(intptr_t ti, void *shutdown_data);
	void				*shutdown_data;
};

static inline int task_lower_event_flag(struct task *t, uint32_t flags);

static pthread_mutex_t	creation_lock = PTHREAD_MUTEX_INITIALIZER;
static bool initialised = false;
static __thread int64_t		__thr_preferred_age;
static __thread struct worker	*__thr_current_worker = NULL;		// The current IO worker.  MUST BE NULL if current thread is not an IO worker
static __thread struct worker	*__thr_preferred_worker = NULL;		// A preferred target IO worker for nested task IO
static __thread struct instance	*__thr_current_instance = NULL;
static struct instance		instances[TASK_MAX_INSTANCES];

#define	lockless_worker(w)	(w == __thr_current_worker)

#ifdef USE_PTHREAD_SPINLOCKS
#define	tfd_lock(lock_index)	pthread_spin_lock(__thr_current_instance->tfd_locks + ((lock_index) % TASK_MAX_TFD_LOCKS))
#define	tfd_unlock(lock_index)	pthread_spin_unlock(__thr_current_instance->tfd_locks + ((lock_index) % TASK_MAX_TFD_LOCKS))
#else
#define	tfd_lock(lock_index)	spin_lock(__thr_current_instance->tfd_locks[((lock_index) % TASK_MAX_TFD_LOCKS)])
#define	tfd_unlock(lock_index)	spin_unlock(__thr_current_instance->tfd_locks[((lock_index) % TASK_MAX_TFD_LOCKS)])
#endif

// ---------------------------------------------------------------------------------------------//
//					DEBUGGING STUFF						//
//----------------------------------------------------------------------------------------------//

static void
task_dump(struct task *t)
{
	fprintf(stderr, "Task Dump for TFD = %ld\n", (t->tfd & 0xffffffff));
	fprintf(stderr, "Expiry us = %ld\n", t->tm_tt.expiry_us);
	fprintf(stderr, "Type = ");
	switch(t->type) {
	case	TASK_TYPE_NONE:
		fprintf(stderr, "NONE, State = ");
		break;
	case	TASK_TYPE_IO:
		fprintf(stderr, "IO, State = ");
		break;
	case	TASK_TYPE_TIMER:
		fprintf(stderr, "TIMER, State = ");
		break;
	case	TASK_TYPE_CONNECT:
		fprintf(stderr, "CONNECT, State = ");
		break;
	case	TASK_TYPE_ACCEPT_PARENT:
		fprintf(stderr, "ACCEPT_PARENT, State = ");
		break;
	case	TASK_TYPE_ACCEPT_CHILD:
		fprintf(stderr, "ACCEPT_CHILD, State = ");
		break;
	default:
		fprintf(stderr, "BAD TYPE, State = ");
		break;
	}
	switch (t->state) {
	case	TASK_STATE_UNUSED:
		fprintf(stderr, "LIMBO\n");
		break;
	case	TASK_STATE_CREATED:
		fprintf(stderr, "CREATED\n");
		break;
	case	TASK_STATE_ACTIVE:
		fprintf(stderr, "ACTIVE\n");
		break;
	case	TASK_STATE_DESTROY:
		fprintf(stderr, "DESTROY\n");
		break;
	default:
		fprintf(stderr, "BAD STATE\n");
		break;
	}
	fprintf(stderr, "FD = %d, errno = %d, age = %ld, migrations = %d\n", t->fd, t->cb_errno, t->age, t->migrations);
	fprintf(stderr, "\n");
} // task_dump

// Uncomment the following #define to allow for inserting backtraces for debugging
//#define TASK_POOL_OP_DEBUG
#ifdef TASK_POOL_OP_DEBUG
#define	DO_BT	do_bt()
#else
#define	DO_BT	true
#endif

#ifdef TASK_POOL_OP_DEBUG
#define BT_BUF_SIZE 256

static __thread int bufpos = 0;
static __thread char buf[65536];

static void
do_bt(void)
{
	int j, nptrs;
	void *buffer[BT_BUF_SIZE];
	char **strings;

	nptrs = backtrace(buffer, BT_BUF_SIZE);
	sprintf(buf + bufpos, "backtrace() returned %d addresses\n", nptrs);
	bufpos += strlen(buf + bufpos);

	/* The call backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO)
	   would produce similar output to the following: */

	strings = backtrace_symbols(buffer, nptrs);
	if (strings == NULL) {
		perror("backtrace_symbols");
		exit(EXIT_FAILURE);
	}

	for (j = 0; j < nptrs; j++) {
		sprintf(buf + bufpos, "%s\n", strings[j]);
		bufpos += strlen(buf + bufpos);
	}

	write(1, buf, bufpos);
	bufpos = 0;

	free(strings);
}
#endif

// Get the microsecond current time into the given int64_t pointer space
static int64_t
get_time_us(time_precision_t prec)
{
	struct timespec ts[1];
	int r;

	if (likely(__thr_current_worker != NULL)) {
		if (likely(prec == TASK_TIME_COARSE)) {
			return __thr_current_worker->curtime_us;
		}
	}
	r = clock_gettime(CLOCK_MONOTONIC, ts);
	assert(r == 0);
	return (TASK_S_TO_US(ts->tv_sec) + TASK_NS_TO_US(ts->tv_nsec));
} // get_time_us


static inline void
worker_lock(struct worker *w)
{
#ifdef USE_PTHREAD_SPINLOCKS
	pthread_spin_lock(&w->spinlock);
#else
	spin_lock(w->lock[0]);
#endif
} // worker_lock


static inline void
worker_unlock(struct worker *w)
{
#ifdef USE_PTHREAD_SPINLOCKS
	pthread_spin_unlock(&w->spinlock);
#else
	spin_unlock(w->lock[0]);
#endif
} // worker_lock


static void
worker_timer_detach(register struct worker *w, register struct task_timer *tt)
{
	tt->tfd = TFD_NONE;
	if (unlikely(tt->next == NULL)) {
		// It's in the paired heap, detach it
		pheap_detach_node(w->timer_queue, tt->node);
		return;
	}

	// It's on one of the cool-off lists
	if (likely(tt->next != tt)) {
		tt->next->prev = tt->prev;
		tt->prev->next = tt->next;
		// If it's a head node, advance the head
		if (w->colt1 == tt) {
			w->colt1 = tt->next;
		} else if (w->colt2 == tt) {
			w->colt2 = tt->next;
		}
	} else {
		if (w->colt1 == tt) {
			w->colt1 = NULL;
		} else {
			w->colt2 = NULL;
		}
	}
	tt->next = NULL;
} // worker_timer_detach


static void
worker_timer_attach(register struct worker *w, register struct task_timer *tt, register int64_t tfd)
{
	register int64_t expires_in_us = tt->expiry_us - w->curtime_us;

	// Set the task-timer TFD
	tt->tfd = tfd;

	// Check if it needs to go on cool-off list 2
	if (expires_in_us > WORKER_TIME_COLT2) {
		if (likely(w->colt2 != NULL)) {
			tt->next = w->colt2;
			tt->prev = w->colt2->prev;
			tt->prev->next = tt;
			tt->next->prev = tt;
		} else {
			tt->next = tt;
			tt->prev = tt;
		}
		w->colt2 = tt;
		return;
	}

	// Check if it needs to go on cool-off list 1
	if (expires_in_us > WORKER_TIME_COLT1) {
		if (likely(w->colt1 != NULL)) {
			tt->next = w->colt1;
			tt->prev = w->colt1->prev;
			tt->prev->next = tt;
			tt->next->prev = tt;
		} else {
			tt->next = tt;
			tt->prev = tt;
		}
		w->colt1 = tt;
		return;
	}

	// Okay then, it needs to go into the paired heap directly
	if (tt->node == NULL) {
		intptr_t tfd_intptr = (intptr_t)tfd;

		tt->node = pheap_insert(w->timer_queue, (void *)tt->expiry_us, (void *)tfd_intptr);
		assert(tt->node != NULL);
	} else {
		pheap_set_key(w->timer_queue, tt->node, (void *)tt->expiry_us);
		pheap_attach_node(w->timer_queue, tt->node);
	}
} // worker_timer_attach


static void
worker_timer_switch_lists(register struct worker *w) {
	register struct task_timer *tt;

	// Drain w->colt1, and attach each entry to the pheap
	while ((tt = w->colt1) != NULL) {
		register intptr_t tfd_intptr = (intptr_t)tt->tfd;

		if (tt->next != tt) {
			tt->next->prev = tt->prev;
			tt->prev->next = tt->next;
			w->colt1 = tt->next;
		} else {
			w->colt1 = NULL;
		}
		tt->next = NULL;

		if (tt->expiry_us < 0) {
			tt->tfd = TFD_NONE;
			continue;
		}

		if (tt->node == NULL) {
			tt->node = pheap_insert(w->timer_queue, (void *)tt->expiry_us, (void *)tfd_intptr);
			assert(tt->node != NULL);
		} else {
			pheap_set_key(w->timer_queue, tt->node, (void *)tt->expiry_us);
			pheap_attach_node(w->timer_queue, tt->node);
		}
	}

	// Now move w->colt2 to w->colt1, and NULL out w->colt2
	w->colt1 = w->colt2;
	w->colt2 = NULL;
	w->colt_next = w->curtime_us + ((WORKER_TIME_COLT1 / 5) * 4);
} // worker_timer_switch_lists


static inline void
worker_timer_update(struct worker *w, struct task_timer *tt, int64_t tfd)
{
	// First detach it, if it's attached
	if (tt->tfd != TFD_NONE) {
		worker_timer_detach(w, tt);
	}

	// If we don't need to re-add, we're done
	if ((tt->expiry_us < 0) || (tfd < 0)) {
		return;
	}

	worker_timer_attach(w, tt, tfd);
} // worker_timer_update


static inline void
task_destroy_timeouts(struct task *t)
{
	struct worker *w = t->worker;

	if (w == NULL) {
		return;
	}

	// Destroy all timeouts
	t->tm_tt.expiry_us = TIMER_TIME_DESTROY;
	worker_timer_update(w, &t->tm_tt, TFD_NONE);
	if (t->tm_tt.node) {
		pheap_release_node(w->timer_queue, t->tm_tt.node);
		t->tm_tt.node = NULL;
	}

	t->wr_tt.expiry_us = TIMER_TIME_DESTROY;
	worker_timer_update(w, &t->wr_tt, TFD_NONE);
	if (t->wr_tt.node) {
		pheap_release_node(w->timer_queue, t->wr_tt.node);
		t->wr_tt.node = NULL;
	}

	t->rd_tt.expiry_us = TIMER_TIME_DESTROY;
	worker_timer_update(w, &t->rd_tt, TFD_NONE);
	if (t->rd_tt.node) {
		pheap_release_node(w->timer_queue, t->rd_tt.node);
		t->rd_tt.node = NULL;
	}
} // task_destroy_timeouts


static void
task_init(struct task *t)
{
	int32_t iteration = (t->tfd_iteration + 1) % 8388608;

	memset(t, 0, sizeof(struct task));
	t->state = TASK_STATE_UNUSED;
	t->tm_tt.expiry_us = TIMER_TIME_CANCEL;
	t->wr_tt.expiry_us = TIMER_TIME_CANCEL;
	t->rd_tt.expiry_us = TIMER_TIME_CANCEL;
	t->rd_state = TASK_READ_STATE_IDLE;
	t->wr_state = TASK_WRITE_STATE_IDLE;
	t->ev.data.u64 = TFD_NONE;	// Just means it's not in epoll_wait() list yet
	TAILQ_INIT(&t->accept_children);
	t->age = get_time_us(TASK_TIME_COARSE);		// Set the age
	t->tfd = TFD_NONE;

	t->tfd_iteration = iteration;
} // task_init


static void
task_free(struct task *t)
{
	ck_pr_dec_64(&__thr_current_instance->tfd_pool_used);
	ck_pr_dec_64(&t->worker->num_tasks);
	task_destroy_timeouts(t);
	task_init(t);
} // task_free


// Safely lowers the flag on the task.  If all flags are down and the task
// is in the DESTROY state, it decouples task from TFD table and frees it
static void
task_unlock(struct task *t, task_action_flag_t action)
{
	register uint64_t tfdi = (t->tfd & 0xffffffff);

	t->active_flags &= ~(action);
	if (unlikely(t->state == TASK_STATE_DESTROY)) {
		if ((t->active_flags == 0) && (t->notifyqlen == 0) && (t->notifyqlen_locked == 0)) {
			tfd_lock(tfdi);
			if (t->state == TASK_STATE_DESTROY) {
				if ((t->active_flags == 0) && (t->notifyqlen == 0) &&
				    (ck_pr_load_64(&t->notifyqlen_locked) == 0)) {
					task_free(t);
				}
			}
			tfd_unlock(tfdi);
		}
	}
} // task_unlock


// Meant to be called by an external calling-facing API.  As a consequence does a
// lot of validation checks
static struct task *
task_lookup(register int64_t tfd, register task_action_flag_t action)
{
	register uint32_t tfdi = (uint32_t)(tfd & 0xffffffff);
	register struct task *t;

	// Validate the instance
	if (unlikely(__thr_current_instance == NULL)) {
		errno = EOWNERDEAD;
		return NULL;
	}
	if (unlikely(__thr_current_instance->magic != INSTANCE_MAGIC)) {
		errno = EINVAL;
		return NULL;
	}
	if (unlikely(__thr_current_instance->state > INSTANCE_STATE_RUNNING)) {
		errno = EOWNERDEAD;
		return NULL;
	}

	// Validate the TFD
	if (unlikely(tfd < 0)) {
		errno = ERANGE;
		return NULL;
	}
	if (unlikely(tfdi >= __thr_current_instance->tfd_pool_size)) {
		errno = ERANGE;
		return NULL;
	}

	// Validate the flags if set
	if (unlikely(action == FLG_NONE)) {	// Cannot have no flags set
		errno = EINVAL;
		return NULL;
	}

	// Can't lookup more than one flag at once.  The correct procdure is to just
	// pick one, look that up, and then set any others when the task is returned
	if (unlikely(__builtin_popcount((uint32_t)action) > 1)) {
		errno = EINVAL;
		return NULL;
	}

	t = __thr_current_instance->tfd_pool + tfdi;

	tfd_lock(tfdi);
	if (unlikely(t == NULL)) {
		tfd_unlock(tfdi);
		errno = ENOENT;
		return NULL;
	}

	if (unlikely(t->state != TASK_STATE_ACTIVE)) {
		tfd_unlock(tfdi);
		errno = EBADF;
		return NULL;
	}

	if (unlikely(!!(action & t->active_flags))) {
		tfd_unlock(tfdi);
		errno = EINPROGRESS;
		return NULL;
	}

	t->active_flags |= action;
	tfd_unlock(tfdi);

	errno = 0;
	return t;
} // task_lookup


// Different to task_lookup in that this is expecting the action to be set, because
// that indicates a reference that MUST be there that was waiting for the caller
// to come along and claim it.  If the task is in DESTROY state though, instead
// the reference is dropped and NULL is returned, so that way all the caller needs
// to check for is a NULL response, and then it knows its job is done.  Otherwise
// the task is returned as locked with the action still set
static struct task *
task_lock(int64_t tfd, task_action_flag_t action)
{
	register struct task *t = __thr_current_instance->tfd_pool + (tfd & 0xffffffff);

	// If Task is in Destroy State, unlock and go
	if (unlikely(t->state == TASK_STATE_DESTROY)) {
		if (action != FLG_CL) {
			task_unlock(t, action);
			return NULL;
		}
	}

	// Check if the action got cancelled
	if ((t->active_flags & action) == 0) {
		task_unlock(t, action);
		return NULL;
	}
	return t;
} // task_lock


// Utterly blows a task away.  Intended only to be called in shutdown scenarios
// when the task's worker is dead, otherwise it's unsafe as all get out
static void
task_nuke(uint32_t tfdi)
{
	register struct task *t = __thr_current_instance->tfd_pool + tfdi;

	task_destroy_timeouts(t);
	task_init(t);
	ck_pr_dec_64(&__thr_current_instance->tfd_pool_used);
} // task_nuke


// Finds a free task in the task pool, and returns the TFD for it
static struct task *
task_get_free_task(void)
{
	if (__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		return NULL;
	}

	// Also ensure we're not at our max open tfd limit
	if (unlikely(__thr_current_instance->tfd_pool_used > (uint64_t)(__thr_current_instance->tfd_pool_size * 0.8))) {
		errno = EMFILE;
		return NULL;
	}
	
	for (register uint32_t attempts = 0; attempts < __thr_current_instance->tfd_pool_size; attempts++) {
		register struct task *t;
		uint32_t tfdi;

		// Try randomly twice, and then scan from there
		if (attempts < 2) {
			tfdi = (int32_t)(random() % __thr_current_instance->tfd_pool_size);
		} else {
			tfdi = ((tfdi + 1) % __thr_current_instance->tfd_pool_size);
		}

		t = __thr_current_instance->tfd_pool + tfdi;

		// Quick check without locking
		if (t->tfd != (int64_t)TFD_NONE) {
			continue;
		}

		// We have a possible candidate.  Validate and set with lock held
		tfd_lock(tfdi);
		if (t->tfd != (int64_t)TFD_NONE) {
			// Someone got to it before us, try again
			tfd_unlock(tfdi);
			continue;
		}
		// We got one!
		t->tfd = t->tfd_iteration;
		t->tfd <<= 32;
		t->tfd |= (uint64_t)tfdi;
		t->tfd_index = tfdi;
		tfd_unlock(tfdi);
		t->io_depth = 0;
		t->state = TASK_STATE_ACTIVE;
		ck_pr_inc_64(&__thr_current_instance->tfd_pool_used);
		return t;
	}

	errno = EMFILE;
	return NULL;
} // task_get_free_tfd 

// ---------------------------------------------------------------------------------------------//
// 				   CPU Affinity Managment API					//
// ---------------------------------------------------------------------------------------------//


static int
get_cpu_of_sock(int sock)
{
	struct instance *i = __thr_current_instance;
	int cpu;
	socklen_t cpusz = sizeof(cpu);

	if (i->cpus == NULL) {
		return -1;
	}

	if (getsockopt(sock, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &cpusz) < 0) {
		return -1;
	}

	if (cpu < 0) {
		return -1;
	}

	pthread_spin_lock(&i->cpuspin);
	if (i->cpus[cpu].seen == true) {
		pthread_spin_unlock(&i->cpuspin);
		return cpu;
	}
	i->cpus[cpu].seen = true;
	i->num_cpus_seen++;
	pthread_spin_unlock(&i->cpuspin);

	if (i->num_cpus_seen == i->num_cpus) {
		i->all_cpus_seen = true;
	}
	return cpu;
} // get_cpu_of_sock


static int
get_cpu_adjusted_for_direction(int cpu, bool is_client)
{
	struct instance *i = __thr_current_instance;

	// Adjust the cpu pick based on the direction
	// Outgoing connections prefer odd CPUs
	// Incoming connections prefer even CPUs
	if (is_client) {
		if (i->num_cpus > 1) {
			if ((cpu & 0x1) == 0) cpu++;
		}
	} else {
		if ((cpu & 0x1) == 1) cpu--;
	}

	return cpu;
} // get_cpu_adjusted_for_direction


static struct worker *
get_affined_worker_from_direction(bool is_client)
{
	struct instance *i = __thr_current_instance;
	struct worker *w = NULL;
	uint64_t least = UINT64_MAX;
	uint32_t nio = i->num_workers_io;

	// Try to find the next worker with the least tasks that matches our direction
	for (uint32_t wn = 0; wn < nio; wn++) {
		struct worker *tw = NULL;

		tw = i->io_workers[wn];
		if (is_client) {
			if ((tw->affined_cpu & 0x1)) {
				if (tw->num_tasks < least) {
					least = tw->num_tasks;
					w = tw;
				}
			}
		} else {
			if ((tw->affined_cpu & 0x1) == 0) {
				if (tw->num_tasks < least) {
					least = tw->num_tasks;
					w = tw;
				}
			}
		}
	}

	// Now re-scan the opposite direction.  Try to find the least worker
	// with less than 2/3 of the tasks of the least directional worker,
	// and use that instead.  There's no point excessively overloading
	// worker threads if there are other worker threads that aren't doing
	// much. In this way, we are essentially implementing a "preferred
	// direction", without doing so with total exclusivity
	least = (least / 3) * 2;

	for (uint32_t wn = 0; wn < nio; wn++) {
		struct worker *tw = NULL;

		tw = i->io_workers[wn];
		if (is_client) {
			if ((tw->affined_cpu & 0x1) == 0) {
				if (tw->num_tasks < least) {
					least = tw->num_tasks;
					w = tw;
				}
			}
		} else {
			if ((tw->affined_cpu & 0x1)) {
				if (tw->num_tasks < least) {
					least = tw->num_tasks;
					w = tw;
				}
			}
		}
	}

	return w;
} // get_affined_worker_from_direction


static void
set_worker_cpu_affinity(struct worker *w, int tcpu)
{
	cpu_set_t acs[1];

	// Set the new worker's CPU affinity to its matching CPU
	CPU_ZERO(acs);
	CPU_SET(tcpu, acs);
	if (pthread_setaffinity_np(w->thr, sizeof(cpu_set_t), acs) != 0) {
		perror("pthread_setaffinity_np failed");
	}
} // set_worker_cpu_affinity


static void
set_one_workers_affinity(int cpu)
{
	struct instance *i = __thr_current_instance;
	struct worker *w = NULL;
	int row, tcpu;
	bool outgoing;

	// Not ideal that we're holding spinlocks for this amount of time
	// but we only do it once for each IO worker, ever, so it's okay

	pthread_spin_lock(&i->cpuspin);

	// Pick a direction based on observed operation.  If we're both
	// client and server, just alternate directions each time
	if (i->is_client && i->is_server) {
		if (i->flip) {
			outgoing = true;
			i->flip = false;
		} else {
			outgoing = false;
			i->flip = true;
		}
	} else if (i->is_client) {
		outgoing = true;
	} else {
		outgoing = false;
	}

	cpu = get_cpu_adjusted_for_direction(cpu, outgoing);

	// Find a worker that isn't affined yet
	for (uint32_t n = 0; n < i->num_workers_io; n++) {
		struct worker *c;
		c = i->io_workers[n];
		if (c->affined_cpu < 0) {
			w = c;
			break;
		}
	}
	if (w == NULL) {
		i->all_io_workers_affined = true;
		pthread_spin_unlock(&i->cpuspin);
		return;
	}

	// We found a worker to affine, now bind it to a CPU
	// Scan row by row.  Fill out a row fully before moving on
	for (row = 0; row < i->num_cpu_rows; row++) {
		int s, sl = (i->num_cpus / 2);

		// Scan by 2's starting from adjusted CPU
		for (s = 0; s < sl; s++) {
			tcpu = (cpu + (s * 2)) % i->num_cpus;
			if (i->cpus[tcpu].workers[row] == NULL) {
				break;
			}
		}
		if (s < sl) {
			break;
		}

		// Try the other set of odds or evens
		for (s = 0; s < sl; s++) {
			tcpu = (cpu + 1 + (s * 2)) % i->num_cpus;
			if (i->cpus[tcpu].workers[row] == NULL) {
				break;
			}
		}
		if (s < sl) {
			break;
		}
	}

	// We got a target CPU (tcpu)!  Bind it to the row (row) and set the CPU affinity
	assert(i->cpus[tcpu].workers[row] == NULL);
	i->cpus[tcpu].num_workers++;
	i->cpus[tcpu].workers[row] = w;
	w->affined_cpu = tcpu;

	// We can finally release the lock!
	pthread_spin_unlock(&i->cpuspin);

	// Actually set the affinity now of w to tcpu
	set_worker_cpu_affinity(w, tcpu);
} // set_one_workers_affinity


static void
task_set_initial_preferred_worker(struct task *t, bool is_client)
{
	struct instance *i = __thr_current_instance;
	struct worker *tw;

	if (i->disable_affinity) {
		t->preferred_worker = NULL;
		return;
	}

	// If tw is not NULL and it's not already our
	// worker then make it the preferred one
	// Incoming TCP socket CPU affinity will be
	// set when the task actually migrates
	tw = get_affined_worker_from_direction(is_client);

	t->preferred_worker = NULL;
	if (tw && (t->worker == NULL)) {
		t->preferred_worker = tw;

		// At least set the incoming affinity for the TCP connection
		// Doesn't matter if this fails.  It's only a hint to the TCP stack
		setsockopt(t->fd, SOL_SOCKET, SO_INCOMING_CPU, &tw->affined_cpu, sizeof(tw->affined_cpu));
		return;
	}

	if (tw && (tw != t->worker)) {
		t->preferred_worker = tw;
		return;
	}

	if (tw == NULL) {
		struct worker *w = t->worker;

		// At least set the incoming affinity for the TCP connection
		// Doesn't matter if this fails.  It's only a hint to the TCP stack
		setsockopt(t->fd, SOL_SOCKET, SO_INCOMING_CPU, &w->affined_cpu, sizeof(w->affined_cpu));
		return;
	}
} // task_set_initial_preferred_worker


static void
worker_learn_cpu_affinity(struct task *t)
{
	struct instance *i = __thr_current_instance;
	int cpu;

	if (i->disable_affinity) {
		return;
	}

	// We need to continue observing
	if((cpu = get_cpu_of_sock(t->fd)) < 0) {
		return;
	}

	if (i->all_cpus_seen) {
		set_one_workers_affinity(cpu);
	}
} // worker_learn_cpu_affinity


// ---------------------------------------------------------------------------------------------//
// 				   LOCK Pool Managment API					//
// ---------------------------------------------------------------------------------------------//

static inline void
instance_lock(struct instance *i)
{
	pthread_mutex_lock(&i->lock);
} // instance_lock


static inline void
instance_unlock(struct instance *i)
{
	pthread_mutex_unlock(&i->lock);
} // instance_unlock


// ---------------------------------------------------------------------------------------------//
// 				Task FD Pool Managment API					//
// ---------------------------------------------------------------------------------------------//

static int
instance_tfd_pool_init(struct instance *i, uint32_t pool_size)
{
	uint32_t n;
	// Don't allow less than 200 tfd entries for pool size
	pool_size = (pool_size < 200) ? 200 : pool_size;

	// Make actual pool size be 140% of what was asked for hashing efficiency
	pool_size *= 1.4;

	i->tfd_pool_used = 0;
	i->tfd_pool_size = pool_size;
	i->tfd_pool = (struct task *)calloc(pool_size, sizeof(struct task));
	if (i->tfd_pool == NULL) {
		errno = ENOMEM;
		return -1;
	}
	for (n = 0; n < pool_size; n++) {
		task_init(i->tfd_pool + n);
	}
	for (n = 0; n < TASK_MAX_TFD_LOCKS; n++) {
#ifdef USE_PTHREAD_SPINLOCKS
		pthread_spin_init(i->tfd_locks + n, PTHREAD_PROCESS_PRIVATE);
#else
		spin_init(i->tfd_locks[n]);
#endif
	}
	return 0;
} // instance_tfd_pool_init


static int
notifier_write(int fd)
{
	uint64_t m = 1;
	ssize_t r;

	if (unlikely(fd < 0)) {
		return -1;
	}

	do {
		r = write(fd, &m, sizeof (m));
	} while (unlikely(unlikely(r == -1) && unlikely(errno == EINTR)));

	if (unlikely(r != sizeof (m))) {
		return -1;
	}

	return 0;
} // notifier_write


// Notify worker to wake up.  This quickly breaks it out of the epoll_wait() call
// when we want it to notice something has changed (like us adding new tasks to it)
static int
worker_notify(struct worker *w)
{
	if (unlikely(w == NULL)) {
		return -1;
	}
	if (likely(w->notified)) {
		return 0;
	}

	w->notified = 1;
	return notifier_write(w->evfd);
} // worker_notify


static int
instance_notify(struct instance *i)
{
	if (i == NULL) {
		return -1;
	}

	worker_notify(i->instance_worker);
	return notifier_write(i->evfd);
} // instance_notify


static struct ntfyq *
worker_notify_get_free_ntfyq(struct worker *w)
{
	register struct ntfyq *tq = NULL;

	if (likely(lockless_worker(w))) {
		if (likely((tq = TAILQ_FIRST(&w->freeq)) != NULL)) {
			TAILQ_REMOVE(&w->freeq, tq, list);
			return tq;
		}
	} else {
		worker_lock(w);
		if (likely((tq = TAILQ_FIRST(&w->freeq_locked)) != NULL)) {
			TAILQ_REMOVE(&w->freeq_locked, tq, list);
			worker_unlock(w);
			return tq;
		}
		worker_unlock(w);
	}

	tq = (struct ntfyq *)calloc(1, sizeof(struct ntfyq));
	if (likely(tq != NULL)) {
		return tq;
	}

	errno = ENOMEM;
	return NULL;
} // worker_notify_get_free_ntfyq


// Must be called with the task lock held. 
static bool
task_notify_action(struct task *t, task_action_flag_t action)
{
	struct worker *w = t->worker;
	register int64_t tfd = t->tfd;
	register struct ntfyq *tq = NULL;

	if (unlikely(w == NULL)) {	// This can sometimes be true during shutdown
		return false;
	}

	// No new actions can be queued once a task enters the DESTROY state
	// We will queue close actions if the t->forward_close flag is set
	if (unlikely(t->state == TASK_STATE_DESTROY)) {
		if ((action != FLG_CL) || (t->forward_close == false)) {
			return false;
		}
	} else if (action == FLG_CL) {
		t->state = TASK_STATE_DESTROY;
	}

	// Get free action from the task worker's free notifyq list
	tq = worker_notify_get_free_ntfyq(w);
	if (unlikely(tq == NULL)) {
		assert(tq != NULL);	// Out of memory
		return false;
	}

	tq->tfd = tfd;
	tq->action = action;

	if (action == FLG_RD) {
		// It's an acceptor.  Queue it in front of everything else
		if (unlikely((t->active_flags & FLG_LI) != 0)) {
			goto task_notify_action_queue_first;
		}
		goto task_notify_action_queue;
	}

	// Anything else left over falls through and is queued
task_notify_action_queue:
	if (likely(lockless_worker(w))) {
		w->notifyqlen++;
		t->notifyqlen++;
		tq->locked = false;
		TAILQ_INSERT_TAIL(&w->notifyq, tq, list);
	} else {
		ck_pr_inc_64(&t->notifyqlen_locked);
		tq->locked = true;
		worker_lock(w);
		w->notifyqlen_locked++;
		TAILQ_INSERT_TAIL(&w->notifyq_locked, tq, list);
		worker_unlock(w);
	}
	t->active_flags |= action;
	worker_notify(w);
	return true;

task_notify_action_queue_first:
	// Queue it at the head of the notifyq
	if (lockless_worker(w)) {
		register struct ntfyq *tqh = NULL;

		w->notifyqlen++;
		t->notifyqlen++;
		tq->locked = false;
		tqh = TAILQ_FIRST(&w->notifyq);
		if (tqh == NULL) {
			TAILQ_INSERT_TAIL(&w->notifyq, tq, list);
		} else {
			TAILQ_INSERT_BEFORE(tqh, tq, list);
		}
	} else {
		register struct ntfyq *tqh = NULL;

		ck_pr_inc_64(&t->notifyqlen_locked);
		tq->locked = true;
		worker_lock(w);
		w->notifyqlen_locked++;
		tqh = TAILQ_FIRST(&w->notifyq_locked);
		if (tqh == NULL) {
			TAILQ_INSERT_TAIL(&w->notifyq_locked, tq, list);
		} else {
			TAILQ_INSERT_BEFORE(tqh, tq, list);
		}
		worker_unlock(w);
	}
	t->active_flags |= action;
	worker_notify(w);
	return true;
} // task_notify_action



// ---------------------------------------------------------------------------------------------//
// 				    Callback Managment API					//
// ---------------------------------------------------------------------------------------------//


static inline void
task_cancel_timer(struct task *t, task_action_flag_t action)
{
	struct worker *w = t->worker;

	if (action & FLG_RT) {
		t->rd_tt.expiry_us = TIMER_TIME_CANCEL;
		worker_timer_update(w, &t->rd_tt, TFD_NONE);
		t->active_flags &= ~(action);
		return;
	}
	if (action & FLG_WT) {
		t->wr_tt.expiry_us = TIMER_TIME_CANCEL;
		worker_timer_update(w, &t->wr_tt, TFD_NONE);
		t->active_flags &= ~(action);
		return;
	}
	if (action & FLG_TM) {
		t->tm_tt.expiry_us = TIMER_TIME_CANCEL;
		worker_timer_update(w, &t->tm_tt, TFD_NONE);
		t->active_flags &= ~(action);
		return;
	}
	// Bad timer type
	assert(0);
} // task_cancel_timer


static void
task_cancel_write(struct task *t)
{
	task_cancel_timer(t, FLG_WT);

#ifdef USE_EPOLLET
 	// Turn off EPOLLET here onwards for safety
	t->ev.events &= ~(EPOLLET);
#endif

	task_lower_event_flag(t, EPOLLOUT);

#ifdef USE_EPOLLONESHOT
		// Ensure to call EPOLL_CTL_MOD
	if (epoll_ctl(t->worker->epfd, EPOLL_CTL_MOD, t->fd, &t->ev) == 0) {
		t->committed_events = (t->ev.events & 0xff);
	}
#endif

	t->active_flags &= ~(FLG_WR);
} // task_cancel_write


static void
task_cancel_read(struct task *t)
{
	task_cancel_timer(t, FLG_RT);

#ifdef USE_EPOLLET
	// Turn off EPOLLET here onwards for safety
	t->ev.events &= ~(EPOLLET);
#endif

	task_lower_event_flag(t, EPOLLIN);

#ifdef USE_EPOLLONESHOT
	// Ensure to call EPOLL_CTL_MOD
	if (epoll_ctl(t->worker->epfd, EPOLL_CTL_MOD, t->fd, &t->ev) == 0) {
		t->committed_events = (t->ev.events & 0xff);
	}
#endif

	t->active_flags &= ~(FLG_RD);
} // task_cancel_read


static inline void
task_do_close_cb(struct task *t, task_action_flag_t action)
{
	void (*cb)(int64_t tfd, void *close_cb_data) = t->close_cb;
	void *cb_data = t->close_cb_data;
	int err = t->cb_errno;
	int64_t tfd = t->tfd;

	// This is it boys, this is war! C'mon what are you waiting for?
	t->state = TASK_STATE_DESTROY;

	// If the user closes a connect socket, the FLG_CO doesn't carry though
	// We need to catch and set it here so the task can get properly freed
	if(t->type == TASK_TYPE_CONNECT) {
		action |= FLG_CO;
	}

	t->active_flags &= ~(FLG_CL);		// Ensure any close reference flag is gone now

	__thr_preferred_worker = NULL;

	if (t->fd >= 0) {
		// If it's a user registered socket, do not close it, just de-register it from epoll
		if (unlikely(t->registered_fd)) {
			epoll_ctl(t->worker->epfd, EPOLL_CTL_DEL, t->fd, NULL);
		} else {
			shutdown(t->fd, SHUT_RDWR);
			close(t->fd);
		}
		t->active_flags &= ~(FLG_PW | FLG_PI | FLG_PO);	// Disable All Poll Wait Flags Now
		t->fd = -1;
		task_cancel_read(t);
		task_cancel_write(t);
	}

	// Cancel all callbacks to avoid user getting notified after we're done
	t->tm_cb = NULL;
	t->wr_cb = NULL;
	t->rd_cb = NULL;
	t->wrv_cb = NULL;
	t->rdv_cb = NULL;
	t->close_cb = NULL;
	t->accept_cb = NULL;
	t->connect_cb = NULL;

	// Cancel the timeouts
	task_destroy_timeouts(t);

	// If we're a child acceptor, don't make the close callback.  The caller
	// only knows about the parent acceptor and won't know what to do with a
	// a child acceptor since it isn't even aware of its existence
	if ((cb == NULL) || (t->type == TASK_TYPE_ACCEPT_CHILD)) {
		task_unlock(t, action);
		return;
	}

	// If the instance is shutting down, don't make any callbacks
 	if (__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN) {
		task_unlock(t, action);
		return;
	}

	task_unlock(t, action);
	errno = err;
	cb(tfd, cb_data);
} // task_do_close_cb


static inline void
task_do_readv_cb(struct task *t, ssize_t result)
{
	void (*cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data) = t->rdv_cb;
	void *cb_data = t->rd_cb_data;
	const struct iovec *iov = t->rdv_iov;
	int iovcnt = t->rdv_iovcnt;
	size_t len = t->rdv_bufpos;
	int err = t->cb_errno;
	int64_t tfd = t->tfd;

	t->rd_state = TASK_READ_STATE_IDLE;
	task_cancel_timer(t, FLG_RT);
	if (likely(result > 0)) {
		t->rd_total += result;
	}

	// If we don't have a callback, just terminate the write peacefully
	if (unlikely(t->rdv_cb == NULL)) {
		return task_unlock(t, FLG_RD);
	}

	// If the instance is shutting down, don't make any callbacks
 	if (unlikely(__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN)) {
		return task_unlock(t, FLG_RD);
	}


	if (likely(t->worker == __thr_current_worker)) {
		__thr_preferred_worker = __thr_current_worker;
		__thr_preferred_age = t->age;
	} else {
		__thr_preferred_worker = NULL;
	}

	task_unlock(t, FLG_RD);

	if (likely(result >= 0)) {
		errno = 0;
	} else {
		errno = err;
	}
	cb(tfd, iov, iovcnt, len, cb_data);
	__thr_preferred_worker = NULL;
} // task_do_readv_cb


static inline void
task_do_read_cb(struct task *t, ssize_t result)
{
	void (*cb)(int64_t tfd, void *buf, ssize_t result, void *rd_cb_data) = t->rd_cb;
	void *cb_data = t->rd_cb_data;
	void *rdbuf = t->rd_buf;
	int err = t->cb_errno;
	int64_t tfd = t->tfd;

	t->rd_state = TASK_READ_STATE_IDLE;
	task_cancel_timer(t, FLG_RT);
	if (likely(result > 0)) {
		t->rd_total += result;
	}

	// If we don't have a callback, just terminate the read peacefully
	if (unlikely(t->rd_cb == NULL)) {
		return task_unlock(t, FLG_RD);
	}

	// If the instance is shutting down, don't make any callbacks
 	if (unlikely(__thr_current_instance->state > INSTANCE_STATE_RUNNING)) {
		return task_unlock(t, FLG_RD);
	}

	if (likely(t->worker == __thr_current_worker)) {
		__thr_preferred_worker = __thr_current_worker;
		__thr_preferred_age = t->age;
	} else {
		__thr_preferred_worker = NULL;
	}

	task_unlock(t, FLG_RD);

	if (likely(result >= 0)) {
		errno = 0;
	} else {
		errno = err;
	}
	cb(tfd, rdbuf, result, cb_data);
	__thr_preferred_worker = NULL;
} // task_do_read_cb


static inline void
task_do_accept_cb(struct task *t, struct task *nt)
{
	void (*cb)(int64_t tfd, void *accept_cb_data) = t->accept_cb;
	void *cb_data = t->accept_cb_data;
	int err = t->cb_errno;
	int64_t tfd = nt->tfd;

	if (unlikely(t->accept_cb == NULL)) {
		task_do_close_cb(nt, FLG_NONE);
		return;
	}

 	if (unlikely(__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN)) {
		task_do_close_cb(nt, FLG_NONE);
		return;
	}

	__thr_preferred_worker = NULL;
	task_unlock(nt, FLG_NONE);
	errno = err;
	cb(tfd, cb_data);		// t is still locked at this point
} // task_do_accept_cb


static inline void
task_do_writev_cb(struct task *t, ssize_t result)
{
	void (*cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data) = t->wrv_cb;
	const struct iovec *iov = t->wrv_iov;
	void *cb_data = t->wr_cb_data;
	int iovcnt = t->wrv_iovcnt;
	int err = t->cb_errno;
	int64_t tfd = t->tfd;

	t->wr_state = TASK_WRITE_STATE_IDLE;
	task_cancel_timer(t, FLG_WT);

	// If we don't have a callback, just terminate the write peacefully
	if (unlikely(t->wrv_cb == NULL)) {
		return task_unlock(t, FLG_WR);
	}

	// If the instance is shutting down, don't make any callbacks
 	if (unlikely(__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN)) {
		return task_unlock(t, FLG_WR);
	}

	if (likely(t->worker == __thr_current_worker)) {
		__thr_preferred_worker = __thr_current_worker;
		__thr_preferred_age = t->age;
	} else {
		__thr_preferred_worker = NULL;
	}

	task_unlock(t, FLG_WR);

	if (likely(result >= 0)) {
		errno = 0;
	} else {
		errno = err;
	}
	cb(tfd, iov, iovcnt, result, cb_data);
	__thr_preferred_worker = NULL;
} // task_do_writev_cb


static inline void
task_do_write_cb(struct task *t, ssize_t result)
{
	void (*cb)(int64_t tfd, const void *buf, ssize_t result, void *wr_cb_data) = t->wr_cb;
	void *cb_data = t->wr_cb_data;
	const void *wrbuf = t->wr_buf;
	int err = t->cb_errno;
	int64_t tfd = t->tfd;

	t->wr_state = TASK_WRITE_STATE_IDLE;
	task_cancel_timer(t, FLG_WT);

	// If we don't have a callback, just terminate the write peacefully
	if (unlikely(t->wr_cb == NULL)) {
		return task_unlock(t, FLG_WR);
	}

	// If the instance is shutting down, don't make any callbacks
 	if (unlikely(__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN)) {
		return task_unlock(t, FLG_WR);
	}

	if (likely(t->worker == __thr_current_worker)) {
		__thr_preferred_worker = __thr_current_worker;
		__thr_preferred_age = t->age;
	} else {
		__thr_preferred_worker = NULL;
	}

	task_unlock(t, FLG_WR);

	if (likely(result >= 0)) {
		errno = 0;
	} else {
		errno = err;
	}
	cb(tfd, wrbuf, result, cb_data);
	__thr_preferred_worker = NULL;
} // task_do_write_cb


static inline void
task_do_connect_cb(struct task *t, int result)
{
	void (*cb)(int64_t tfd, int result, void *connect_cb_data) = t->connect_cb;
	void *cb_data = t->connect_cb_data;
	int err = t->cb_errno;
	int64_t tfd = t->tfd;

	// Connect sets FLG_WR as well, cancel that here
	task_cancel_timer(t, FLG_WT);
	t->active_flags &= ~(FLG_WR);

	// If there's nothing the connection, all we can do it close it
	if (unlikely(cb == NULL)) {
		task_do_close_cb(t, FLG_CO);
		return;
	}

 	if (unlikely(__thr_current_instance->state == INSTANCE_STATE_SHUTTING_DOWN)) {
		close(t->fd);
		t->fd = -1;
		result = -1;
		errno = EOWNERDEAD;
	}

	__thr_preferred_worker = NULL;
	task_unlock(t, FLG_CO);

	if (result < 0) {
		errno = err;
	} else {
		errno = 0;
	}
	cb(tfd, result, cb_data);
} // task_do_connect_cb


static inline void
task_do_timeout_cb(struct task *t, task_action_flag_t action, int64_t timeout_us)
{
	void (*cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data) = NULL;
	int64_t now_us = get_time_us(TASK_TIME_PRECISE);
	struct worker *w = t->worker;
	int64_t lateness_us = 0;
	int64_t tfd = t->tfd;
	void *cb_data = NULL;

	__thr_preferred_worker = NULL;

	// Something timed out.  Assess
	switch (action) {
	case	FLG_TM:
		goto handle_timer_timeout;
	case	FLG_RT:
		goto handle_read_timeout;
	case	FLG_WT:
		goto handle_write_timeout;
	default:
		break;
	}

	// Bad action type
	assert(0);
	return;

handle_timer_timeout:
	// Check if we're just cancelling the existing timeout
	if (t->tm_tt.expiry_us < 0) {
		task_cancel_timer(t, FLG_TM);
		return task_unlock(t, FLG_NONE);
	}

	// Check if the timeout actually fired
	if (now_us >= t->tm_tt.expiry_us) {
		cb = t->tm_cb;
		cb_data = t->tm_cb_data;
		lateness_us = now_us - t->tm_tt.expiry_us;
		task_cancel_timer(t, FLG_TM);
		task_unlock(t, FLG_TM);

		if (cb == NULL) {
			// Why is there a timer timeout without a callback?
			return;
		}

		errno = ETIMEDOUT;
		cb(tfd, lateness_us, cb_data);
		errno = 0;
		return;
	}

	// Check if need to update the existing timer node
	if (t->tm_tt.expiry_us != timeout_us) {
		t->tm_tt.expiry_us = timeout_us;
		worker_timer_update(w, &t->tm_tt, t->tfd);
		t->active_flags |= action;
		return task_unlock(t, FLG_NONE);
	}

	// Nothing changed at all.  Just unlock and go
	return task_unlock(t, FLG_NONE);

handle_read_timeout:
	// Check if we're just cancelling the existing timeout
	if (t->rd_tt.expiry_us < 0) {
		task_cancel_timer(t, FLG_RT);
		return task_unlock(t, FLG_NONE);
	}

	// Check if the timeout actually fired
	if (now_us >= t->rd_tt.expiry_us) {
		t->cb_errno = ETIMEDOUT;
		if (t->rd_state == TASK_READ_STATE_BUFFER) {
			return task_do_read_cb(t, -1);
		}
		if (t->rd_state == TASK_READ_STATE_VECTOR) {
			return task_do_readv_cb(t, -1);
		}

		// No reads were active, just cancel the timer and go
		task_cancel_timer(t, FLG_RT);
		errno = 0;
		return task_unlock(t, FLG_NONE);
	}

	// Check if need to update the existing timer node
	if (t->rd_tt.expiry_us != timeout_us) {
		t->rd_tt.expiry_us = timeout_us;
		worker_timer_update(w, &t->rd_tt, t->tfd);
		t->active_flags |= action;
		return task_unlock(t, FLG_NONE);
	}

	// Nothing changed at all.  Just unlock and go
	return task_unlock(t, FLG_NONE);

handle_write_timeout:
	// Check if we're just cancelling the existing timeout
	if (t->wr_tt.expiry_us < 0) {
		task_cancel_timer(t, FLG_WT);
		return task_unlock(t, FLG_NONE);
	}

	// Check if the timeout actually fired.  Connect timeouts are also handled here
	if (now_us >= t->wr_tt.expiry_us) {
		t->cb_errno = ETIMEDOUT;
		if (t->type == TASK_TYPE_CONNECT) {
			return task_do_connect_cb(t, -1);
		}
		if (t->wr_state == TASK_WRITE_STATE_BUFFER) {
			return task_do_write_cb(t, -1);
		}
		if (t->wr_state == TASK_WRITE_STATE_VECTOR) {
			return task_do_writev_cb(t, -1);
		}

		// No writes were active, just cancel the timer and go
		task_cancel_timer(t, FLG_WT);
		errno = 0;
		return task_unlock(t, FLG_NONE);
	}

	// Check if need to update the existing timer node
	if (t->wr_tt.expiry_us != timeout_us) {
		t->wr_tt.expiry_us = timeout_us;
		worker_timer_update(w, &t->wr_tt, t->tfd);
		t->active_flags |= action;
		return task_unlock(t, FLG_NONE);
	}

	// Nothing changed at all.  Just unlock and go
	return task_unlock(t, FLG_NONE);
} // task_do_timeout_cb


// Separate any accept children from parent and mark them for destruction
static void
task_shutdown_accept_children(struct worker *w, struct task *t)
{
	struct ntfyq *tq;

	while ((tq = TAILQ_FIRST(&t->accept_children)) != NULL) {
		int64_t tfd;
		struct task *tac;	// Accept Child

		// Pull accept child off list.  We already have the task lock
		TAILQ_REMOVE(&t->accept_children, tq, list);
		tfd = tq->tfd;
		tq->tfd = TFD_NONE;
		tq->action = FLG_NONE;

		worker_lock(w);
		TAILQ_INSERT_TAIL(&w->freeq_locked, tq, list);
		worker_unlock(w);

		if ((tac = task_lock(tfd, FLG_LI)) == NULL) {
			continue;
		}

		task_do_close_cb(tac, FLG_LI);
	}
} // task_shutdown_accept_children


// Destroy all the notifications on the worker's notification queue
static void
worker_cleanup(struct worker *w)
{
	struct ntfyq_list notifyq, freeq;
	register struct ntfyq *tq;

	if (unlikely(TAILQ_EMPTY(&w->notifyq))) {
		return;		// Nothing to do
	}

	TAILQ_INIT(&notifyq);
	TAILQ_INIT(&freeq);

	// Bulk grab the queue to process.  This minimises lock contention/churn
	TAILQ_CONCAT(&notifyq, &w->notifyq, list);
	worker_lock(w);
	TAILQ_CONCAT(&notifyq, &w->notifyq_locked, list);
	TAILQ_CONCAT(&freeq, &w->freeq_locked, list);
	worker_unlock(w);

	while (likely((tq = TAILQ_FIRST(&notifyq)) != NULL)) {
		struct task *t = NULL;
		register int64_t tfd;
		task_action_flag_t action;

		TAILQ_REMOVE(&notifyq, tq, list);
		tfd = tq->tfd;
		action = tq->action;
		tq->tfd = TFD_NONE;
		tq->action = FLG_NONE;
		TAILQ_INSERT_TAIL(&freeq, tq, list);

		// Grab the task and task lock for the tfd+action tuple
		if (unlikely((t = task_lock(tfd, action)) == NULL)) {
			continue;
		}

		// Just drop all the actions we see by forwarding them to task_do_close_cb()
		t->close_cb = NULL;
		task_do_close_cb(t, action);
	}

	// Now remove all our free notification entries
	while((tq = TAILQ_FIRST(&freeq))) {
		TAILQ_REMOVE(&freeq, tq, list);
		free(tq);
	}
} // worker_cleanup


// Free up a worker's state entirely
static void
worker_destroy(struct worker *w)
{
	struct instance *i = w->instance;

	if (w == NULL) {
		return;
	}

	// Remove from instance's list of IO workers
	if (w->type == WORKER_TYPE_IO) {
		for (uint32_t n = 0; n < i->num_workers_io; n++) {
			if (i->io_workers[n] == w) {
				i->io_workers[n] = NULL;
				break;
			}
		}
	}

	// While technically 0 is a valid fd number, that's also stdin, and we're assuming that is still there
	// and will never have been chosen for any of the worker's fd's
	if (w->epfd >= 0) {
		close(w->epfd);
		w->epfd = 0;
	}
	if (w->evfd >= 0) {
		close(w->evfd);
		w->evfd = 0;
	}
	if (w->timer_queue) {
		pheap_destroy(w->timer_queue, NULL);
		w->timer_queue = NULL;
	}

#ifdef USE_POLL
	if (w->poll_event_set1) {
		free(w->poll_event_set1);
	}
	if (w->poll_event_set2) {
		free(w->poll_event_set2);
	}
	if (w->poll_tfd_set1) {
		free(w->poll_tfd_set1);
	}
	if (w->poll_tfd_set2) {
		free(w->poll_tfd_set2);
	}
#endif

#ifdef USE_PTHREAD_SPINLOCKS
	pthread_spin_destroy(&w->spinlock);
#endif
	free(w);
	w = NULL;
} // worker_destroy


// Selects an io worker to send a task to.  We just do a round robin selection
// XXX - Implement a more advanced load-balance worker selection here as needed
static struct worker *
worker_select_io_worker(struct instance *i, struct task *t, bool outgoing)
{
	struct worker *w = NULL;
	int n, nio;
	uint64_t least = UINT64_MAX;

	// Try CPU affinity method for worker selection first
	if (t && i->all_io_workers_affined && (t->fd >= 0)) {
		task_set_initial_preferred_worker(t, outgoing);
		if ((w = t->preferred_worker) != NULL) {
			t->preferred_worker = NULL;
			return w;
		}
	}

	// Select the worker with the least number of tasks
	nio = i->num_workers_io;
	for (n = 0; n < nio; n++) {
		struct worker *tw = i->io_workers[n];

		if (tw->state > WORKER_STATE_RUNNING) {
			continue;
		}
		if (tw->num_tasks < least) {
			least = tw->num_tasks;
			w = tw;
		}
	}
	return w;
} // worker_select_io_worker


// Choose the first running IO worker we find to attach to
static int
instance_assign_worker(struct instance *i)
{
	struct worker *w;

	if ((w = worker_select_io_worker(i, NULL, false)) == NULL) {
		// No available workers
		i->instance_worker = NULL;
		errno = ECHILD;
		return -1;
	}
	i->instance_worker = w;
	return 0;
} // instance_assign_worker


static void
worker_set_state(struct worker *w, worker_state_t new_state)
{
	struct instance *i;

	instance_lock(w->instance);

	// First handle lock failure for a hard instance shutdown scenario
	if ((i = w->instance) == NULL) {
		w->old_state = w->state;
		w->state = new_state;
		if (new_state == WORKER_STATE_DEAD) {
			worker_destroy(w);
		}
		return;
	}


	// If new state is same as old state, we have nothing to do
	if (w->state == new_state) {
		instance_unlock(i);
		if ((new_state == WORKER_STATE_NOTIFYING) ||
		    (new_state == WORKER_STATE_SHUTTING_DOWN) ||
		    (new_state == WORKER_STATE_DEAD)) {
			instance_notify(i);
		}
		return;
	}

	// Enforce inability to move to a new state if old state was WORKER_STATE_DEAD
	if (w->old_state == WORKER_STATE_DEAD) {
		// Ensure that the current state is WORKER_STATE_LIMBO
		assert(w->state == WORKER_STATE_LIMBO);
		instance_unlock(i);
		return;
	}

	// Remove from old state list
	switch(w->state) {
	case WORKER_STATE_LIMBO:
		// Do nothing. It shouldn't be attached to any list
		break;
	case WORKER_STATE_CREATED:
		TAILQ_REMOVE(&i->workers_created, w, list);
		break;
	case WORKER_STATE_IDLE:
		i->num_blocking_idle--;
		TAILQ_REMOVE(&i->workers_idle, w, list);
		break;
	case WORKER_STATE_RUNNING:
		TAILQ_REMOVE(&i->workers_running, w, list);
		break;
	case WORKER_STATE_BLOCKING:
		TAILQ_REMOVE(&i->workers_blocking, w, list);
		break;
	case WORKER_STATE_NOTIFYING:
		TAILQ_REMOVE(&i->workers_notify, w, list);
		break;
	case WORKER_STATE_SHUTTING_DOWN:
		TAILQ_REMOVE(&i->workers_shutdown, w, list);
		break;
	case WORKER_STATE_DEAD:
		// The only valid state to move into from WORKER_STATE_DEAD
		// is WORKER_STATE_LIMBO.  Enforce that here
		TAILQ_REMOVE(&i->workers_dead, w, list);
		new_state = WORKER_STATE_LIMBO;
		break;
	default:
		assert(0);
	}

	w->old_state = w->state;

	switch(new_state) {
	case WORKER_STATE_LIMBO:
		// Do nothing. It shouldn't be attached to any list
		break;
	case WORKER_STATE_CREATED:
		TAILQ_INSERT_TAIL(&i->workers_created, w, list);
		break;
	case WORKER_STATE_IDLE:
		i->num_blocking_idle++;
		TAILQ_INSERT_TAIL(&i->workers_idle, w, list);
		break;
	case WORKER_STATE_RUNNING:
		TAILQ_INSERT_TAIL(&i->workers_running, w, list);
		break;
	case WORKER_STATE_BLOCKING:
		TAILQ_INSERT_TAIL(&i->workers_blocking, w, list);
		break;
	case WORKER_STATE_NOTIFYING:
		TAILQ_INSERT_TAIL(&i->workers_notify, w, list);
		break;
	case WORKER_STATE_SHUTTING_DOWN:
		TAILQ_INSERT_TAIL(&i->workers_shutdown, w, list);
		break;
	case WORKER_STATE_DEAD:
		TAILQ_INSERT_TAIL(&i->workers_dead, w, list);
		break;
	default:
		assert(0);
	}

	w->state = new_state;

	// Move the instance to a new worker if we need to
	if (i->instance_worker && (i->instance_worker == w)) {
		if (w->state != WORKER_STATE_RUNNING) {
			instance_assign_worker(i);
		}
	}

	instance_unlock(i);

	// Notify instance of a worker state change as required
	if ((new_state == WORKER_STATE_NOTIFYING) ||
	    (new_state == WORKER_STATE_SHUTTING_DOWN) ||
	    (new_state == WORKER_STATE_DEAD)) {
		instance_notify(i);
	}
} // worker_set_state


static void
task_update_timer(struct task *t)
{
	struct worker *w = t->worker;

	// If we're not on the correct worker thread, queue a notify instead
	if (!lockless_worker(w)) {
		task_notify_action(t, FLG_TM);
		task_unlock(t, FLG_NONE);
		return;
	}

	// Check actions based upon any timeout changes
	if (t->tm_tt.expiry_us < 0) {
		task_cancel_timer(t, FLG_TM);
		task_unlock(t, FLG_TM);
		return;
	}

	// Check if it already expired
	if (w->curtime_us > t->tm_tt.expiry_us) {
		task_do_timeout_cb(t, FLG_TM, t->tm_tt.expiry_us);	// Releases the task lock
		return;
	}

	// Just update the timeout in the timeout system
	worker_timer_update(w, &t->tm_tt, t->tfd);
	t->active_flags |= FLG_TM;

	task_unlock(t, FLG_NONE);
} // task_update_timer


static inline void
task_activate_rd_timeout(register struct task *t)
{
	register struct worker *w = t->worker;

	// If we're not on the correct worker thread, queue a notify instead
	if (!lockless_worker(w)) {
		task_notify_action(t, FLG_RT);
		return;
	}

	// Acceptors have no timeouts
	if (unlikely(!!(t->active_flags & FLG_LI))) {
		return;
	}

	worker_timer_update(w, &t->rd_tt, t->tfd);
	if (likely(t->rd_tt.expiry_us >= 0)) {
		t->active_flags |= FLG_RT;
	} else {
		t->active_flags &= ~(FLG_RT);
	}
} // task_activate_rd_timeout


static inline void
task_activate_wr_timeout(register struct task *t)
{
	register struct worker *w = t->worker;

	// If we're not on the correct worker thread, queue a notify instead
	if (!lockless_worker(w)) {
		task_notify_action(t, FLG_WT);
		return;
	}

	worker_timer_update(w, &t->rd_tt, t->tfd);
	if (likely(t->wr_tt.expiry_us >= 0)) {
		t->active_flags |= FLG_WT;
	} else {
		t->active_flags &= ~(FLG_WT);
	}
} // task_activate_wr_timeout


#ifdef USE_POLL
// Raises the given event flag(s)
static inline int
task_raise_event_flag(struct task *t, uint32_t flags)
{
	register uint32_t tfdi = (uint32_t)(t->tfd & 0xffffffff);
	register struct tfd_pool_node *tpn = __thr_current_instance->tfd_pool + tfdi;
	struct worker *w = t->worker;
	int wnfds;

	// We only allow EPOLLIN and EPOLLOUT. We auto add EPOLLRDHUP
	if (unlikely(t->rdhup)) {
		flags &= (EPOLLIN | EPOLLOUT);
	} else {
		flags = (flags & (EPOLLIN | EPOLLOUT)) | EPOLLRDHUP;
	}

	// Grab a refcnt for what we're going to place on the poll() notification set
	pthread_spin_lock(&tpn->lock);
	tpn->refcnt++;
	pthread_spin_unlock(&tpn->lock);

	// Place the event onto the poll() notification set
	worker_lock(w);
	wnfds = w->nfds++;
	w->poll_events[wnfds].fd = t->fd;
	w->poll_events[wnfds].events = flags;
	w->poll_events[wnfds].revents = 0;
	w->poll_tfds[wnfds] = t->tfd;
	worker_unlock(w);

	return worker_notify(w);;
} // task_raise_event_flag


// Lowers the given event flag(s)
static inline int
task_lower_event_flag(struct task *t, uint32_t flags)
{
	t->ev.events &= ~flags;
	return 0;
} // task_lower_event_flag

#else

// Creates the given event flag(s)
static inline int
task_create_event_flag(register struct task *t)
{
	t->ev.data.u64 = (uint64_t)t->tfd;
	if (likely(t->rd_shut == false)) {
		t->ev.events |= EPOLLRDHUP;
	}
	t->active_flags |= FLG_PW;

#ifdef USE_EPOLLONESHOT
	t->ev.events |= EPOLLONESHOT;
#endif

	register int res = epoll_ctl(t->worker->epfd, EPOLL_CTL_ADD, t->fd, &t->ev);

#ifdef USE_EPOLLET
	// Only turn on EPOLLET AFTER the add, or we race
	// with the kernel on the initial event notification
	if (res == 0) {
		t->ev.events |= EPOLLET;
	}
#endif

	return res;
} // task_create_event_flag


// Raises the given event flag(s)
static inline int
task_raise_event_flag(struct task *t, uint32_t flags)
{
	if (t->rd_shut && t->wr_shut) {
		errno = EPIPE;
		return -1;
	}

	t->ev.events |= flags;

	if (likely(t->ev.data.u64 != TFDU_NONE)) {
		if (unlikely(epoll_ctl(t->worker->epfd, EPOLL_CTL_MOD, t->fd, &t->ev) < 0)) {
			return -1;
		}
	} else {
		// We need to to EPOLL_CTL_ADD instead
		if (task_create_event_flag(t) < 0) {
			return -1;
		}
	}

	t->committed_events = (t->ev.events & 0xff);

	if (flags & EPOLLIN) {
		if (unlikely(t->rd_shut)) {
			errno = EPIPE;
			return -1;
		}
		t->active_flags |= FLG_PI;
		task_activate_rd_timeout(t);
	}

	if (flags & EPOLLOUT) {
		if (unlikely(t->wr_shut)) {
			errno = EPIPE;
			return -1;
		}
		t->active_flags |= FLG_PO;
		task_activate_wr_timeout(t);
	}

	return 0;
} // task_raise_event_flag


// Lowers the given event flag(s)
static inline int
task_lower_event_flag(struct task *t, uint32_t flags)
{
	int res;

	// Lower the flags on the task
	t->ev.events &= ~flags;

	// If we have no TFD, we nothing else to do
	if (unlikely(t->ev.data.u64 == TFDU_NONE)) {
		return 0;
	}

	if (flags & EPOLLIN) {
		t->active_flags &= ~(FLG_PI);
	}

	if (flags & EPOLLOUT) {
		t->active_flags &= ~(FLG_PO);
	}

#ifdef USE_EPOLLONESHOT
	// If EPOLLONESHOT was set, we can bypass the call to epoll_ctl
	// if there's no other IN/OUT flag still set
	if ((flags & (EPOLLIN | EPOLLOUT)) == 0) {
		return 0;
	}
#endif

	res = epoll_ctl(t->worker->epfd, EPOLL_CTL_MOD, t->fd, &t->ev);
	if (likely(res == 0)) {
		t->committed_events = (t->ev.events & 0xff);
	}
	return res;
} // task_lower_event_flag
#endif


static struct task *
task_create(struct instance *i, int type, int fd, struct worker *w, void *close_cb_data,
	    void (*close_cb)(int64_t tfd, void *close_cb_data), bool outgoing)
{
	struct task *t;

	if (i == NULL) {
		errno = EINVAL;
		return NULL;
	}

	// Validate the FD parameter
	switch (type) {
	case TASK_TYPE_IO:
	case TASK_TYPE_ACCEPT_PARENT:
	case TASK_TYPE_ACCEPT_CHILD:
		if (fd < 0) {
			// Reject task creation for invalid fd's
			errno = EBADF;
			return NULL;
		}
		break;
	default:
		// Force FD to be invalid for other task types
		fd = -1;
	}

	// We got a new connection. Create the task state for it
	if ((t = task_get_free_task()) == NULL) {
		return NULL;
	}

	// Assign a worker
	if (w == NULL) {
		if ((w = worker_select_io_worker(i, t, outgoing)) == NULL) {
			task_free(t);
			errno = ECHILD;
			return NULL;
		}
	}
	t->worker = w;
	ck_pr_inc_64(&w->num_tasks);
	t->type = type;
	t->close_cb = close_cb;
	t->close_cb_data = close_cb_data;
	t->fd = fd;

	// task_get_free_tfd() already locked the task for us
	return t;
} // task_create


static ssize_t
task_write_vector(register struct task *t, register bool queued)
{
	// Check for EOF condition before doing anything else
	if (unlikely(t->wr_shut)) {
		if (queued) {
			t->cb_errno = EPIPE;
			task_do_writev_cb(t, -1);	// Unlocks task
			return 0;
		}
		t->rd_state = TASK_WRITE_STATE_IDLE;
		errno = EPIPE;
		return -1;
	}

	// Keep trying to write until we're done, or we're blocked
	while (t->wrv_bufpos < t->wrv_buflen) {
		ssize_t written;

		t->cb_errno = 0;
		if (t->wrv_bufpos > 0) {
			struct iovec iov[IOV_MAX];
			size_t iov_pos = 0, seek_pos = 0;
			int n = 0, iovcnt = 0;

			// Seek to the current write position within the caller provided iovec
			do {
				if ((seek_pos + t->wrv_iov[n].iov_len) > t->wrv_bufpos) {
					break;
				}
				seek_pos += t->wrv_iov[n].iov_len;
			} while (++n < t->wrv_iovcnt);

			// This must be true, otherwise our value for t->wrv_buflen is wrong/corrupted
			assert(n < t->wrv_iovcnt);

			// Now copy across the remainder into our stack local iov
			iov_pos = t->wrv_bufpos - seek_pos;
			iov[0].iov_len = t->wrv_iov[n].iov_len - iov_pos;
			iov[0].iov_base = ((char *)t->wrv_iov[n].iov_base) + iov_pos;
			for (iovcnt++, n++; n < t->wrv_iovcnt; n++, iovcnt++) {
				iov[iovcnt].iov_len = t->wrv_iov[n].iov_len;
				iov[iovcnt].iov_base = t->wrv_iov[n].iov_base;
			}

			// We've now got a copy in our local iov of the remainder of what's left to write
			written = writev(t->fd, iov, iovcnt);
		} else {
			// We have no offset.  Just use what was passed to us for speed
			written = writev(t->fd, t->wrv_iov, t->wrv_iovcnt);
		}

		if (written < 0) {
			if (errno == EAGAIN) {
				// Write blocked for now, raise EPOLLOUT and wait to be unblocked
				if (likely(task_raise_event_flag(t, EPOLLOUT) == 0)) {
					if (queued) {
						// Write is still active, don't disable its reference
						task_unlock(t, FLG_NONE);
					}
					errno = 0;
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other write or event flag raising error.  Inform caller
			if (t->wrv_bufpos > 0) {
				// If we had written something before the error.  Inform caller of how
				// much that was.  They'll have to catch the error on their next write
				errno = 0;
				if (queued) {
					t->cb_errno = 0;
					task_do_writev_cb(t, (ssize_t)t->wrv_bufpos);	// Unlocks task
					return 0;
				}
				t->wr_state = TASK_WRITE_STATE_IDLE;
				return (ssize_t)t->wrv_bufpos;
			}

			if (queued) {
				t->cb_errno = errno;
				task_do_writev_cb(t, -1);	// Unlocks task
				return 0;
			}
			t->wr_state = TASK_WRITE_STATE_IDLE;
			return -1;
		}

		// We wrote something!
		t->wrv_bufpos += written;
		t->wr_total += written;
	}

	// We wrote it all.  Make the callback
	if (queued) {
		t->cb_errno = 0;
		task_do_writev_cb(t, (ssize_t)t->wrv_bufpos);	// Unlocks task
		return 0;
	}
	errno = 0;
	t->wr_state = TASK_WRITE_STATE_IDLE;
	return (ssize_t)t->wrv_bufpos;
} // task_write_vector


// Attempt to flush out anything left in our task write buffer
static ssize_t
task_write_buffer(register struct task *t, register bool queued)
{
	register size_t max_can_do = TASK_MAX_IO_UNIT;

	// Check for EOF condition before doing anything else
	if (unlikely(t->wr_shut)) {
		if (queued) {
			t->cb_errno = EPIPE;
			task_do_write_cb(t, -1);	// Unlocks task
			return 0;
		}
		t->rd_state = TASK_WRITE_STATE_IDLE;
		errno = EPIPE;
		return -1;
	}

	// Keep trying to write until we're done, or we're blocked
	t->cb_errno = 0;
	while (t->wr_bufpos < t->wr_buflen) {
		register size_t to_write;
		register ssize_t written;

		// Restrict the amount that can be written in one go for fairness
		if (max_can_do == 0) {
			if (task_notify_action(t, FLG_WR)) {
				if (queued) {
					task_unlock(t, FLG_NONE);
				}
				return 0;
			}
			max_can_do = SIZE_MAX;
		}
		to_write = t->wr_buflen - t->wr_bufpos;
		if (to_write > max_can_do) {
			to_write = max_can_do;
		}

		written = write(t->fd, t->wr_buf + t->wr_bufpos, to_write);
		if (written < 0) {
			if (errno == EAGAIN) {
				// Write blocked for now, raise EPOLLOUT and wait to be unblocked
				if (likely(task_raise_event_flag(t, EPOLLOUT) == 0)) {
					if (queued) {
						// Write is still active, don't disable its reference
						task_unlock(t, FLG_NONE);
					}
					errno = 0;
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other write or event flag raising error.  Inform caller
			if (t->wr_bufpos > 0) {
				// If we had written something before the error.  Inform caller of how
				// much that was.  They'll have to catch the error on their next write
				errno = 0;
				if (queued) {
					t->cb_errno = 0;
					task_do_write_cb(t, (ssize_t)t->wr_bufpos);	// Unlocks task
					return 0;
				}
				t->wr_state = TASK_WRITE_STATE_IDLE;
				return (ssize_t)t->wr_bufpos;
			}

			if (queued) {
				t->cb_errno = errno;
				task_do_write_cb(t, -1);	// Unlocks task
				return 0;
			}
			t->wr_state = TASK_WRITE_STATE_IDLE;
			return -1;
		}

		// We wrote something!
		t->wr_bufpos += written;
		t->wr_total += written;
		max_can_do -= written;
	}

	// We read it all.  Make the callback
	if (queued) {
		t->cb_errno = 0;
		task_do_write_cb(t, (ssize_t)t->wr_bufpos);	// Unlocks task
		return 0;
	}
	errno = 0;
	t->wr_state = TASK_WRITE_STATE_IDLE;
	return (ssize_t)t->wr_bufpos;
} // task_write_buffer


static void
task_handle_connect_event(struct task *t)
{
	struct instance *i = __thr_current_instance;
	int err;
	socklen_t len = sizeof(int);

	assert(t->type == TASK_TYPE_CONNECT);

	if (!i->all_io_workers_affined) {
		worker_learn_cpu_affinity(t);
	}

	// Determine result of the connect
	if (getsockopt(t->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
		err = errno;
	}

	// Turn task into a regular IO task and make callback
	t->type = TASK_TYPE_IO;
	t->cb_errno = err;

	if (err != 0) {
		task_do_connect_cb(t, -1);		// Unlocks task
	} else {
		task_do_connect_cb(t, 1);		// Unlocks task
	}
} // task_handle_connect_event


static ssize_t
task_read_vector(register struct task *t, register bool queued)
{
	// Check for EOF condition before doing anything else
	if (unlikely(t->rd_shut)) {
		if (queued) {
			t->cb_errno = EPIPE;
			task_do_read_cb(t, -1);	// Unlocks task
			return 0;
		}
		t->rd_state = TASK_READ_STATE_IDLE;
		errno = EPIPE;
		return -1;
	}

	// Read what we can
	while (t->rdv_bufpos < t->rdv_buflen) {
		ssize_t reddin;

		if (t->rdv_bufpos > 0) {
			struct iovec iov[IOV_MAX];
			size_t iov_pos = 0, seek_pos = 0;
			int n = 0, iovcnt = 0;

			// Seek to the current write position within the caller provided iovec
			do {
				if ((seek_pos + t->rdv_iov[n].iov_len) > t->rdv_bufpos) {
					break;
				}
				seek_pos += t->rdv_iov[n].iov_len;
			} while (++n < t->rdv_iovcnt);

			// This must be true, otherwise our value for t->rdv_buflen is wrong/corrupted
			assert(n < t->rdv_iovcnt);

			// Now copy across the remainder into our stack local iov
			iov_pos = t->rdv_bufpos - seek_pos;
			iov[0].iov_len = t->rdv_iov[n].iov_len - iov_pos;
			iov[0].iov_base = ((char *)t->rdv_iov[n].iov_base) + iov_pos;
			for (iovcnt++, n++; n < t->rdv_iovcnt; n++, iovcnt++) {
				iov[iovcnt].iov_len = t->rdv_iov[n].iov_len;
				iov[iovcnt].iov_base = t->rdv_iov[n].iov_base;
			}

			reddin = readv(t->fd, iov, iovcnt);
		} else {
			// We have no offset.  Just use what was passed to us for speed
			reddin = readv(t->fd, t->rdv_iov, t->rdv_iovcnt);
		}

		if (reddin < 0) {
			// We read until we're blocked.
			if (errno == EAGAIN) {
				// Make a callback now if we got anything at all
				// Do not raise EPOLLIN again until user asks us to read more
				if (t->rdv_bufpos > 0) {
					if (queued) {
						t->cb_errno = 0;
						task_do_readv_cb(t, (ssize_t)t->rdv_bufpos);	// Unlocks task
						return 0;
					}
					errno = 0;
					t->rd_state = TASK_READ_STATE_IDLE;
					return (ssize_t)t->rdv_bufpos;
				}

				// We got nothing at all.  Raise EPOLLIN and wait for something
				if (task_raise_event_flag(t, EPOLLIN) == 0) {
					// Read is still active, don't disable its reference yet
					if (queued) {
						task_unlock(t, FLG_NONE);
					}
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other read or event flag raise error.
			if (queued) {
				t->cb_errno = errno;
				task_do_readv_cb(t, -1);	// Unlocks task
				return 0;
			}
			t->rd_state = TASK_READ_STATE_IDLE;
			return -1;
		}

		// End of file response check
		if (reddin == 0) {
			// Notify first if we have anything in the buffer.  The actual EOF
			// condition will have to get picked up on the next read attempt
			if (t->rdv_bufpos > 0) {
				if (queued) {
					t->cb_errno = 0;
					task_do_readv_cb(t, (ssize_t)t->rdv_bufpos);	// Unlocks task
					return 0;
				}
				errno = 0;
				t->rd_state = TASK_READ_STATE_IDLE;
				return (ssize_t)t->rdv_bufpos;
			}

			// We got nothing at all.
			if (queued) {
				t->cb_errno = EPIPE;
				task_do_readv_cb(t, -1);	// Unlocks task
				return 0;
			}
			errno = EPIPE;
			t->rd_state = TASK_READ_STATE_IDLE;
			return -1;
		}

		// We read something!
		t->rdv_bufpos += reddin;
	}

	// We read it all.  Make the callback
	if (queued) {
		t->cb_errno = 0;
		task_do_readv_cb(t, (ssize_t)t->rdv_bufpos);	// Unlocks task
		return 0;
	}
	errno = 0;
	t->rd_state = TASK_READ_STATE_IDLE;
	return (ssize_t)t->rdv_bufpos;
} // task_read_vector


// Attempt to flush out anything left in our task write buffer
static ssize_t
task_read_buffer(register struct task *t, register int queued)
{
	register size_t max_can_do = TASK_MAX_IO_UNIT;

	// Check for EOF condition before doing anything else
	if (unlikely(t->rd_shut)) {
		if (queued) {
			t->cb_errno = EPIPE;
			task_do_read_cb(t, -1);	// Unlocks task
			return 0;
		}
		t->rd_state = TASK_READ_STATE_IDLE;
		errno = EPIPE;
		return -1;
	}

	// Read what we can
	t->cb_errno = 0;
	while (t->rd_bufpos < t->rd_buflen) {
		register size_t to_read;
		register ssize_t reddin;

		// Restrict the amount that can be read in one go for fairness
		if (max_can_do == 0) {
			if (queued) {
				task_do_read_cb(t, (ssize_t)t->rd_bufpos);	// Unlocks task
				return 0;
			}
			t->rd_state = TASK_READ_STATE_IDLE;
			return (ssize_t)t->rd_bufpos;
		}

		to_read = t->rd_buflen - t->rd_bufpos;
		if (to_read > max_can_do) {
			to_read = max_can_do;
		}

		reddin = read(t->fd, t->rd_buf + t->rd_bufpos, to_read);
		if (reddin < 0) {
			// We read until we're blocked.
			t->cb_errno = 0;
			if (errno == EAGAIN) {
				// Make a callback now if we got anything at all
				// Do not raise EPOLLIN again until user asks us to read more
				if (t->rd_bufpos > 0) {
					if (queued) {
						t->cb_errno = 0;
						task_do_read_cb(t, (ssize_t)t->rd_bufpos);	// Unlocks task
						return 0;
					}
					errno = 0;
					t->rd_state = TASK_READ_STATE_IDLE;
					return (ssize_t)t->rd_bufpos;
				}

				// We got nothing at all.  Raise EPOLLIN and wait for something
				if (task_raise_event_flag(t, EPOLLIN) == 0) {
					if (queued) {
						task_unlock(t, FLG_NONE);
					}
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other read or event flag raise error.
			if (queued) {
				t->cb_errno = errno;
				task_do_read_cb(t, -1);	// Unlocks task
				return 0;
			}
			t->rd_state = TASK_READ_STATE_IDLE;
			return -1;
		}

		// End of file response check
		if (unlikely(reddin == 0)) {
			t->rd_shut = true;
			task_lower_event_flag(t, EPOLLIN);	// Ensure poll event flag is lowered

			// Notify first if we have anything in the buffer.  The actual EOF
			// condition will have to get picked up on the next read attempt
			if (t->rd_bufpos > 0) {
				if (queued) {
					t->cb_errno = 0;
					task_do_read_cb(t, (ssize_t)t->rd_bufpos);	// Unlocks task
					return 0;
				}
				errno = 0;
				t->rd_state = TASK_READ_STATE_IDLE;
				return (ssize_t)t->rd_bufpos;
			}

			// We got nothing at all.
			if (queued) {
				t->cb_errno = EPIPE;
				task_do_read_cb(t, -1);		// Unlocks task
				return 0;
			}
			t->rd_state = TASK_READ_STATE_IDLE;
			errno = EPIPE;
			return -1;
		}

		// We read something!
		t->rd_bufpos += reddin;
		max_can_do -= reddin;
	}

	// We read it all.  Make the callback
	if (queued) {
		t->cb_errno = 0;
		task_do_read_cb(t, t->rd_bufpos);		// Unlocks task
		return 0;
	}
	errno = 0;
	t->rd_state = TASK_READ_STATE_IDLE;
	return (ssize_t)t->rd_bufpos;
} // task_read_buffer


// Fast setting of non-blocking state on a socket
static int
sock_set_nonblocking(int sock)
{
	int flags[1] = {1};

	return ioctl(sock, FIONBIO, flags);
} // sock_set_nonblocking


static int
sock_set_nodelay(int sock)
{
	int flags[1] = {1};

	return ioctl(sock, FIONBIO, flags);
	return setsockopt(sock, SOL_SOCKET, TCP_NODELAY, flags, sizeof(*flags));
} // sock_set_nonblocking


static int
sock_set_sndbuf(int sock)
{
	int sndbuf_size = __thr_current_instance->per_task_sndbuf;

	// Placing an upper limit of 1MB on outgoing buffer size appears to ensure
	// that the TCP stack doesn't go into hysteresis with huge numbers of clients
	if (sndbuf_size > (TASK_MAX_IO_UNIT * 32)) {
		sndbuf_size = (TASK_MAX_IO_UNIT * 32);
	} else if (sndbuf_size < TASK_MAX_IO_UNIT) {
		sndbuf_size = TASK_MAX_IO_UNIT;
	}
	return setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(sndbuf_size));
} // sock_set_sndbuf


static int
sock_set_rcvbuf(int sock)
{
	int rcvbuf_size = __thr_current_instance->per_task_sndbuf;

	// Placing an upper limit of 1MB on outgoing buffer size appears to ensure
	// that the TCP stack doesn't go into hysteresis with huge numbers of clients
	if (rcvbuf_size > (TASK_MAX_IO_UNIT * 32)) {
		rcvbuf_size = (TASK_MAX_IO_UNIT * 32);
	} else if (rcvbuf_size < TASK_MAX_IO_UNIT) {
		rcvbuf_size = TASK_MAX_IO_UNIT;
	}
	return setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(rcvbuf_size));
} // sock_set_rcvbuf


static inline int
sock_set_linger(int sock, int on_or_off)
{
	struct linger flags[1] = {0};

	flags->l_onoff = ((on_or_off == 0) ? 0 : 1);
	return setsockopt(sock, SOL_SOCKET, SO_LINGER, flags, sizeof(*flags));
}

typedef enum {
	TASK_ACTION_RETRY = 0,
	TASK_ACTION_ABORT = 1,
	TASK_ACTION_ERROR = 2
} task_action_t;

// Process an accept event, which is basically someone having connected to one of our
// accept ports that we are listening on
static void
task_handle_accept_event(struct task *t)
{
	struct instance *i = __thr_current_instance;

	while (t->fd >= 0) {
		int cfd;
		struct task *t_new;
		struct sockaddr_storage		addr;
		socklen_t			addrlen;

		// Attempt to accept.  Break out of loop if nothing is there to pick up
		addrlen = sizeof(addrlen);
		if ((cfd = accept(t->fd, (struct sockaddr *)&addr, &addrlen)) < 0) {
			task_action_t ta;

			switch(errno) {
			case EAGAIN:
				ta = TASK_ACTION_ABORT;
#ifdef USE_EPOLLONESHOT
				// Re-arm the io event
				if (task_raise_event_flag(t, EPOLLIN) < 0) {
					perror("task_raise_event_flag");
					goto task_handle_accept_event_fail;
				}
#endif
				break;
			case EMFILE:
			case ENFILE:
				ta = TASK_ACTION_ABORT;
				break;
			case EINTR:
				ta = TASK_ACTION_RETRY;
				break;
			default:
				ta = TASK_ACTION_ERROR;
				break;
			}

			if (ta == TASK_ACTION_RETRY) {
				continue;
			} else if (ta == TASK_ACTION_ABORT) {
				break;
			}
			goto task_handle_accept_event_fail;
		}

		//  If we get a new connection, but can't inform anyone, just close it
		if (t->accept_cb == NULL) {
			close(cfd);
			continue;
		}

		// We've got a new connection!
		sock_set_nonblocking(cfd);
		sock_set_nodelay(cfd);
		sock_set_sndbuf(cfd);
		sock_set_rcvbuf(cfd);
		sock_set_linger(cfd, 0);

		// Inherit the close callback from the accept task until the user sets their
		// own.  The caller can distingish between the two by inspecting the tfd that
		// arrives on the accept close callback.  If the tfd matches the accept tfd,
		// then the close is on the accept task, otherwise it is for the new task
		if ((t_new = task_create(i, TASK_TYPE_IO, cfd, NULL, t->close_cb_data, t->close_cb, false)) == NULL) {
			// We failed to create a task structure for it :(  Inform user
			// of the failure so they're aware of us dropping connections
			if (t->accept_cb) {
				t->accept_cb(-1, t->accept_cb_data);    // t is still locked at this point
			}
			close(cfd);
			continue;
		}

		if (!i->all_io_workers_affined) {
			worker_learn_cpu_affinity(t);
		}

		t->cb_errno = 0;
		task_do_accept_cb(t, t_new);	// Unlocks t_new
	}
	task_unlock(t, FLG_NONE);
	return;

task_handle_accept_event_fail:
	// Major accept failure.  Cancel the accept task
	// Inform user that accept is now failing/gone
	t->active_flags &= ~(FLG_LI);
	task_do_close_cb(t, FLG_RD);	// Unlocks task
} // task_handle_accept_event


static void
task_handle_io_event(struct task *t, task_action_flag_t action)
{
	// At this point, we have a task, and it's already locked for us

	// Write stuff out as needed
	if (action & FLG_WR) {
		if (likely(t->wr_state == TASK_WRITE_STATE_BUFFER)) {
			task_write_buffer(t, true);		// Unlocks the task for us
		} else if (unlikely(t->type == TASK_TYPE_CONNECT)) {
			task_handle_connect_event(t);		// Unlocks the task for us
		} else if (t->wr_state == TASK_WRITE_STATE_VECTOR) {
			task_write_vector(t, true);		// Unlocks the task for us
		} else {
			// Do nothing
			task_unlock(t, action);
		}
		return;
	}

	// Read in whatever as directed
	if (action & FLG_RD) {
		if (likely(t->rd_state == TASK_READ_STATE_BUFFER)) {
			task_read_buffer(t, true);	// Unlocks the task for us
		} else if ((t->type == TASK_TYPE_ACCEPT_PARENT) ||
			   (t->type == TASK_TYPE_ACCEPT_CHILD)) {
			task_handle_accept_event(t);	// Unlocks the task for us
		} else if (t->rd_state == TASK_READ_STATE_VECTOR) {
			task_read_vector(t, true);	// Unlocks the task for us
		} else {
			// Do nothing
			task_unlock(t, action);
		}
		return;
	}

	// Task is still locked.  Unlock it here and go
	task_unlock(t, action);
} // task_handle_io_event


// Process anything that has expired on the worker's timeout queue
static void
worker_check_timeouts(struct worker *w)
{
	// Pickup new timer heap entries from the timer cool-off lists as needed
	if (w->curtime_us > w->colt_next) {
		worker_timer_switch_lists(w);
	}

	while(true) {
		void *data, *timer_node;
		int64_t tfd;
		int64_t tfd_intptr, expiry_us;
		task_action_flag_t action;
		register struct task *t;

		timer_node = pheap_get_min_node(w->timer_queue, (void **)&expiry_us, (void **)&data);
		if (unlikely(timer_node == NULL)) {
			break;
		}

		if (likely(expiry_us > w->curtime_us)) {
			// We'll assume that 1 event takes 100us to process on average
			w->processed_tc = w->processed_total + ((expiry_us - w->curtime_us) / 100);
			break;
		}

		// Okay, someone has expired.  The game is afoot Watson!
		tfd_intptr = (intptr_t)data;
		tfd = (int64_t)tfd_intptr;

		// Determine which timeout fired
		t = __thr_current_instance->tfd_pool + (tfd & 0xffffffff);
		if (unlikely(t == NULL)) {
			continue;
		}

		assert(t->worker == w);

		if (t->tm_tt.node == timer_node) {
			action = FLG_TM;
		} else if (t->rd_tt.node == timer_node) {
			action = FLG_RT;
		} else if (t->wr_tt.node == timer_node) {
			action = FLG_WT;
		} else {
			// We have a dangling node with no owner?
			assert(0);
		}

		// Check if the action got cancelled
		if ((t->active_flags & action) == 0) {
			task_cancel_timer(t, action);
			task_unlock(t, action);
			continue;
		}

		// If Task is in Destroy State, unlock and go
		if (unlikely(t->state == TASK_STATE_DESTROY)) {
			task_cancel_timer(t, action);
			task_unlock(t, action);
			continue;
		}

		task_do_timeout_cb(t, action, expiry_us);	// Unlocks the task for us
	}
} // worker_check_timeouts


// Check for any blocking thread notifications and make the appropriate
// callbacks and then move the blocking thread back to the idle state
static void
worker_handle_instance(struct instance *i)
{
	int32_t ti = i - instances;

	// Process blocking worker callbacks
	i->curtime_us = get_time_us(TASK_TIME_COARSE);
	if (!TAILQ_EMPTY(&i->workers_notify)) {
		struct worker *w;

		instance_lock(i);
		while ((w = TAILQ_FIRST(&i->workers_notify))) {
			TAILQ_REMOVE(&i->workers_notify, w, list);

			if (w->work_cb_func) {
				void (*work_cb_func)(int32_t ti, void *work_cb_data), *work_cb_data;

				work_cb_func = w->work_cb_func;
				work_cb_data = w->work_cb_data;
				w->work_cb_func = NULL;
				w->work_cb_data = NULL;

				instance_unlock(i);
				work_cb_func(ti, work_cb_data);
				instance_lock(i);
			}

			// Place the worker on the idle queue, but don't
			// notify it. It doesn't have anything to do yet
			w->state = WORKER_STATE_IDLE;
			TAILQ_INSERT_TAIL(&i->workers_idle, w, list);
		}
		instance_unlock(i);
	}

	// Start reaping workers from the idle queue if it hasn't been
	// empty for 5 minutes. After that, reap 1 thread every 5s
	if (TAILQ_EMPTY(&i->workers_idle)) {
		i->worker_idle_empty_reaped = 0;
		i->worker_idle_empty_time_us = get_time_us(TASK_TIME_COARSE);
	} else {
		int64_t idle_reap_expiry_us;

		idle_reap_expiry_us = i->worker_idle_empty_time_us;
		idle_reap_expiry_us += TASK_S_TO_US(300);
		idle_reap_expiry_us += (i->worker_idle_empty_reaped * TASK_S_TO_US(5));

		// Loop to catch up in case it's been a while
		while (i->curtime_us >= idle_reap_expiry_us) {
			struct worker *w;

			// Reap an idle worker
			instance_lock(i);
			if ((w = TAILQ_FIRST(&i->workers_idle))) {
				w->old_state = w->state;
				w->state = WORKER_STATE_SHUTTING_DOWN;
				TAILQ_REMOVE(&i->workers_idle, w, list);
				TAILQ_INSERT_TAIL(&i->workers_shutdown, w, list);
			}
			instance_unlock(i);
			if (w) {
				worker_notify(w);
			}
			i->worker_idle_empty_reaped++;
			idle_reap_expiry_us += TASK_S_TO_US(5);
		}
	}
} // worker_handle_instance


// We're being notified that something happened.  Typically this would be new tasks
// having being added. Read off the notification event.
static void
worker_handle_event(struct worker *w, uint32_t events)
{
	uint64_t c;
	struct instance *i = w->instance;

	// If we're the designated worker for instance management, then handle that now
	if (w == i->instance_worker) {
		worker_handle_instance(i);
	}

	if (w->evfd < 0) {
		return;
	}

	if (events & (EPOLLERR | EPOLLHUP)) {
		close(w->evfd);
		w->evfd = -1;
		worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
		return;
	}

	// Pull off the event notification from the input queue
	while (true) {
		int len;

		w->notified = 0;
		if ((len = read(w->evfd, (void *)&c, sizeof(c)) < 0)) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EAGAIN) {
				break;
			}
		} else if (len >= 0) {
			// We picked up the notification
			break;
		}
		// weird length read or other serious error.  Close our eventfd and set worker to shutdown
		close(w->evfd);
		w->evfd = -1;
		worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
		break;
	}
} // worker_handle_event


static void
worker_handle_task_migration(struct worker *w, struct task *t)
{
	struct worker *tw = t->preferred_worker;

	t->migrations++;
	t->preferred_worker = NULL;
	t->worker = tw;
	ck_pr_dec_64(&w->num_tasks);
	ck_pr_inc_64(&tw->num_tasks);

	// Decouple any timeout from this worker
	// Passing in TFD_NONE will de-couple, even though the expiry time is still set
	worker_timer_update(w, &t->tm_tt, TFD_NONE);
	worker_timer_update(w, &t->rd_tt, TFD_NONE);
	worker_timer_update(w, &t->wr_tt, TFD_NONE);

	// Cancel any epoll_wait on this worker, and move to new
	if ((t->fd >= 0) && (t->ev.events)) {
		if (epoll_ctl(w->epfd, EPOLL_CTL_DEL, t->fd, &t->ev) == 0) {
			t->committed_events = 0;
		}
#ifdef USE_EPOLLET
		t->ev.events &= ~(EPOLLET);		// Don't do an ADD with EPOLLET set
#endif
		if (epoll_ctl(tw->epfd, EPOLL_CTL_ADD, t->fd, &t->ev) == 0) {
			t->committed_events = (t->ev.events & 0xff);
		}
#ifdef USE_EPOLLET
		t->ev.events |= EPOLLET;		// Set EPOLLET in the task ev flags now.
#endif
	}

	// All queued notifications automatically get forwarded to the new task worker

	if (tw->affined_cpu >= 0) {
		// Doesn't matter if this fails.  It's only a hint to the TCP stack
		setsockopt(t->fd, SOL_SOCKET, SO_INCOMING_CPU, &tw->affined_cpu, sizeof(tw->affined_cpu));
	}

	// Wake up the target worker with the migration event that just happened
	task_notify_action(t, FLG_MG);
} // worker_handle_task_migration


static void
worker_do_timeout_check(register struct worker *w)
{
	w->curtime_us = get_time_us(TASK_TIME_PRECISE);
	while(true) {
		register int64_t time_to_wait_us = 0;
		register void *timer_node;
		int64_t expiry_us = 0;

		timer_node = (void *)pheap_get_min_node(w->timer_queue, (void **)&expiry_us, NULL);

		if(unlikely(timer_node == NULL)) {
			w->processed_tc = UINT64_MAX;
			return;
		}

		time_to_wait_us = expiry_us - w->curtime_us;
		if (time_to_wait_us > 0) {
			// We'll assume that 1 event takes 100us to process on average
			w->processed_tc = w->processed_total + (time_to_wait_us / 100);
			return;
		}

		// A timeout has expired, let's process that now
		worker_check_timeouts(w);
	}
} // worker_do_timeout_check


// Insert new tasks that the acceptor had passed to this worker into the
// appropriate task state.
static void
worker_process_notifyq(register struct worker *w)
{
	register uint32_t num_processed = 0;
	register struct ntfyq *tq;
	struct ntfyq_list notifyq, freeq;

	// Process nothing if we're shutting down
	// Everything will get cleaned up when the worker shuts down
	if ((__thr_current_instance->state > INSTANCE_STATE_RUNNING) || (w->state > WORKER_STATE_RUNNING)) {
		return;
	}

	if (TAILQ_EMPTY(&w->notifyq)) {
		if (likely(TAILQ_EMPTY(&w->notifyq_locked))) {
			return;		// Nothing to do
		}
	}

	// Check the locking notifyq lists
	worker_lock(w);
	if (unlikely(TAILQ_EMPTY(&w->freeq_locked))) {
		// If the locking freeq is empty, just give it our present free list
		// We'll be adding entries to our own free list at the end of this loop
		TAILQ_CONCAT(&w->freeq_locked, &w->freeq, list);
	}
	if (unlikely(!TAILQ_EMPTY(&w->notifyq_locked))) {
		// Append the locking notifyq list to our unlocked list
		TAILQ_CONCAT(&w->notifyq, &w->notifyq_locked, list);
		w->notifyqlen += w->notifyqlen_locked;
	}
	w->notifyqlen_locked = 0;
	worker_unlock(w);

	// Everything past this point is lockless
	// We use local lists because it's faster
	TAILQ_INIT(&notifyq);
	TAILQ_INIT(&freeq);
	TAILQ_CONCAT(&notifyq, &w->notifyq, list);

	w->curtime_us = get_time_us(TASK_TIME_PRECISE);
	while (likely((tq = TAILQ_FIRST(&notifyq)) != NULL)) {
		register struct task *t = NULL;
		register int64_t tfd;
		register bool locked;
		register task_action_flag_t action;

		num_processed++;

		if (unlikely(++w->processed_total >= w->processed_tc)) {
			worker_do_timeout_check(w);
		}

		TAILQ_REMOVE(&notifyq, tq, list);
		tfd = tq->tfd;
		action = tq->action;
		locked = tq->locked;
		tq->tfd = TFD_NONE;
		tq->action = FLG_NONE;
		tq->locked = false;
		TAILQ_INSERT_TAIL(&freeq, tq, list);

		t = __thr_current_instance->tfd_pool + (tfd & 0xffffffff);

		if (unlikely(locked)) {
			ck_pr_dec_64(&t->notifyqlen_locked);
		} else {
			t->notifyqlen--;
		}

		if (unlikely(tfd != t->tfd)) {
			continue;
		}

		// If Task is not in the Active State, unlock and go
		if (unlikely(t->state != TASK_STATE_ACTIVE)) {
			if (action != FLG_CL) {
				task_unlock(t, action);
				continue;
			}
		}

		// Check if the action got cancelled
		if (unlikely((t->active_flags & action) == 0)) {
			if (action != FLG_CL) {
				task_unlock(t, action);
				continue;
			}
		}

		// If this worker does not match the task's worker, then that's probably because
		// the task recently migrated.  Send the notification to the correct worker
		if(unlikely(t->worker != w)) {
			// Forward the action directly to the correct worker.  Temporarily
			// raise t->forward_close to allow FLG_CL actions through even when
			// the task is in DESTROY state
			t->forward_close = true;
			if (task_notify_action(t, action)) {
				t->forward_close = false;
				task_unlock(t, FLG_NONE);
			} else {
				// It didn't get forwarded, drop the action reference
				t->forward_close = false;
				task_unlock(t, action);
			}
			continue;
		}

		if (unlikely(action == FLG_CL)) {
			// If we're an accept parent with children then shut them down now
			if (t->type == TASK_TYPE_ACCEPT_PARENT) {
				task_shutdown_accept_children(w, t);
				task_do_close_cb(t, FLG_LI);
				continue;
			}
			task_do_close_cb(t, FLG_NONE);	// FLG_CL is always reset by task_do_close_cb
			continue;
		}

		// It's a change action from here on.  Process the action we got

		// IO actions are usually the most common. Test for them first
		if (action == FLG_WR) {
			task_handle_io_event(t, FLG_WR);	// Releases the task lock
			continue;
		}

		if (action == FLG_RD) {
			task_handle_io_event(t, FLG_RD);	// Releases the task lock
			continue;
		}

		if (action == FLG_RT) {
			task_activate_rd_timeout(t);
			task_unlock(t, FLG_NONE);
			continue;
		}

		if (action == FLG_WT) {
			task_activate_wr_timeout(t);
			task_unlock(t, FLG_NONE);
			continue;
		}

		if (likely(action == FLG_TM)) {
			task_update_timer(t);			// Releases the task lock
			continue;
		}

		// Handle a migration notification
		if (action == FLG_MG) {
			if (t->preferred_worker != NULL) {
				if (t->preferred_worker != w) {
					// We're the sender
					worker_handle_task_migration(w, t);
				} else {
					// We're the receiver
					t->preferred_worker = NULL;

					// Re-attach any timer nodes as needed
					worker_timer_update(w, &t->tm_tt, t->tfd);
					worker_timer_update(w, &t->rd_tt, t->tfd);
					worker_timer_update(w, &t->wr_tt, t->tfd);
				}
			}
			task_unlock(t, action);
			continue;
		}

		// Bad action type
		assert(0);
	}

	w->notifyqlen -= num_processed;

	// Concat any remainder back to the actual lists
	if (likely(tq != NULL)) {
		TAILQ_CONCAT(&w->notifyq, &notifyq, list);
	}

	if (likely(!TAILQ_EMPTY(&freeq))) {
		TAILQ_CONCAT(&w->freeq, &freeq, list);
	}

// Uncomment the following definition to turn on notifyqlen validation
//#define VALIDATE_QLEN
#ifdef VALIDATE_QLEN
	uint64_t actual_len = 0;
	TAILQ_FOREACH(tq, &w->notifyq, list) {
		actual_len++;
	}

	if(actual_len > 0)
		fprintf(stderr, "Actual Length = %lu\n", actual_len);
	assert(w->notifyqlen == actual_len);
#endif
} // worker_process_notifyq


// Determine the maximum time to wait in epoll_wait()
static int
get_next_epoll_timeout_ms(struct worker *w)
{
	int64_t time_to_wait_us = 0, expiry_us = 0;
	int time_to_wait_ms = TASK_MAX_EPOLL_WAIT_MS;
	void *timer_node;

	// Update the worker time
	w->curtime_us = get_time_us(TASK_TIME_PRECISE);
	worker_check_timeouts(w);

	// If we new have tasks to pickup, don't wait in epoll()
	if (!TAILQ_EMPTY(&w->notifyq)) {
		return 0;
	}

	// Grab the first item on the timer queue.  The time until it expires is the
	// time that we will wait for.  We won't wait for longer than TASK_MAX_EPOLL_WAIT_MS.
	// The reason why we might want to break out of epoll_wait() early is to keep
	// the worker state "warm" in the CPU caches
	timer_node = (void *)pheap_get_min_node(w->timer_queue, (void **)&expiry_us, NULL);

	if(likely(timer_node != NULL)) {
		// The 333 and 667 below respectively represent 1/3 and
		// 2/3 of a millisecond, expressed in microseconds
		time_to_wait_us = expiry_us - w->curtime_us;
		if (time_to_wait_us <= 0) {
			time_to_wait_ms = 0;
		} else if (TASK_US_TO_MS(time_to_wait_us - 667) > TASK_MAX_EPOLL_WAIT_MS) {
			time_to_wait_ms = TASK_MAX_EPOLL_WAIT_MS;
		} else {
			time_to_wait_ms = (int)TASK_US_TO_MS(time_to_wait_us - 333);
			if (time_to_wait_ms < 0) {
				time_to_wait_ms = 0;
			}
			if ((time_to_wait_ms == 0) && (time_to_wait_us > 667)) {
				time_to_wait_ms = 1;
			}
		}
	}

	return time_to_wait_ms;
} // get_next_epoll_timeout_ms


#ifdef USE_POLL
static void
worker_loop_io_poll(struct worker *w)
{
	while(w->state == WORKER_STATE_RUNNING) {
		struct pollfd *poll_events;
		int32_t *poll_tfds;
		int nfds, wait_time = 0;

		// Swap the FD sets
		worker_lock(w);
		poll_events = w->poll_events;
		poll_tfds = w->poll_tfds;
		nfds = w->nfds;

		if (w->poll_events == w->poll_event_set1) {
			w->poll_events = w->poll_event_set2;
			w->poll_tfds = w->poll_tfd_set2;
		} else {
			w->poll_events = w->poll_event_set1;
			w->poll_tfds = w->poll_tfd_set1;
		}
		w->nfds = 1;
		worker_unlock(w);

		// The first entry is always reserved for the worker notification FD
		poll_events[0].fd = w->evfd;
		poll_events[0].events = POLLIN;
		poll_events[0].revents = 0;
		poll_tfds[0] = -1;

		// Wait for something to happen!
		wait_time = get_next_epoll_timeout_ms(w);
		if (poll(poll_events, nfds, wait_time) < 0) {
			if (errno == EINTR) {
				continue;
			}
			perror("epoll_wait");
			worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
			continue;
		}

		// Process the worker notification first
		if (poll_events[0].revents) {
			worker_handle_event(w, poll_events[0].revents);
		}

		// Now scan through the full list of events
		for (int n = 1; likely(n < nfds); n++) {
			int32_t revents = poll_events[n].revents;
			int64_t tfd = poll_tfds[n];

			if (revents) {
				struct task *t = __thr_current_instance->tfd_pool[tfd].t;

				if (instance_tfd_pool_refcnt_dec(tfd)) {
					continue;
				}
				task_notify_action(t, action);
			} else {
				int wnfds;

				// Put the event back onto the redo list. We still have the reference
				worker_lock(w);
				wnfds = w->nfds++;
				worker_unlock(w);

				w->poll_events[wnfds].fd = poll_events[n].fd;
				w->poll_events[wnfds].events = poll_events[n].events;
				w->poll_events[wnfds].revents = 0;
				w->poll_tfds[wnfds] = tfd;
			}
		}

		// Reap any tasks that finished up here.  We do it in this two step fashion because a task
		// may have both timeout and I/O events to be processed after an epoll_wait() call and we
		// don't want to destroy the task state until everyone who needs to touch it has done so
		w->curtime_us = get_time_us(TASK_TIME_PRECISE);
		worker_check_timeouts(w);

		// Update the worker time and check for anything that's timed out
		worker_process_notifyq(w);
	}
} // worker_loop_poll

#else

static void
worker_do_io_epoll(register struct worker *w)
{
	struct epoll_event events[TASK_MAX_EVENTS];
	register int wait_time = 0, nfds = TASK_MAX_EVENTS;
	register bool do_direct = true;

	// Determine the initial time we want to be waiting in epoll for
	wait_time = get_next_epoll_timeout_ms(w);
	while (true) {
		// Disable direct callbacks if notifyq's are too long
		// It's not essential here to get an exact notifyqlen_locked
		// value, which is why we don't atomically examine it
		if ((w->notifyqlen + w->notifyqlen_locked) > TASK_MAX_EVENTS) {
			do_direct = false;
		}

		// Wait for something to happen!
		nfds = epoll_wait(w->epfd, events, TASK_MAX_EVENTS, wait_time);

		if (nfds < 0) {
			if (errno == EINTR) {
				continue;
			}
			perror("epoll_wait");
			worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
			break;
		}

		w->curtime_us = get_time_us(TASK_TIME_PRECISE);
		worker_check_timeouts(w);

		if (nfds == 0) {
			break;
		}

		// Quick load check.  If there's too much going on just put the events
		// directly onto the notify queue, otherwise, make direct callbacks
		if (nfds == TASK_MAX_EVENTS) {
			do_direct = false;
		}

		// Scan through the list of all the events we've received
		for (int n = 0; likely(n < nfds); n++) {
			register int64_t tfd = (int64_t)events[n].data.u64;
			register uint32_t tfdi = (uint32_t)(tfd & 0xffffffff);
			register uint32_t revents = events[n].events;
			register struct task *t;

			if (unlikely(++w->processed_total >= w->processed_tc)) {
				worker_do_timeout_check(w);
			}

			// Handle non-IO items first
			if (unlikely(tfd < 0)) {
				if (tfd == (int64_t)TFD_NONE) {
					worker_handle_event(w, revents);
				}
				continue;
			}

			// If Task is in Destroy State, unlock and go
			t = __thr_current_instance->tfd_pool + tfdi;
			if (unlikely(t->state != TASK_STATE_ACTIVE)) {
				task_unlock(t, FLG_PW);
				continue;
			}

			// Check if the action got cancelled
			if ((t->active_flags & FLG_PW) == 0) {
				task_unlock(t, FLG_PW);
				continue;
			}

			// Handle ERROR/HUP events first
			if (unlikely(unlikely(!!(revents & EPOLLERR)) || unlikely(!!(revents & EPOLLHUP)))) {
				// Shutdown both connection sides and force an IO event
				// which should make a system call to detect what happened
				t->rd_shut = true;
				t->wr_shut = true;
				task_lower_event_flag(t, EPOLLIN | EPOLLOUT);
				epoll_ctl(w->epfd, EPOLL_CTL_DEL, t->fd, NULL);
				if (t->active_flags & FLG_RD) {
					task_handle_io_event(t, FLG_RD);	// Releases the task
					continue;
				}
				if (t->active_flags & FLG_WR) {
					task_handle_io_event(t, FLG_WR);	// Releases the task
					continue;
				}
				task_do_close_cb(t, FLG_PW);
				continue;
			}

			// Handle RDHUP case now
			if (unlikely(!!(revents & EPOLLRDHUP))) {
				t->rd_shut = true;
				if (t->active_flags & FLG_RD) {
					task_lower_event_flag(t, EPOLLIN | EPOLLRDHUP);
					task_handle_io_event(t, FLG_RD);	// Releases the task
					continue;
				}
				task_lower_event_flag(t, EPOLLIN);
				revents &= ~(EPOLLRDHUP);
				if (revents == 0) {
					task_unlock(t, FLG_NONE);
					continue;
				}
			}

			// Nice normal events.  Yay!

			// Handle accepts directly, no queueing
			if (unlikely((t->active_flags & FLG_LI) != 0)) {
				assert(revents == EPOLLIN);
				task_handle_io_event(t, FLG_RD);
				continue;
			}

			// If we're not overloaded then do immediate callbacks
			if (do_direct) {
				if (revents & EPOLLIN) {
					if (revents & EPOLLOUT) {
						// A write is active too.  Just queue that
						task_lower_event_flag(t, EPOLLIN | EPOLLOUT);
						task_notify_action(t, FLG_WR);
					} else {
						task_lower_event_flag(t, EPOLLIN);
					}
					task_handle_io_event(t, FLG_RD);	// Releases the task
					continue;
				}

				if (revents & EPOLLOUT) {
					task_lower_event_flag(t, EPOLLOUT);
					task_handle_io_event(t, FLG_WR);	// Releases the task
					continue;
				}
			}

			// Need to queue instead. Place the events on worker notifyq
			if (revents & EPOLLIN) {
				task_lower_event_flag(t, EPOLLIN);
				task_notify_action(t, FLG_RD);
			}

			if (revents & EPOLLOUT) {
				task_lower_event_flag(t, EPOLLOUT);
				task_notify_action(t, FLG_WR);
			}

			// All the flag updates have already been handled
			task_unlock(t, FLG_NONE);
		}

		if (nfds < TASK_MAX_EVENTS) {
			break;
		}

		wait_time = 0;
	}
} // worker_do_epoll
#endif


// The main io worker loop.  One instance exists per io worker thread
static void *
worker_loop_io(void *arg)
{
	struct worker *w = (struct worker *)arg;

	__thr_current_worker = w;
	__thr_current_instance = w->instance;

	while(w->state == WORKER_STATE_RUNNING) {
#ifdef USE_POLL
		worker_do_io_poll(w);
#else
		worker_do_io_epoll(w);
#endif
		// Call it twice, because first pass can generate events
		worker_process_notifyq(w);
		worker_process_notifyq(w);
	}

	// Time to cleanup
	worker_cleanup(w);
	worker_set_state(w, WORKER_STATE_DEAD);
	return NULL;
} // worker_loop_io


// The main blocking worker loop.  One instance exists per blocking worker thread
static void *
worker_loop_blocking(void *arg)
{
	struct worker *w = (struct worker *)arg;

	__thr_current_worker = NULL;
	__thr_current_instance = w->instance;

	while((w->state == WORKER_STATE_IDLE) ||
	      (w->state == WORKER_STATE_NOTIFYING)) {
		uint64_t c;

		// Wait for something to happen! This is a blocking read
		w->notified = 0;
		int len = read(w->evfd, (void *)&c, sizeof(c));
		w->notified = 0;

		if (len < 0) {
			if (errno != EINTR) {
				perror("read");
				worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
				break;
			}
			continue;
		}

		if (len == 0) {
			continue;
		}

		if ((w->state == WORKER_STATE_SHUTTING_DOWN) ||
		    (w->state == WORKER_STATE_DEAD)) {
			break;
		}

		assert(w->state == WORKER_STATE_IDLE);
		worker_set_state(w, WORKER_STATE_BLOCKING);

		if (w->work_func) {
			void (*work_func)(void *work_data), *work_data;

			// Make the actual call to the blocking work the user wants
			work_func = w->work_func;
			work_data = w->work_data;
			w->work_func = NULL;
			w->work_data = NULL;
			work_func(work_data);

			// Set up to notify the user from the instance thread (NOT
			// this worker thread's instance) because the user may not
			// be expecting notifications to arrive from this thread
			// If there's no callback to make, just go back to idling
			if (w->state == WORKER_STATE_BLOCKING) {
				if (w->work_cb_func) {
					worker_set_state(w, WORKER_STATE_NOTIFYING);
				} else {
					worker_set_state(w, WORKER_STATE_IDLE);
				}
			}
		}
	}

	// Set our state to dead.  The instance handler is responsible for freeing our state
	worker_set_state(w, WORKER_STATE_DEAD);

	return NULL;
} // worker_loop_blocking


static int
worker_start(struct worker *w)
{
	assert(w->magic == WORKER_MAGIC);

	if ((w->type != WORKER_TYPE_IO) && (w->type != WORKER_TYPE_BLOCKING)) {
		errno = EINVAL;
		return -1;
	}

	if (w->type == WORKER_TYPE_BLOCKING) {
		worker_set_state(w, WORKER_STATE_BLOCKING);
		// Create worker thread for handling task requests
		if (pthread_create(&w->thr, NULL, worker_loop_blocking, w) != 0) {
			goto worker_start_failed;
		}
		worker_notify(w);
		return 0;
	} 

	if (w->type == WORKER_TYPE_IO) {
		worker_set_state(w, WORKER_STATE_RUNNING);
		// Create worker thread for handling task requests
		if (pthread_create(&w->thr, NULL, worker_loop_io, w) != 0) {
			goto worker_start_failed;
		}
		worker_notify(w);
		return 0;
	} 

worker_start_failed:
	worker_set_state(w, WORKER_STATE_DEAD);
	errno = ECHILD;
	return -1;
} // worker_start


// Create a new worker state and initialise it
// XXX - Implement a CPU scheduling affinity selection mechanism for better multi-processing efficiency
static struct worker *
worker_create(struct instance *i, int worker_type)
{
	struct worker *w = NULL;

	if ((worker_type != WORKER_TYPE_IO) && (worker_type != WORKER_TYPE_BLOCKING)) {
		errno = EINVAL;
		return NULL;
	}

	// Create the worker state itself
	if ((w = (struct worker *)calloc(1, sizeof(struct worker))) == NULL) {
		errno = ENOMEM;
		return NULL;
	}

	// Now initialise the worker state
	w->magic = WORKER_MAGIC;
	w->state = WORKER_STATE_LIMBO;
	w->instance = i;
	w->type = worker_type;
	w->affined_cpu = -1;
	w->curtime_us = get_time_us(TASK_TIME_PRECISE);
#ifdef USE_PTHREAD_SPINLOCKS
	pthread_spin_init(&w->spinlock, PTHREAD_PROCESS_PRIVATE);
#else
	spin_init(w->lock[0]);
#endif
	TAILQ_INIT(&w->notifyq_locked);
	TAILQ_INIT(&w->freeq_locked);
	TAILQ_INIT(&w->notifyq);
	TAILQ_INIT(&w->freeq);

	if (w->type == WORKER_TYPE_IO) {
#ifdef USE_POLL
		w->epfd = -1;
#else
		// Create epoll fd for incoming task events
		if ((w->epfd = epoll_create1(0)) < 0) {
			// epoll_create1 will set errno
			goto worker_create_failed;
		}
#endif

		// Create event fd for task event loop notifications
		if ((w->evfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
			// eventfd will set errno
			goto worker_create_failed;
		} else {
#ifndef USE_POLL
			// Register worker notification event fd against the worker epoll fd
			// Do not use EPOLLET here since we don't do selective re-arming
			struct epoll_event ev[1] = {0};

			ev->events = EPOLLIN;
			ev->data.u64 = TFD_NONE;
			if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->evfd, ev) < 0) {
				// epoll_ctl will set errno
				goto worker_create_failed;
			}
#endif
		}
	} else if (w->type == WORKER_TYPE_BLOCKING) {
		// Blocking workers use a blocking eventfd and just wait on that instead of epoll_wait
		w->epfd = -1;

		if ((w->evfd = eventfd(0, 0)) < 0) {
			// eventfd will set errno
			goto worker_create_failed;
		}
	} else {
		errno = EINVAL;
		goto worker_create_failed;
	}

#ifdef USE_POLL
	w->poll_event_set1 = calloc((i->tfd_pool_size / i->num_workers_io) * 2, sizeof(struct pollfd));
	if (w->poll_event_set1 == NULL) {
		goto worker_create_failed;
	}
	w->poll_event_set2 = calloc((i->tfd_pool_size / i->num_workers_io) * 2, sizeof(struct pollfd));
	if (w->poll_event_set2 == NULL) {
		goto worker_create_failed;
	}
	w->poll_tfd_set1 = calloc((i->tfd_pool_size / i->num_workers_io) * 2, sizeof(int32_t));
	if (w->poll_tfd_set1 == NULL) {
		goto worker_create_failed;
	}
	w->poll_tfd_set2 = calloc((i->tfd_pool_size / i->num_workers_io) * 2, sizeof(int32_t));
	if (w->poll_tfd_set2 == NULL) {
		goto worker_create_failed;
	}

	// Set up the initial set
	w->poll_events = w->poll_event_set1;
	w->poll_tfds = w->poll_tfd_set1;
	w->nfds = 1;
#endif

	// Create timer queue for the worker. Use the default keys as intptr_t comparison function
	if ((w->timer_queue = pheap_create(NULL)) == NULL) {
		errno = ENOMEM;
		goto worker_create_failed;
	}

	// Success!
	worker_set_state(w, WORKER_STATE_CREATED);
	return w;

	// Failure :(
worker_create_failed:
	worker_destroy(w);
	return NULL;
} // worker_create


// Creates a duplicate set of listeners for the given task
// across all available IO workers for the given instance.
static void
instance_listen_balance(struct task *t)
{
	struct instance *i = t->worker->instance;
	int sock_domain, sock_type, sock_protocol;
	socklen_t solen;

	if (i->num_workers_io < 2) {
		return;
	}

	// Get the domain (family)
	solen = sizeof(sock_domain);
	if (getsockopt(t->fd, SOL_SOCKET, SO_DOMAIN, (void *)&sock_domain, &solen)) {
		perror("instance_listen_balance->getsockopt->SO_DOMAIN");
		return;
	}

	// Get the socket type
	solen = sizeof(sock_type);
	if (getsockopt(t->fd, SOL_SOCKET, SO_TYPE, (void *)&sock_type, &solen)) {
		perror("instance_listen_balance->getsockopt->SO_TYPE");
		return;
	}
	sock_type |= SOCK_NONBLOCK;

	// Get the protocol
	solen = sizeof(sock_protocol);
	if (getsockopt(t->fd, SOL_SOCKET, SO_PROTOCOL, (void *)&sock_protocol, &solen)) {
		perror("instance_listen_balance->getsockopt->SO_PROTOCOL");
		return;
	}

	// Now create a task for each worker that isn't us
	for (uint32_t n = 0; n < i->num_workers_io; n++) {
		struct worker *w;
		struct task *nt;
		int nfd;

		// Grab a non-NULL worker state
		if ((w = i->io_workers[n]) == NULL) {
			continue;
		}

		// Don't assign to workers that aren't really alive
		if ((w->state != WORKER_STATE_CREATED) &&
		    (w->state != WORKER_STATE_RUNNING)) {
			continue;
		}

		// It's our own worker.  Skip it
		if (t->worker == w) {
			continue;
		}

		// Success!  We've found a candidate worker

		// Create a new socket with matching parameters
		if ((nfd = socket(sock_domain, sock_type, sock_protocol)) < 0) {
			perror("instance_listen_balance->socket");
			continue;
		}

		// Set the SO_REUSEADDR and SO_REUSEPORT flag on the socket
		int val = 1;
		if (setsockopt(nfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
			perror("instance_listen_balance->so_reuseaddr");
			close(nfd);
			continue;
		}
		if (setsockopt(nfd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val)) < 0) {
			perror("instance_listen_balance->so_reuseport");
			close(nfd);
			continue;
		}
		
		// Now bind to the same address
		if (bind(nfd, (const struct sockaddr *)&t->addr, t->addrlen) < 0) {
			perror("instance_listen_balance->bind");
			close(nfd);
			continue;
		}

		// Now listen on it
		if (listen(nfd, TASK_LISTEN_BACKLOG) < 0) {
			continue;
		}

		// Create a child task to look after the socket. Ensure to specify the worker for the new task
		if ((nt = task_create(i, TASK_TYPE_ACCEPT_CHILD, nfd, w, t->close_cb_data, t->close_cb, false)) == NULL) {
			perror("instance_listen_balance->task_create");
			continue;
		}

		// Convert new task to an acceptor type and inform task to expect incoming events
		memcpy(&nt->addr, &t->addr, t->addrlen);
		nt->addrlen = t->addrlen;
		nt->accept_cb = t->accept_cb;
		nt->accept_cb_data = t->accept_cb_data;
		nt->active_flags |= FLG_LI;
		if (task_raise_event_flag(nt, EPOLLIN) < 0) {
			perror("instance_listen_balance->task_raise_event_flag");
			task_lower_event_flag(nt, (EPOLLIN | EPOLLOUT));
			nt->state = TASK_STATE_DESTROY;
			task_unlock(nt, (task_action_flag_t)0xffff);
			continue;
		}
		struct ntfyq *ntq = worker_notify_get_free_ntfyq(w);
		ntq->tfd = nt->tfd;
		ntq->action = FLG_LI;
		TAILQ_INSERT_TAIL(&t->accept_children, ntq, list);
		task_unlock(nt, FLG_NONE);
	}
} // instance_listen_balance


// Run through list of workers.  Remove anything marked as dead and destroy its state
static void
instance_reap_workers(struct instance *i)
{
	struct worker_list workers_to_destroy;
	struct worker *w;

	TAILQ_INIT(&workers_to_destroy);

	// Move all workers to destroy scan list
	if (!TAILQ_EMPTY(&i->workers_dead)) {
		instance_lock(i);
		TAILQ_CONCAT(&workers_to_destroy, &i->workers_dead, list);
		instance_unlock(i);
	}

	// Move workers that are still alive back to the worker list
	// Destroy anything that's dead
	while ((w = TAILQ_FIRST(&workers_to_destroy))) {
		TAILQ_REMOVE(&workers_to_destroy, w, list);
		worker_destroy(w);
	}
} // instance_reap_workers


// Ask all tasks to shutdown.
static void
instance_shutdown_tasks(struct instance *i, int force)
{
	int num_open;
	(void)force;

	// Just iterates over the entire instance task pool and if it finds
	// an open task, it forces it to shut down
	do {
		num_open = 0;
		for (uint32_t n = 0; n < i->tfd_pool_size; n++) {
			struct task *t = i->tfd_pool + n;
			if (force) {
				task_nuke(n);
				num_open++;
				continue;
			}
			if (t->state == TASK_STATE_ACTIVE) {
				task_destroy_timeouts(t);
				t->state = TASK_STATE_DESTROY;
				worker_notify(t->worker);
				num_open++;
			}
		}
		if (force) {
			usleep(1000);
		}
	} while (force && (num_open > 0));
} // instance_shutdown_tasks


// Ask all workers to shutdown.
static void
instance_shutdown_workers(struct instance *i)
{
	struct worker_list workers_to_shutdown;
	struct worker *w;

	instance_lock(i);

	// Move all workers to shutdown scan list
	TAILQ_INIT(&workers_to_shutdown);
	TAILQ_CONCAT(&workers_to_shutdown, &i->workers_created, list);
	TAILQ_CONCAT(&workers_to_shutdown, &i->workers_running, list);
	TAILQ_CONCAT(&workers_to_shutdown, &i->workers_blocking, list);
	TAILQ_CONCAT(&workers_to_shutdown, &i->workers_idle, list);
	TAILQ_CONCAT(&workers_to_shutdown, &i->workers_notify, list);

	while ((w = TAILQ_FIRST(&workers_to_shutdown))) {
		w->state = WORKER_STATE_SHUTTING_DOWN;
		TAILQ_REMOVE(&workers_to_shutdown, w, list);
		TAILQ_INSERT_TAIL(&i->workers_shutdown, w, list);
		instance_unlock(i);
		worker_notify(w);
		instance_lock(i);
	}
	instance_unlock(i);
} // instance_shutdown_workers


static void
instance_destroy(struct instance *i)
{
	if ((i == NULL) || (i->magic != INSTANCE_MAGIC)) {
		return;
	}
	if (i->evfd >= 0) {
		close(i->evfd);
		i->evfd = -1;
	}
	if (i->io_workers) {
		free(i->io_workers);
		i->io_workers = NULL;
	}
	if (i->tfd_pool) {
#ifdef USE_PTHREAD_SPINLOCKS
		for (uint32_t n = 0; n < i->tfd_pool_size; n++) {
			pthread_spin_destroy(i->tfd_locks + n);
		}
#endif
		free((void *)i->tfd_pool);
		i->tfd_pool = NULL;
	}
	if (i->cpus) {
		for(int n = 0; n < i->num_cpus; n++) {
			if (i->cpus[n].workers) {
				free(i->cpus[n].workers);
			}
		}
		free(i->cpus);
		i->cpus = NULL;
	}
	if (__thr_current_instance == i) {
		__thr_current_instance = NULL;
	}
	pthread_spin_destroy(&i->cpuspin);
	memset(i, 0, sizeof(struct instance));
	i->magic = INSTANCE_MAGIC;
	i->state = INSTANCE_STATE_FREE;
} // instance_destroy


static struct instance *
instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks)
{
	uint32_t ti;
	struct instance *i;

	pthread_mutex_lock(&creation_lock);

	// Find an unused instance
	for (ti = 0; ti < TASK_MAX_INSTANCES; ti++) {
		if (instances[ti].state != INSTANCE_STATE_FREE) {
			continue;
		}
		i = instances + ti;
		break;
	}

	if (i == NULL) {
		goto instance_creation_fail;
	} else {
		memset(i, 0, sizeof(struct instance));
	}

	if ((i->io_workers = (struct worker **)calloc(num_workers_io, sizeof(struct worker *))) == NULL) {
		goto instance_creation_fail;
	}

	if (instance_tfd_pool_init(i, max_tasks) < 0) {
		goto instance_creation_fail;
	}

	i->magic = INSTANCE_MAGIC;
	i->curtime_us = get_time_us(TASK_TIME_PRECISE);

	i->state = INSTANCE_STATE_CREATED;
	i->thr = pthread_self();
	pthread_mutex_init(&i->lock, NULL);

	TAILQ_INIT(&i->workers_created);
	TAILQ_INIT(&i->workers_running);
	TAILQ_INIT(&i->workers_blocking);
	TAILQ_INIT(&i->workers_idle);
	TAILQ_INIT(&i->workers_notify);
	TAILQ_INIT(&i->workers_shutdown);
	TAILQ_INIT(&i->workers_dead);

	i->num_workers_io = num_workers_io;	// May get modified downwards later
	i->num_blocking_workers = 0;
	i->num_blocking_idle = 0;

	pthread_spin_init(&i->cpuspin, PTHREAD_PROCESS_PRIVATE);

	// CPU/Worker affinity setup
	i->num_cpus_seen = 0;
	if (num_workers_io > 1) {
		i->num_cpus = get_nprocs_conf();
		if ((i->num_cpus > 0) && ((i->cpus = calloc(i->num_cpus, sizeof(struct cpuinfo))))) {
			int rows = (num_workers_io + i->num_cpus - 1) / i->num_cpus;

			i->num_cpu_rows = rows;
			for (int n = 0; n < i->num_cpus; n++) {
				i->cpus[n].workers = (struct worker **)calloc(rows, sizeof(struct worker *));
			}
		}
	}

	i->max_blocking_workers = max_blocking_workers;

	if ((i->evfd = eventfd(0, 0)) < 0) {
		goto instance_creation_fail;
	}

	pthread_mutex_unlock(&creation_lock);
	return i;

instance_creation_fail:
	pthread_mutex_unlock(&creation_lock);
	instance_destroy(i);
	return NULL;
} // instance_create


// ---------------------------------------------------------------------------------------------//
//			EXTERNALLY VISABLE API CALLS BELOW THIS POINT				//
// ---------------------------------------------------------------------------------------------//

// ---------------------------------------------------------------------------------------------//
// 				Task Library Timeouts API					//
// ---------------------------------------------------------------------------------------------//

int64_t
TASK_get_us_time(int64_t *time_us)
{
	int64_t now_us;
	now_us = get_time_us(TASK_TIME_PRECISE);
	if (time_us) {
		*time_us = now_us;
	}
	return now_us;
} // TASK_get_us_time


// Destroys a timeout task's state
// This cannot be called against an socket-style task
int
TASK_timeout_destroy(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_TD);

	if (t == NULL) return -1;	// errno already set

	t->tm_tt.expiry_us = TIMER_TIME_DESTROY;
	task_update_timer(t);		// Releases the lock for us
	task_unlock(t, FLG_TD);
	return 0;
} // TASK_timeout_destroy


// Cancels the timeout callback against the given tfd
int
TASK_timeout_cancel(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_TC);

	if (t == NULL) return -1;	// errno already set

	t->tm_tt.expiry_us = TIMER_TIME_CANCEL;
	task_update_timer(t);		// Releases the lock for us
	task_unlock(t, FLG_TC);
	return 0;
} // TASK_timeout_cancel


// Sets the timeout on the given tfd to occur in us_from_now micro-seconds from now
// Returns error if tfd doesn't exist
// If the tfd already has a timeout set, then its value will just be updated
// The time from now cannot be negative or more than one year into the future
int
TASK_timeout_set(int64_t tfd, int64_t us_from_now, void *timeout_cb_data,
		void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_TS);

	if (t == NULL) return -1;	// errno already set

	if (us_from_now < 5000000) {
		us_from_now = (us_from_now < 0) ? 0 : us_from_now;
		t->tm_tt.expiry_us = get_time_us(TASK_TIME_PRECISE) + us_from_now;
	} else {
		us_from_now = (us_from_now > TASK_TIMEOUT_ONE_YEAR) ? TASK_TIMEOUT_ONE_YEAR : us_from_now;
		t->tm_tt.expiry_us = get_time_us(TASK_TIME_COARSE) + us_from_now;
	}

	t->tm_cb = timeout_cb;
	t->tm_cb_data = timeout_cb_data;
	task_update_timer(t);		// Releases the lock for us
	task_unlock(t, FLG_TS);
	return 0;
} // TASK_timeout_set


// Creates an arbitrary timeout task that exists independently of any socket
// The task is assigned to the current worker context automatically, or if
// this is not known, then to a random IO worker.
// The time from now cannot be negative or more than one year into the future
int64_t
TASK_timeout_create(int32_t ti, intptr_t us_from_now, void *timeout_cb_data,
		   void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data))
{
	struct instance *i;
	struct task *t;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;

	if ((i == NULL) || i->magic != INSTANCE_MAGIC) {
		errno = EINVAL;
		return -1;
	}

	__thr_current_instance = i;

	if (i->state == INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		return -1;
	}

	if ((t = task_create(i, TASK_TYPE_TIMER, -1, NULL, NULL, NULL, false)) == NULL) {
		return -1;
	}

	if (us_from_now < 5000000) {
		us_from_now = (us_from_now < 0) ? 0 : us_from_now;
		t->tm_tt.expiry_us = get_time_us(TASK_TIME_PRECISE) + us_from_now;
	} else {
		us_from_now = (us_from_now > TASK_TIMEOUT_ONE_YEAR) ? TASK_TIMEOUT_ONE_YEAR : us_from_now;
		t->tm_tt.expiry_us = get_time_us(TASK_TIME_COARSE) + us_from_now;
	}

	t->tm_cb = timeout_cb;
	t->tm_cb_data = timeout_cb_data;
	task_notify_action(t, FLG_TM);
	task_unlock(t, FLG_NONE);	// The notifyq has the reference now
	return t->tfd;
} // TASK_timeout_create


//----------------------------------------------------------------------------------------------//
// 				Task Library Socket IO API					//
//----------------------------------------------------------------------------------------------//

// Update the task expiry timeout for a socket operation
static inline void
task_socket_update_timeout(register int64_t us_from_now, int64_t *p_tm)
{
	register int64_t new_tm_us;

	if (unlikely(us_from_now < 0)) {
		new_tm_us = TIMER_TIME_CANCEL;
	} else if (us_from_now < 5000000) {
		new_tm_us = get_time_us(TASK_TIME_PRECISE) + us_from_now;
	} else if (us_from_now < TASK_TIMEOUT_ONE_YEAR) {
		// Set to the current worker time plus the us_from_now plus half
		// the worker's maximum epoll timeout.  It'll be close enough
		new_tm_us = get_time_us(TASK_TIME_COARSE) + us_from_now + (TASK_MAX_EPOLL_WAIT_MS * 500);
	} else {
		us_from_now = TASK_TIMEOUT_ONE_YEAR;
	}

	if (new_tm_us != *p_tm) {
		*p_tm = new_tm_us;
	}
} // task_socket_update_timeout


// Will write the entire contents of the supplied buffers to the given tfd, or die trying
ssize_t
TASK_socket_writev(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *wr_cb_data,
		  void (*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_WR);

	if (t == NULL) return -1;	// errno already set

	if (unlikely(t->wr_shut)) {
		task_unlock(t, FLG_WR);
		errno = EPIPE;
		return -1;
	}

	if (unlikely(t->fd < 0)) {
		task_unlock(t, FLG_WR);
		errno = EBADF;
		return -1;
	}

	if (unlikely(t->wr_state != TASK_WRITE_STATE_IDLE)) {
		task_unlock(t, FLG_WR);
		errno = EBUSY;
		return -1;
	}

	// Validate the iov arguments and determine total read length
	t->wrv_bufpos = 0;
	t->wrv_buflen = 0;
	t->wrv_iov = iov;
	t->wrv_iovcnt = iovcnt;
	if ((iovcnt < 0) || (iovcnt > IOV_MAX)) {
		task_unlock(t, FLG_WR);
		errno = EINVAL;
		return -1;
	}
	for (int n = 0; n < iovcnt; n++) {
		if (iov[n].iov_base == NULL) {
			task_unlock(t, FLG_WR);
			errno = EINVAL;
			return -1;
		}
		t->wrv_buflen += iov[n].iov_len;
	}

	// Set worker migration preference if needed
	if (__thr_preferred_worker != NULL) {
		if (__thr_preferred_worker != __thr_current_worker) {
			if (t->preferred_worker == NULL) {
				if (__thr_preferred_age < t->age) {
					// Move ourselves to the senior task worker
					t->preferred_worker = __thr_preferred_worker;
					task_notify_action(t, FLG_MG);
				}
			}
		}
	}

	t->wr_state = TASK_WRITE_STATE_VECTOR;
	t->wrv_cb = wrv_cb;
	t->wr_cb_data = wr_cb_data;

	// Check if we can call the writev handler directly
	if (t->io_depth < TASK_MAX_IO_DEPTH) {
		if (likely(lockless_worker(t->worker)) || (t->io_depth == 0)) {
			ssize_t result;

			t->io_depth++;
			if ((result = task_write_vector(t, false)) == 0) {
				goto TASK_socket_writev_queued;
			}
			task_unlock(t, FLG_WR);
			return result;
		}
	}

	// Queue the writev
	t->io_depth = 0;
	if (unlikely(task_notify_action(t, FLG_WR) == false)) {
		int err = errno;

		task_unlock(t, FLG_WR);
		errno = err;
		return -1;
	}

TASK_socket_writev_queued:
	// The operation is queued.  Update the timeout, unlock the task and go
	task_socket_update_timeout(expires_in_us, &t->wr_tt.expiry_us);
	task_unlock(t, FLG_NONE);
	return 0;
} // TASK_socket_writev


// Will write the entire contents of the supplied buffer to the given tfd, or die trying
ssize_t
TASK_socket_write(int64_t tfd, const void *wrbuf, size_t buflen, int64_t expires_in_us, void *wr_cb_data,
		 void (*wr_cb)(int64_t tfd, const void *wrbuf, ssize_t result, void *wr_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_WR);

	if (unlikely(t == NULL)) return -1;	// errno already set

	if (unlikely(t->wr_shut)) {
		task_unlock(t, FLG_WR);
		errno = EPIPE;
		return -1;
	}

	if (unlikely(t->fd < 0)) {
		task_unlock(t, FLG_WR);
		errno = EBADF;
		return -1;
	}

	if (unlikely(t->wr_state != TASK_WRITE_STATE_IDLE)) {
		task_unlock(t, FLG_WR);
		errno = EBUSY;
		return -1;
	}

	// Set worker migration preference if needed
	if (unlikely(__thr_preferred_worker && (__thr_preferred_worker != __thr_current_worker))) {
		if ((t->preferred_worker == NULL) && (__thr_preferred_age < t->age)) {
			// Move ourselves to the senior task worker
			t->preferred_worker = __thr_preferred_worker;
			task_notify_action(t, FLG_MG);
		}
	}

	t->wr_state = TASK_WRITE_STATE_BUFFER;
	t->wr_buf = wrbuf;
	t->wr_bufpos = 0;
	t->wr_buflen = buflen;
	t->wr_cb = wr_cb;
	t->wr_cb_data = wr_cb_data;

	// Check if we can call the write handler directly
	if (t->io_depth < TASK_MAX_IO_DEPTH) {
		if (likely(lockless_worker(t->worker)) || (t->io_depth == 0)) {
			ssize_t result;

			t->io_depth++;
			if ((result = task_write_buffer(t, false)) == 0) {
				goto TASK_socket_write_queued;
			}
			task_unlock(t, FLG_WR);
			return result;
		}
	}

	// Queue the write
	t->io_depth = 0;
	if (unlikely(task_notify_action(t, FLG_WR) == false)) {
		int err = errno;

		task_unlock(t, FLG_WR);
		errno = err;
		return -1;
	}

TASK_socket_write_queued:
	// The operation is queued.  Update the timeout, unlock the task and go
	task_socket_update_timeout(expires_in_us, &t->wr_tt.expiry_us);
	task_unlock(t, FLG_NONE);
	return 0;
} // TASK_socket_write


ssize_t
TASK_socket_readv(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *rd_cb_data,
		 void (*rdv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_RD);

	if (t == NULL) return -1;	// errno already set

	if (unlikely(t->rd_shut)) {
		task_unlock(t, FLG_RD);
		errno = EPIPE;
		return -1;
	}

	if (unlikely(t->fd < 0)) {
		task_unlock(t, FLG_RD);
		errno = EBADF;
		return -1;
	}

	if (unlikely(t->rd_state != TASK_READ_STATE_IDLE)) {
		task_unlock(t, FLG_RD);
		errno = EBUSY;
		return -1;
	}

	// Validate the iov arguments and determine total read length
	t->rdv_bufpos = 0;
	t->rdv_buflen = 0;
	t->rdv_iov = iov;
	t->rdv_iovcnt = iovcnt;
	if ((iovcnt < 0) || (iovcnt > IOV_MAX)) {
		task_unlock(t, FLG_RD);
		errno = EINVAL;
		return -1;
	}
	for (int n = 0; n < iovcnt; n++) {
		if (iov[n].iov_base == NULL) {
			task_unlock(t, FLG_RD);
			errno = EINVAL;
			return -1;
		}
		t->rdv_buflen += iov[n].iov_len;
	}

	// Set worker migration preference if needed
	if (__thr_preferred_worker && (__thr_preferred_worker != __thr_current_worker)) {
		if ((t->preferred_worker == NULL) && (__thr_preferred_age < t->age)) {
			// Move ourselves to the senior task worker
			t->preferred_worker = __thr_preferred_worker;
			task_notify_action(t, FLG_MG);
		}
	}

	t->rd_state = TASK_READ_STATE_VECTOR;
	t->rdv_cb = rdv_cb;
	t->rd_cb_data = rd_cb_data;

	// Check if we can call the readv handler directly
	if (t->io_depth < TASK_MAX_IO_DEPTH) {
		if (likely(lockless_worker(t->worker)) || (t->io_depth == 0)) {
			ssize_t result;

			t->io_depth++;
			if ((result = task_read_vector(t, false)) == 0) {
				goto TASK_socket_readv_queued;
			}
			task_unlock(t, FLG_RD);
			return result;
		}
	}

	// Queue the read
	t->io_depth = 0;
	if (unlikely(task_notify_action(t, FLG_RD) == false)) {
		int err = errno;

		task_unlock(t, FLG_RD);
		errno = err;
		return -1;
	}

TASK_socket_readv_queued:
	// The operation is queued.  Update the timeout, unlock the task and go
	task_socket_update_timeout(expires_in_us, &t->rd_tt.expiry_us);
	task_unlock(t, FLG_NONE);
	return 0;
} // TASK_socket_readv


ssize_t
TASK_socket_read(int64_t tfd, void *rdbuf, size_t buflen, int64_t expires_in_us, void *rd_cb_data,
		void (*rd_cb)(int64_t tfd, void *rdbuf, ssize_t result, void *rd_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_RD);

	if (unlikely(t == NULL)) return -1;	// errno already set

	if (unlikely(t->rd_shut)) {
		task_unlock(t, FLG_RD);
		errno = EPIPE;
		return -1;
	}

	if (unlikely(t->fd < 0)) {
		task_unlock(t, FLG_RD);
		errno = EBADF;
		return -1;
	}

	if (unlikely(t->rd_state != TASK_READ_STATE_IDLE)) {
		task_unlock(t, FLG_RD);
		errno = EBUSY;
		return -1;
	}

	// Set worker migration preference if needed
	if (unlikely(__thr_preferred_worker && (__thr_preferred_worker != __thr_current_worker))) {
		if ((t->preferred_worker == NULL) && (__thr_preferred_age < t->age)) {
			// Move ourselves to the senior task worker
			t->preferred_worker = __thr_preferred_worker;
			task_notify_action(t, FLG_MG);
		}
	}

	t->rd_state = TASK_READ_STATE_BUFFER;
	t->rd_buf = rdbuf;
	t->rd_bufpos = 0;
	t->rd_buflen = buflen;
	t->rd_cb = rd_cb;
	t->rd_cb_data = rd_cb_data;

	// Check if we can call the read handler directly
	if (t->io_depth < TASK_MAX_IO_DEPTH) {
		if (likely(lockless_worker(t->worker)) || (t->io_depth == 0)) {
			ssize_t result;

			t->io_depth++;
			if ((result = task_read_buffer(t, false)) == 0) {
				goto TASK_socket_read_queued;
			}
			task_unlock(t, FLG_RD);
			return result;
		}
	}

	// Queue the read
	t->io_depth = 0;
	if (unlikely(task_notify_action(t, FLG_RD) == false)) {
		int err = errno;

		task_unlock(t, FLG_RD);
		errno = err;
		return -1;
	}

TASK_socket_read_queued:
	// The operation is queued.  Update the timeout, unlock the task and go
	task_socket_update_timeout(expires_in_us, &t->rd_tt.expiry_us);
	task_unlock(t, FLG_NONE);
	return 0;
} // TASK_socket_read


//----------------------------------------------------------------------------------------------//
// 				Task Library Socket Connection API				//
//----------------------------------------------------------------------------------------------//

int
TASK_socket_get_fd(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_GFD);

	if (t == NULL) return -1;	// errno already set

	int fd = t->fd;
	task_unlock(t, FLG_GFD);
	
	return fd;
} // TASK_socket_get_fd


// Closes the tfd control over the given WebOps FD.  If an on_close_cb() has been
// registered against the tfd, then that will be call just prior to closing
// If there is a timeout registered against the tfd, it will be automatically cancelled
// If called against a timeout-only task, it just calls TASK_timeout_destroy(tfd);
int
TASK_close(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_CL);

	if (t == NULL) return -1;	// errno already set

	// Cancel all timers and schedule task destruction
	t->tm_tt.expiry_us = TIMER_TIME_CANCEL;
	t->rd_tt.expiry_us = TIMER_TIME_CANCEL;
	t->wr_tt.expiry_us = TIMER_TIME_CANCEL;
	task_notify_action(t, FLG_CL);
	task_unlock(t, FLG_NONE);
	return 0;
} // TASK_close


int
TASK_socket_shutdown(int64_t tfd, int how)
{
	register struct task *t = task_lookup(tfd, FLG_SD);
	int res;

	if (t == NULL) return -1;	// errno already set

	if (t->type == TASK_TYPE_TIMER) {
		task_unlock(t, FLG_SD);
		errno = EINVAL;
		return -1;
	}

	if (t->fd < 0) {
		task_unlock(t, FLG_SD);
		errno = EBADF;
		return -1;
	}

	if (how == SHUT_RD) {
		t->rd_shut = true;
	} else if (how == SHUT_WR) {
		t->wr_shut = true;
	} else if (how == SHUT_RDWR) {
		t->rd_shut = true;
		t->wr_shut = true;
	}
	res = shutdown(t->fd, how);
	task_unlock(t, FLG_SD);
	return res;
} // TASK_socket_shutdown


// Listen for and accept new connections on a given addr
int
TASK_socket_listen(int64_t tfd, void *accept_cb_data, void (*accept_cb)(int64_t tfd, void *accept_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_LI);

	if (t == NULL) return -1;	// errno already set

	// Convert this task to a parent acceptor type and inform task to expect incoming events
	t->type = TASK_TYPE_ACCEPT_PARENT;
	t->accept_cb = accept_cb;
	t->accept_cb_data = accept_cb_data;
	__thr_current_instance->is_server = true;

	// Retrieve the local address the accept task is bound to
	t->addrlen = sizeof(t->addr);
	if (getsockname(t->fd, (struct sockaddr *)&t->addr, &t->addrlen) < 0) {
		int err = errno;
		task_unlock(t, (FLG_LI));
		errno = err;
		return -1;
	}

	// Set the SO_REUSEADDR and SO_REUSEPORT flag on the socket
	int val = 1;
	if (setsockopt(t->fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
		int err = errno;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	if (setsockopt(t->fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val)) < 0) {
		int err = errno;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	// Start the listen
	if (listen(t->fd, TASK_LISTEN_BACKLOG) < 0) {
		int err = errno;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	if (task_raise_event_flag(t, EPOLLIN) < 0) {
		int err = errno;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	// Now apply the listener to all IO workers
	instance_listen_balance(t);

	task_unlock(t, FLG_NONE);
	return 0;
} // TASK_socket_listen


// Connect to given destination address. If src_addr is NULL, it will just use the default interface IP and choose any local source port
int
TASK_socket_connect(int64_t tfd, struct sockaddr *addr, socklen_t addrlen, int64_t expires_in_us,
		   void *connect_cb_data, void (*connect_cb)(int64_t tfd, int result, void *connect_cb_data))
{
	register struct task *t;

	if ((t = task_lookup(tfd, FLG_CO)) == NULL) return -1;		// errno already set

	if (unlikely(t->fd < 0)) {
		task_unlock(t, FLG_CO);
		errno = EBADF;
		return -1;
	}

	// Convert this task to connect type
	t->type = TASK_TYPE_CONNECT;
	t->connect_cb = connect_cb;
	t->connect_cb_data = connect_cb_data;
	__thr_current_instance->is_client = true;
	task_socket_update_timeout(expires_in_us, &t->wr_tt.expiry_us);

	// Start the connect
	while (1) {
		memcpy(&t->addr, addr, addrlen);
		t->addrlen = addrlen;
		t->cb_errno = 0;
		if (connect(t->fd, addr, addrlen) == 0) {
			// We connected immediately! Return 1
			task_unlock(t, FLG_CO);
			return 1;
		}

		if (errno == EINTR) {
			// Try again
			continue;
		}

		if ((errno == EINPROGRESS) || (errno == EAGAIN)) {
			t->active_flags |= FLG_WR;
			if (task_raise_event_flag(t, EPOLLOUT) < 0) {
				break;
			}
			task_unlock(t, FLG_NONE);

			// We queued it, return 0
			return 0;
		}
		break;
	}

	// Connect failure of some kind. Notify the caller
	int err = errno;
	task_unlock(t, FLG_CO);
	errno = err;
	return -1;
} // TASK_socket_connect


int
TASK_socket_bind(int64_t tfd, struct sockaddr *addr, socklen_t addrlen)
{
	register struct task *t;

	if ((t = task_lookup(tfd, FLG_BND)) == NULL) {
		// errno already set
		return -1;
	}

	// Bind to the given address
	int ret = bind(t->fd, addr, addrlen), err = errno;
	task_unlock(t, FLG_BND);
	errno = err;
	return ret;
} // TASK_socket_bind


int
TASK_socket_set_close_cb(int64_t tfd, void *close_cb_data, void (*close_cb)(int64_t tfd, void *close_cb_data))
{
	register struct task *t;

	if ((t = task_lookup(tfd, FLG_CCB)) == NULL) {
		// errno already set
		return -1;
	}

	// Set the task's close callback information
	t->close_cb = close_cb;
	t->close_cb_data = close_cb_data;
	task_unlock(t, FLG_CCB);
	return 0;
} // TASK_socket_set_close_cb


// Registers an external user supplied file-descriptor with the given task instance
// Returns an abstract task socket descriptor that now owns control of the fd
int64_t
TASK_socket_register(int32_t ti, int sock, void *close_cb_data,
		    void (*close_cb)(int64_t tfd, void *close_cb_data))
{
	struct instance *i;
	struct task *t;
	int64_t tfd;
	int err;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;

	if ((i == NULL) || (i->magic != INSTANCE_MAGIC)) {
		errno = EINVAL;
		return -1;
	}

	__thr_current_instance = i;

	if (i->state == INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		return -1;
	}

	sock_set_nonblocking(sock);
	sock_set_nodelay(sock);
	sock_set_sndbuf(sock);
	sock_set_rcvbuf(sock);

	// task_create() will return the task as already locked
	if ((t = task_create(i, TASK_TYPE_IO, sock, NULL, close_cb_data, close_cb, false)) == NULL) {
		return -1;
	}
	tfd = t->tfd;
	err = errno;
	t->registered_fd = true;	// Mark it as registered so it isn't closed by task_do_close_cb()
	task_unlock(t, FLG_NONE);
	errno = err;
	return tfd;
} // TASK_register_fd


// Creates a new socket within the Task Library.  Returns an abstract descriptor to the socket
int64_t
TASK_socket_create(int32_t ti, int domain, int type, int protocol, void *close_cb_data,
		  void (*close_cb)(int64_t tfd, void *close_cb_data))
{
	struct instance *i;
	struct task *t;
	int sock, tfd, err;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;

	if ((i == NULL) || (i->magic != INSTANCE_MAGIC)) {
		errno = EINVAL;
		return -1;
	}

	__thr_current_instance = i;

	if (i->state == INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		return -1;
	}

	// Only create sockets for supported domains
	switch (domain) {
	case AF_INET:
	case AF_INET6:
	case AF_UNIX:
		break;
	default:
		errno = EINVAL;
		return -1;
	}

	// Create a socket.  Must be non-blocking so we always turn that on here
	if ((sock = socket(domain, type | SOCK_NONBLOCK, protocol)) < 0) {
		return -1;
	}

	sock_set_nonblocking(sock);
	sock_set_nodelay(sock);
	sock_set_sndbuf(sock);
	sock_set_rcvbuf(sock);

	if ((t = task_create(i, TASK_TYPE_IO, sock, NULL, close_cb_data, close_cb, true)) == NULL) {
		return -1;
	}
	tfd = t->tfd;
	err = errno;
	task_unlock(t, FLG_NONE);
	errno = err;
	return tfd;
} // TASK_socket_create


//----------------------------------------------------------------------------------------------//
// 			     Task Library Blocking Work API					//
//----------------------------------------------------------------------------------------------//

// Spawns a new worker thread, and calls the supplied via work_func(work_data)
// When the work_func() completes, work_cb(work_cb_data) is called
int
TASK_do_blocking_work(int32_t ti, void *work_data, void (*work_func)(void *work_data), void *work_cb_data, void (*work_cb_func)(int32_t ti, void *work_cb_data))
{
	struct instance *i;
	struct worker *w;
	int do_start = 0;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;
	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		return -1;
	}

	__thr_current_instance = i;

	// If we're not allowed to spawn blocking workers then we
	// have to do it ourselves.  This is NOT ideal at all!!
	if (i->max_blocking_workers == 0) {
		work_func(work_data);
		if (work_cb_func) {
			work_cb_func(ti, work_cb_data);
		}
		return 0;
	}

	// We need to hold onto the instance lock while checking, so we cheat
	// a bit with worker states here, instead of using worker_set_state()
	instance_lock(i);

	i->curtime_us = get_time_us(TASK_TIME_PRECISE);

	if (i->state == INSTANCE_STATE_SHUTTING_DOWN) {
		instance_unlock(i);
		errno = EOWNERDEAD;
		return -1;
	}

	if (i->state == INSTANCE_STATE_RUNNING) {
		do_start = 1;
	}

	if((w = TAILQ_FIRST(&i->workers_idle))) {
		i->num_blocking_idle--;
		TAILQ_REMOVE(&i->workers_idle, w, list);
		TAILQ_INSERT_TAIL(&i->workers_blocking, w, list);
	}

	// If workers_idle queue is now empty, take note of that
	// here. This affects the idle pool reaping mechanism
	if (TAILQ_EMPTY(&i->workers_idle)) {
		i->worker_idle_empty_reaped = 0;
		i->worker_idle_empty_time_us = get_time_us(TASK_TIME_PRECISE);
	}

	if(w == NULL) {
		// Don't spawn more workers than we're allowed to
		if (i->num_blocking_workers >= i->max_blocking_workers) {
			instance_unlock(i);
			errno = EAGAIN;
			return -1;
		}
		// Pre-increment the blocking count before we release the lock
		i->num_blocking_workers++;
	}
	instance_unlock(i);

	// Create a new worker if needed
	if (w == NULL) {
		if ((w = worker_create(i, WORKER_TYPE_BLOCKING)) == NULL) {
			instance_lock(i);
			// Release our pre-increment of i->num_blocking_workers
			i->num_blocking_workers--;
			instance_unlock(i);

			errno = EAGAIN;
			return -1;
		}
	}

	// We now have a worker to do the work on. Start it!
	w->work_func = work_func;
	w->work_data = work_data;
	w->work_cb_func = work_cb_func;
	w->work_cb_data = work_cb_data;

	// If we don't start it now, it'll get started when TASK_instance_start() is called
	if (do_start) {
		worker_start(w);
	}

	return 0;
} // TASK_do_blocking_work


//----------------------------------------------------------------------------------------------//
//			     Task Library Debugging Management API				//
//----------------------------------------------------------------------------------------------//

// Dumps detailed information about the task to stderr
int
TASK_debug_task(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_DBG);

	if (t == NULL) return -1;	// errno already set

	// Make the task debug call
	task_dump(t);

	task_unlock(t, FLG_DBG);
	return 0;
} //TASK_debug_task


//----------------------------------------------------------------------------------------------//
//			     Task Library Instance Management API				//
//----------------------------------------------------------------------------------------------//

// Destroys an instance without prejudice
// Do NOT use the instance after this
int
TASK_instance_destroy(int32_t ti)
{
	struct instance *i;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;
	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		return -1;
	}

	__thr_current_instance = i;

	i->state = INSTANCE_STATE_SHUTTING_DOWN;

	instance_shutdown_workers(i);
	usleep(500000);			// Sleep for half a second to give workers a chance to clean up
	instance_shutdown_tasks(i, 1);

	instance_destroy(i);
	return 0;
} // TASK_instance_destroy


// Waits for instance to fully shutdown
int
TASK_instance_wait(int32_t ti)
{
	struct instance *i;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;
	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		return -1;
	}

	__thr_current_instance = i;

	for (;;) {
		worker_handle_instance(i);

		if (i->evfd < 0) {
			poll(NULL, 0, 1000);
		} else {
			int len;
			uint64_t c;

			// Wait for something to happen! This is a blocking read
			if ((len = read(i->evfd, (void *)&c, sizeof(c))) < 0) {
				if ((errno == EINTR) || (errno == EAGAIN)) {
					continue;
				}
				close(i->evfd);
				i->evfd = -1;
			}
		}

		instance_lock(i);
		if ((i->state == INSTANCE_STATE_RUNNING)  ||
		     (!TAILQ_EMPTY(&i->workers_created)) ||
		     (!TAILQ_EMPTY(&i->workers_running)) ||
		     (!TAILQ_EMPTY(&i->workers_blocking)) ||
		     (!TAILQ_EMPTY(&i->workers_notify)) ||
		     (!TAILQ_EMPTY(&i->workers_idle)) ||
		     (!TAILQ_EMPTY(&i->workers_shutdown))) {
			instance_unlock(i);
			continue;
		}
		instance_unlock(i);
		break;
	}

	// Reap any workers still about
	instance_reap_workers(i);

	// If user wanted to be notified that we just shutdown
	if (i->shutdown_cb) {
		i->shutdown_cb(ti, i->shutdown_data);
		i->shutdown_cb = NULL;
		i->shutdown_data = NULL;
	}

	return 0;
} // TASK_instance_wait


int
TASK_instance_shutdown(int32_t ti, void *shutdown_data, void (*shutdown_cb)(intptr_t ti, void *shutdown_data))
{
	struct instance *i;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;
	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		return -1;
	}

	__thr_current_instance = i;
	if (i->state == INSTANCE_STATE_SHUTTING_DOWN) {
		return 0;
	}

	i->state = INSTANCE_STATE_SHUTTING_DOWN;
	i->shutdown_cb = shutdown_cb;
	i->shutdown_data = shutdown_data;
	instance_shutdown_workers(i);
	usleep(500000);			// Sleep for half a second to give workers a chance to clean up
	instance_shutdown_tasks(i, 0);
	instance_notify(i);
	return 0;
} // TASK_instance_shutdown


// Just a convenient function for the user to call for the initiating
// process to stall on while an instance does its work
int
TASK_instance_start(int32_t ti)
{
	struct worker *w;
	struct instance *i;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = EINVAL;
		return -1;
	}

	i = instances + ti;
	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		return -1;
	}

	__thr_current_instance = i;

	// Start all workers waiting to start
	while ((w = TAILQ_FIRST(&i->workers_created))) {
		worker_start(w);
	}

	// Assign the instance to a worker for instance notifications
	instance_lock(i);
	instance_assign_worker(i);
	w = i->instance_worker;
	i->state = INSTANCE_STATE_RUNNING;
	instance_unlock(i);

	return 0;
} // TASK_instance_start


int32_t
TASK_instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks, int tcp_sndbuf_size)
{
	struct instance *i = NULL;
	int num_io_to_spawn = 0;

	pthread_mutex_lock(&creation_lock);
	do {
		struct sigaction sa[1];
		int32_t ti;

		if (initialised) break;

		memset(sa, 0, sizeof(struct sigaction));
		sa->sa_handler = SIG_IGN;
		sigaction(SIGPIPE, sa, NULL);

		for(ti = 0; ti < TASK_MAX_INSTANCES; ti++) {
			memset(instances + ti, 0, sizeof(struct instance));
			i = instances + ti;
			i->magic = INSTANCE_MAGIC;
			i->state = INSTANCE_STATE_FREE;
		}
		initialised = true;
	} while (false);
	pthread_mutex_unlock(&creation_lock);

	// If we're not asked to spawn any io workers then
	// we need to auto-detect how many threads to start
	if (num_workers_io == 0) {
		// If 0, default to 1 IO worker per 3 configured CPUs
		num_io_to_spawn = (get_nprocs_conf() + 2) / 3;
		// If the above command fails, just spawn 2
		if (num_io_to_spawn < 2) {
			num_io_to_spawn = 2;
		}
	} else {
		num_io_to_spawn = num_workers_io;
	}

	if ((i = instance_create(num_io_to_spawn, max_blocking_workers, max_tasks)) == NULL) {
		errno = ENOMEM;
		goto TASK_instance_create_error;
	}
	__thr_current_instance = i;

	if (tcp_sndbuf_size == 0) {
		tcp_sndbuf_size = (1024 * 1024 * 1024) / max_tasks;
	}
	i->per_task_sndbuf = tcp_sndbuf_size;

	// Create our workers now
	int nio = 0;
	for (int n = 0; n < num_io_to_spawn; n++) {
		struct worker *w;

		if ((w = worker_create(i, WORKER_TYPE_IO))) {
			i->io_workers[nio++] = w;
		}
	}
	i->num_workers_io = nio;

	if (i->num_workers_io == 0) {
		errno = ENOMEM;
		goto TASK_instance_create_error;
	}

	return (instances - i);

TASK_instance_create_error:
	if (i) {
		instance_destroy(i);
	}
	return -1;
} // TASK_instance_create
