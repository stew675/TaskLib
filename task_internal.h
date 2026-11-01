#ifndef __TASK_INTERNAL_H__
#define __TASK_INTERNAL_H__

#include <stddef.h>
#include "ph.h"
#include "task_lib.h"
#include "stewticket_thr.h"

// Uncomment to turn on either EPOLLET/EPOLLONESHOT style epolling (doesn't apply to poll() mode)
#define USE_EPOLLET
#define USE_EPOLLONESHOT

// Uncomment the following to turn on "straggler" detection. A small set of active TFD's will
// be stored to instance->stragglers, which allows us to attach a debugger and
// quickly find any active TFD's within the pool that probably should not still be active
// #define TFD_POOL_DEBUG

// Uncomment to have the sizes of various structures printed out at startup
// #define DEBUG_SIZES

// General tuning/limit settings
#define TASK_MAX_IO_DEPTH	2		// Max depth IO nested callbacks can be before queueing
#define TASK_MAX_IO_UNIT	32768		// The maximum amount that may be read/written in one go
#define TASK_LISTEN_BACKLOG	((int)1024)	// System auto-truncates it to system limit anyway
#define	TASK_MAX_INSTANCES	16		// Maximum number of Task library instances allowed at once

// In order to keep the paired heap priority queue from having to deal with IO timeouts that are
// frequently cancelled again soon after being activated we have cool-off timer lists (colt's).
// It's much cheaper to remove a timeout from one of the cool-off lists than when they are in the
// priority queue.  All I/O timeouts > WORKER_TIME_COLT2 microseconds go onto the 2nd cool-off
// list.  Any timeouts not on the 2nd list with with timeouts > WORKER_TIME_COLT1 us go onto the
// 1st cool-off list. Every (WORKER_TIME_COLT1 * 0.8) microseconds the timing system adds everything
// on the first list to the priority queue, and swaps the 2nd list to the 1st list, and empties the
// 2nd list.  What this effectively means is that for all timeouts >8.1s get (on average) 5.4s to
// expire before being placed into the priority queue, and all timeouts between 4.5s and 8.1s get,
// (on average) 1.8s to expire before being placed into the priority queue.  All timeouts <4.5s get
// placed onto the priority queue immediately.  This system is cheap, and cuts down on priority queue
// use for I/O timeouts by ~98% in typical use cases
#define TASK_MIN_COLT_TIME_US	1500000				// Below 1.5s to go, we move a timeout entry to the paired heap
#define	WORKER_COLT_SCAN_TIME	40000				// The time between colt scans (in microseconds)
#define TASK_MAX_EPOLL_WAIT_MS	(1000000 / WORKER_COLT_SCAN_TIME) // 1/5th that of COLT1 (expressed in millisecs)

// Number of TFD locks in an instance's lock pool.  Altering this value provides a non-intuitive
// performance impact. While more lock entries offer less lock contention, they also are accessed
// fairly frequently, and so can cause excessive CPU cache contention misses if there are too many,
// which will negatively impact overall performance.  Values in the low-middling 100's appear to be
// good compromise values
// Number of shards in the lock-free task free list.  Each shard has its own
// tagged head on a separate cache line, so concurrent task create/free paths on
// different CPUs do not all bounce the same cache line.  Must be a power of 2.
#define TASK_FREE_SHARDS		16
#define TASK_FREE_SHARD_MASK		(TASK_FREE_SHARDS - 1)

#define	TASK_MAX_TFD_LOCKS		(127)
#define TFDI_TO_LOCK_INDEX(tfdi)	(tfdi % TASK_MAX_TFD_LOCKS)

// Uncomment to enable use of ticketed spinlocks, instead of adaptive pthread mutexes.  The main
// reason for why you'd want to do this is to gather statistics about the contention rates of the
// TFD lookup table locks to better tune the lock table sizes.  Ticket locks are slightly faster
// than the adaptive pthread mutexes, but performance is terrible if the worker threads ever get
// context switched by the Linux scheduler. It is slightly easier however to gather profiling
// statistics about the time spent in TFD locks with gprof using ticketlocks
#define USE_TICKET_LOCKS

#ifdef USE_TICKET_LOCKS
typedef struct {
	ticketlock_t		lock;
} __attribute__ ((aligned(64))) tfd_lock_t;

#define	tfd_lock(t, tfdi)	 ticket_lock(&((t)->worker->instance->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#define	tfd_unlock(t, tfdi) ticket_unlock(&((t)->worker->instance->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#define	itfd_lock(i, tfdi)	 ticket_lock(&((i)->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#define	itfd_unlock(i, tfdi) ticket_unlock(&((i)->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))

#else

typedef struct {
	pthread_mutex_t		lock;
} __attribute__ ((aligned(64))) tfd_lock_t;

#define	tfd_lock(t, tfdi)	 pthread_mutex_lock(&((t)->worker->instance->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#define	tfd_unlock(t, tfdi) pthread_mutex_unlock(&((t)->worker->instance->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#define	itfd_lock(i, tfdi)	 pthread_mutex_lock(&((i)->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#define	itfd_unlock(i, tfdi) pthread_mutex_unlock(&((i)->tfd_locks[TFDI_TO_LOCK_INDEX(tfdi)].lock))
#endif

//-------------------------------------------------------------------------------------------

TAILQ_HEAD(worker_list, worker);
STAILQ_HEAD(ntfyq_list, ntfyq);

typedef enum {
	TASK_TIME_PRECISE,
	TASK_TIME_COARSE
} time_precision_t;

// The ordering of these tasks states is important
typedef enum {
	TASK_STATE_UNUSED = 0,		// Idle
	TASK_STATE_ACTIVE = 1,		// In the FD table
	TASK_STATE_DESTROY = 2,		// Waiting for worker to kill it
	TASK_STATE_FREEING = 3		// Claimed for recycling; no new references
} task_state_t;

typedef enum {
	TASK_TYPE_NONE = 0,
	TASK_TYPE_IO = 1,
	TASK_TYPE_TIMER = 2,
	TASK_TYPE_CONNECT = 3,
	TASK_TYPE_LISTEN = 4
} task_type_t;

typedef enum {
	TASK_READ_STATE_IDLE = 0,
	TASK_READ_STATE_VECTOR = 1,
	TASK_READ_STATE_BUFFER = 2,
	TASK_READ_STATE_LISTEN = 3,
	TASK_READ_STATE_PENDING = 4	// API has reserved the read side
} task_rd_state_t;

typedef enum {
	TASK_WRITE_STATE_IDLE = 0,
	TASK_WRITE_STATE_VECTOR = 1,
	TASK_WRITE_STATE_BUFFER = 2,
	TASK_WRITE_STATE_CONNECT = 3,
	TASK_WRITE_STATE_PENDING = 4	// API has reserved the write side
} task_wr_state_t;

enum {
	TIMER_TIME_DESTROY = -2,
	TIMER_TIME_CANCEL = -1
};

// The below are activity reference bits. The system works a bit like reference counting
// except there's a limit to the number of references, and each reference is exclusive.
// Doing it this way, protected by mutexes, is WAY faster than using atomic counters plus
// being easier to debug since we always specifically know which reference is held
typedef enum {
	FLG_NONE =	0x00000000,		// Special no flag for certain operations
	FLG_RD	=	0x00000001,		// A read for a TFD is active in the library
	FLG_WR	=	0x00000002,		// A write for a TFD is active
	FLG_RT	=	0x00000004,		// A Read Timeout Update is active
	FLG_WT	=	0x00000008,		// A Write Timeout Update is active
	FLG_LU	=	0x00000010,		// Lookup Lock
	FLG_CO	=	0x00000020,		// A Connect Task is active
	FLG_LI	=	0x00000040,		// A Listen Task is active
	FLG_TM	=	0x00000080,		// A Timer Task is active
	FLG_MG	=	0x00000100,		// A Migration Event is active
	FLG_CL	=	0x00000200,		// A Close Event is active
	FLG_BND	=	0x00000400,		// A Bind operation is in progress
	FLG_CN	= 	0x00000800,		// Task is connecting
	FLG_CCB =	0x00001000,		// A Close CallBack API is in progress
	FLG_TC	=	0x00002000,		// A Timer Cancel API is in progress
	FLG_TD	=	0x00004000,		// A Timer Destroy API is in progress
	FLG_TS	=	0x00008000,		// A Timer Set API is in progress
	FLG_GFD	=	0x00010000,		// A Get FD operation is already in progress
	FLG_SD	=	0x00020000,		// A Shutdown operation is in progress
	FLG_DBG	=	0x00040000,		// Task Debug API in progress
	FLG_DRP = 	0x80000000,		// If this a drop of another action
} task_action_flag_t;

struct task;

struct ntfyq {
	struct ntfyq		*next;
	struct task		*t;
	uint32_t		action;
	uint32_t		unused;
};

struct task_timer {
	struct task_timer	*next;		// Next cool-off list entry
	struct task_timer	*prev;		// Previous cool-off list entry
	int64_t			expiry_us;	// The expiry time
	void			*node;		// The paired heap node
};

// The task structure is broken into 2 parts, being the "stride" and "active" portions. Infrequently
// accessed fields exists within the "stride" portion.  The sizes of these structures are also
// broken out into 3 x 64 byte and 6 x 64 byte sized elements, which introduces a staggered data
// "striding" pattern for the CPU caches for better CPU cache-set utlisation.  The static
// assertions after struct worker enforce these cache-line boundaries.

struct task_stride {
	// For TASK_TYPE_ACCEPT tasks, addr refers to the local address we're listening on
	// For TASK_TYPE_IO/CONNECT tasks, addr refers to the remote communication address
	struct sockaddr_storage		addr;			// Addresses for tasks within the task pool

	//===================================  128 BYTE BOUNDARY   =====================================//

	//----------------------------------------------------------------------------------------------//
	//===================================    CALLBACK FIELDS    ====================================//
	//----------------------------------------------------------------------------------------------//

__attribute__ ((aligned(64))) void	*accept_cb_data;
	void				(*accept_cb)(int64_t tfd, void *accept_cb_data);
	void				*connect_cb_data;
	void				(*connect_cb)(int64_t tfd, int result, void *connect_cb_data);
	void				*close_cb_data;		//  User data to pass to the close callback
	void				(*close_cb)(int64_t tfd, void *close_cb_data);
	struct worker			*preferred_worker;	// To initiate task io worker migration
	int64_t				parent_tfd;		// Parent listener TFD for child listeners

	//===================================  64 BYTE BOUNDARY    =====================================//
} __attribute__ ((aligned(64)));

// XXX - All the struct task_timer entries MUST reside within struct task, or otherwise the timeout system
// will fail to work. Do NOT move any of these to struct task_stride
struct task {
	//===================================  64 BYTE BOUNDARY    =====================================//

	// General task information.  Every task structure is aligned to a 64-byte boundary. We pack
	// as much hot information as we can into the first single CPU cache line size of 64 bytes.
	//
	// Fields that can be touched by an API caller and a worker at the same time are
	// either manipulated with atomic builtins or are only ever changed
	// by the owning worker while the task's TFD lock is held.

	uint32_t		active_flags;		// Worker-processed action flags
	uint32_t		state;			// Operational state of the task
	_Atomic uint32_t	type;			// Type of task
	uint32_t			io_depth;		// Direct callback nesting depth
	_Atomic int64_t		tfd;			// Task File Descriptor that identifies this task
	struct worker			*_Atomic worker;	// The current io worker task is bound to
	_Atomic int32_t		fd;			// The actual system socket FD we're working on
	int32_t				epfd;			// The worker epoll FD we're registered with
	uint32_t			events;			// Active poll/epoll events
	uint32_t		notifyqlen;		// Number of (worker-owned) notifyq entries
	uint32_t		api_refs;		// In-flight public API calls holding this task
	socklen_t			addrlen;		// The valid length of the data in .addr
	struct task_stride		*stride;

	//===================================  64 BYTE BOUNDARY    =====================================//

	//----------------------------------------------------------------------------------------------//
	//=================================    WRITE CONTROL FIELDS    =================================//
	//----------------------------------------------------------------------------------------------//

	// Data write fields
__attribute__ ((aligned(64))) const char *wr_buf;
	size_t				wr_buflen;
	size_t				wr_bufpos;
	void				*wr_cb_data;
	int64_t				wr_expires_in_us;
	void				(*wr_cb)(int64_t tfd, const void *buf, ssize_t result, void *wr_cb_data);
	void				(*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data);

	//===================================  64 BYTE BOUNDARY    =====================================//

	//----------------------------------------------------------------------------------------------//
	//==================================    READ CONTROL FIELDS    =================================//
	//----------------------------------------------------------------------------------------------//

	// Data read fields
__attribute__ ((aligned(64))) char	*rd_buf;
	size_t				rd_buflen;
	size_t				rd_bufpos;
	void				*rd_cb_data;
	int64_t				rd_expires_in_us;
	void				(*rd_cb)(int64_t tfd, void *buf, ssize_t result, void *rd_cb_data);
	void				(*rdv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data);

	//===================================  64 BYTE BOUNDARY    =====================================//

	//===================================  64 BYTE BOUNDARY    =====================================//

__attribute__ ((aligned(64))) struct task_timer rd_tt;		// 32 bytes - RD Timeout Information
	const struct iovec		*rdv_iov;
	size_t				rdv_buflen;
	size_t				rdv_bufpos;
	int32_t				rdv_iovcnt;
	uint32_t			migrations;		// Number of times the task has migrated

	//===================================  64 BYTE BOUNDARY    =====================================//

__attribute__ ((aligned(64))) struct task_timer	wr_tt;		// 32 bytes - WR Timeout Information
	const struct iovec		*wrv_iov;
	size_t				wrv_bufpos;
	size_t				wrv_buflen;
	int32_t				wrv_iovcnt;
	uint32_t		meta_flags;		// Short-lived public API exclusion bits

	//===================================  64 BYTE BOUNDARY    =====================================//

	//----------------------------------------------------------------------------------------------//
	//=================================    TIMER CONTROL FIELDS    =================================//
	//----------------------------------------------------------------------------------------------//

__attribute__ ((aligned(64))) struct task_timer tm_tt;		// 32 bytes - Timer Information
	void				*tm_cb_data;
	void				(*tm_cb)(int64_t tfd, int64_t lateness_us, void *tm_cb_data);
#define	MAX_NOTIFY_QUEUE_LEN	(7)
	// notifyqlen_locked is updated by an atomic count operation.  We want to keep it on a CPU cache
	// line (64 bytes width) with other infreqently accessed data.
	uint64_t			notifyqlen_locked;	// Number of locked notifyq entries this task has
	struct task			*task_next;		// List of free tasks

	//===================================  64 BYTE BOUNDARY    =====================================//

	// State/flags that are either owner-only or manipulated with atomic
	// operations, kept away from the hot line 0 words.  The separate words
	// mean a write to one flag can never lose a concurrent write to another
	// (the previous packed bitfield could).
	uint32_t			rd_state;
	uint32_t			wr_state;
	_Atomic uint32_t		rd_shut;
	_Atomic uint32_t		wr_shut;
	_Atomic uint32_t		rd_cancel;
	_Atomic uint32_t		wr_cancel;
	uint32_t		rt_cancelled;
	uint32_t		wt_cancelled;
	uint32_t		rd_expires_modified;
	uint32_t		wr_expires_modified;
	uint32_t		tm_cancelled;
	uint32_t		in_epoll;
	uint32_t		registered_fd;
	uint32_t		listen_child;
	_Atomic uint32_t		closed;			// Final close cleanup has run
	_Atomic uint32_t		free_next;		// Next slot index on the lock-free free list

	//===================================  64 BYTE BOUNDARY    =====================================//
} __attribute__ ((aligned(64)));

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
__attribute__ ((aligned(64))) uint32_t	type;
	int32_t				affined_cpu;
	struct ntfyq			*freeq;
	struct ntfyq			*freeq_locked;
	struct ntfyq			*notifyq_head;
	struct ntfyq			*notifyq_tail;
	struct ntfyq			*notifyq_batches;
	struct ntfyq			*_Atomic notifyq_locked_head;
	struct ntfyq			*_Atomic notifyq_locked_tail;

__attribute__ ((aligned(64))) void	*timer_queue;
	int64_t				colt_next_us;
	size_t				colt_len;
	struct task_timer		*colt;
	struct epoll_event 		*events;
	int32_t				max_events;
	int32_t				num_events;
	int32_t				max_pollfds;
	int32_t				poll_listeners;
	int32_t				gepfd;			// General epoll_wait fd
	int32_t				evfd;

__attribute__ ((aligned(64))) pthread_t	thr;
	_Atomic uint32_t		state;
	_Atomic uint32_t		old_state;
	struct task			*listeners;
	struct pollfd 			*pollfds;
	struct instance			*instance;
	uint64_t			num_tasks;
	TAILQ_ENTRY(worker)		list;

	// Blocking worker call info.  The state flags live at the start of this
	// cache line so the third worker cache line above remains exactly 64 bytes.
__attribute__ ((aligned(64))) _Atomic uint32_t started;	// Thread has been created
	_Atomic uint32_t		notified;	// Wakeup already pending (atomic)
	void				(*work_func)(void *work_data);
	void				*work_data;
	void				(*work_cb_func)(int32_t ti, void *work_cb_data);
	void				*work_cb_data;
	struct task			**poll_tasks;	// Listener snapshot for poll()

	// Put this all by itself at the end
__attribute__ ((aligned(64))) pthread_mutex_t lock;
}  __attribute__ ((aligned(64)));

//----------------------------------------------------------------------------------------------//
// Cache-line layout invariants.  The task/worker/stride structures are deliberately laid out
// in 64-byte cache-line-sized groups.  These assertions catch accidental field additions or
// removals that would straddle a cache line or shift a group off its boundary.
//----------------------------------------------------------------------------------------------//
_Static_assert(_Alignof(struct task) == 64, "struct task alignment");
_Static_assert(sizeof(struct task) == 448, "struct task must be 7 cache lines");
_Static_assert(offsetof(struct task, wr_buf) == 64, "task write group must start on line 1");
_Static_assert(offsetof(struct task, wrv_cb) + sizeof(((struct task *)0)->wrv_cb) <= 128, "task write group crosses line 1");
_Static_assert(offsetof(struct task, rd_buf) == 128, "task read group must start on line 2");
_Static_assert(offsetof(struct task, rdv_cb) + sizeof(((struct task *)0)->rdv_cb) <= 192, "task read group crosses line 2");
_Static_assert(offsetof(struct task, rd_tt) == 192, "task read timer group must start on line 3");
_Static_assert(offsetof(struct task, migrations) + sizeof(uint32_t) <= 256, "task read timer group crosses line 3");
_Static_assert(offsetof(struct task, wr_tt) == 256, "task write timer group must start on line 4");
_Static_assert(offsetof(struct task, meta_flags) + sizeof(uint32_t) <= 320, "task write timer group crosses line 4");
_Static_assert(offsetof(struct task, tm_tt) == 320, "task timer group must start on line 5");
_Static_assert(offsetof(struct task, task_next) + sizeof(void *) <= 384, "task timer group crosses line 5");
_Static_assert(offsetof(struct task, rd_state) == 384, "task state group must start on line 6");
_Static_assert(offsetof(struct task, free_next) + sizeof(uint32_t) <= 448, "task state group crosses line 6");
_Static_assert((TASK_FREE_SHARDS & TASK_FREE_SHARD_MASK) == 0, "free-list shard count must be a power of 2");

// Timer keys are stored in void * keys by the paired heap.  A 32-bit void *
// cannot represent the absolute monotonic microsecond timestamps used here, so
// reject such builds rather than silently mis-ordering timers after ~35 minutes.
_Static_assert(sizeof(void *) >= 8, "TaskLib requires 64-bit pointers for timer keys");

_Static_assert(_Alignof(struct task_stride) == 64, "struct task_stride alignment");
_Static_assert(sizeof(struct task_stride) == 192, "struct task_stride must be 3 cache lines");
_Static_assert(offsetof(struct task_stride, accept_cb_data) == 128, "task_stride callback group must start on line 2");
_Static_assert(offsetof(struct task_stride, parent_tfd) + sizeof(int64_t) <= sizeof(struct task_stride), "task_stride callback group crosses line 2");

_Static_assert(_Alignof(struct worker) == 64, "struct worker alignment");
_Static_assert(sizeof(struct worker) % 64 == 0, "struct worker must be cache-line sized");
_Static_assert(offsetof(struct worker, timer_queue) == 64, "worker timer group must start on line 1");
_Static_assert(offsetof(struct worker, evfd) + sizeof(int32_t) <= 128, "worker timer group crosses line 1");
_Static_assert(offsetof(struct worker, thr) == 128, "worker state group must start on line 2");
_Static_assert(offsetof(struct worker, list) + sizeof(TAILQ_ENTRY(worker)) <= 192, "worker state group crosses line 2");
_Static_assert(offsetof(struct worker, started) == 192, "worker work group must start on line 3");
_Static_assert(offsetof(struct worker, work_cb_data) + sizeof(void *) <= 256, "worker work group crosses line 3");
_Static_assert(offsetof(struct worker, lock) % 64 == 0, "worker lock must be cache-line aligned");

_Static_assert(_Alignof(tfd_lock_t) == 64, "tfd_lock_t alignment");
_Static_assert(sizeof(tfd_lock_t) % 64 == 0, "tfd_lock_t must be cache-line sized");

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
} __attribute__ ((aligned(32)));;

struct task_free_head {
	_Atomic uint64_t	head;		// Packed (tag << 32) | free-list head index
} __attribute__ ((aligned(64)));

struct instance {
	uint32_t			magic;
#define INSTANCE_MAGIC	0x487a3d67

	int64_t				worker_idle_empty_time_us;
	int64_t				worker_idle_empty_reaped;
	int64_t				curtime_us;

	bool				is_client;		// If we're observed to connect()
	bool				is_server;		// If we're observed to listen()
	bool				flip;			// Affects CPU affinity flipping
	uint64_t			flags;

	instance_state_t		state;
	pthread_t			thr;
	pthread_mutex_t			lock;
	int				evfd;
	int32_t				ti;

	// TFD->task pool
	struct task			*tfd_pool;		// The total pool of tasks we can work with
	struct task_stride		*tfd_stride;		// The "stride" portion of the task pool
	struct task_free_head		free_heads[TASK_FREE_SHARDS];

	uint64_t			tfd_pool_used;		// The total number of active tasks in the pool
	uint32_t			tfd_pool_size;

	tfd_lock_t			*tfd_locks_real;
	tfd_lock_t			*tfd_locks;

	struct worker			*instance_worker;
	struct worker			**io_workers;
	uint32_t			cur_io_worker;
	uint32_t			num_workers_io;

	uint64_t			max_tcp_mem;
	uint64_t			per_task_sndbuf;

	// Protects every worker's listeners list and listener migration
	pthread_mutex_t			listeners_lock;

	// CPU/Worker Affinity Fields
	pthread_mutex_t			cpulock;
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

#ifdef TFD_POOL_DEBUG
#define NUM_STRAGGLERS 50
	uint32_t			stragglers[NUM_STRAGGLERS];
#endif
	void				(*shutdown_cb)(intptr_t ti, void *shutdown_data);
	void				*shutdown_data;
};

// ---------------------------------------------------------------------------------------------//
//		   General Macros/definitions and global/per-thread variables			//
//----------------------------------------------------------------------------------------------//


// The current IO worker.  MUST BE NULL if current thread is not an IO worker
extern __thread struct worker	*__thr_current_worker;
extern __thread	int64_t		w__curtime_us;

extern struct instance		*instances[TASK_MAX_INSTANCES];
extern bool initialised;
extern size_t __page_size;
extern pthread_mutex_t	creation_lock;

// How to identify if we're on the worker thread we're assigned to
#define	lockless_worker(w)	((w) == __thr_current_worker)

// Handy time unit conversion macros
#define TASK_MS_TO_US(a)	(((int64_t)a) * 1000)
#define TASK_US_TO_MS(a)	(((int64_t)a) / 1000)
#define TASK_S_TO_US(a)		(((int64_t)a) * 1000000)
#define TASK_NS_TO_US(a)	(((int64_t)a) / 1000)
#define TASK_US_TO_S(a)		(((int64_t)a) / 1000000)

// A TFD is a 64-bit opaque handle composed of:
//   bits  0-31: index into the instance's task pool
//   bits 32-35: instance index (TASK_MAX_INSTANCES is 16)
//   bits 36-62: generation/iteration counter (27 bits, keeps the handle positive)
// The instance index embedded in the handle means TFD APIs do not need to rely on
// thread-local state to determine which instance a TFD belongs to.  The generation
// counter allows stale handles to be rejected after a task slot has been recycled.
#define	TFD_TO_INDEX(tfd)	((uint32_t)((uint64_t)(tfd) & 0xffffffffu))
#define	TFD_TO_INSTANCE(tfd)	((uint32_t)(((uint64_t)(tfd) >> 32) & 0xfu))
#define	TFD_TO_ITERATION(tfd)	((uint32_t)(((uint64_t)(tfd) >> 36) & TFD_ITERATION_MASK))
#define	TFD_ITERATION_MASK	(0x07ffffffu)
#define	TFD_MAKE(ti, iteration, index) \
	((int64_t)((((uint64_t)(iteration)) & TFD_ITERATION_MASK) << 36) | \
		 (((uint64_t)(ti) & 0xfu) << 32) | ((uint64_t)(index) & 0xffffffffu))

// The non-existent TFD Identifier
#define TFD_NONE 		(int64_t)(0xffffffffffffffff)



//----------------------------------------------------------------------------------------------//
// Cross-module prototypes
//----------------------------------------------------------------------------------------------//

void task_dump(struct task *t);
int64_t get_time_us(time_precision_t prec);
void worker_timer_attach_list(struct worker *w, struct task_timer *tt);
void worker_timer_detach_list(struct worker *w, struct task_timer *tt);
void worker_timer_attach_heap(struct worker *w, struct task_timer *tt);
void worker_timer_detach_heap(struct worker *w, struct task_timer *tt);
void worker_timer_update(struct worker *w, struct task_timer *tt);
struct task * worker_get_task_from_tt(struct worker *w, struct task_timer *tt);
void worker_timer_scan_colt(struct worker *w);
void task_destroy_timeouts(struct task *t);
void task_remove_list(struct task **head, struct task *t);
void task_init(struct instance *i, struct task *t);
void task_free(struct instance *i, struct task *t, bool published);
void task_maybe_free(struct task *t);
void task_action_done(struct task *t, task_action_flag_t action);
bool task_action_is_meta(task_action_flag_t action);
struct task * task_lookup(int64_t tfd, task_action_flag_t action);
void task_unlock(struct task *t, task_action_flag_t action);
bool task_claim_close(struct instance *i, struct task *t);
bool task_claim_listen_child(struct instance *i, struct task *c, int64_t parent_tfd);
void task_nuke(struct instance *i, uint32_t tfdi);
struct task * task_get_free_task(struct instance *i);
int get_cpu_of_sock(struct instance *i, int sock);
int get_cpu_adjusted_for_direction(struct instance *i, int cpu, bool is_client);
struct worker * get_affined_worker_from_direction(struct instance *i, bool is_client);
void set_worker_cpu_affinity(struct worker *w, int tcpu);
void set_one_workers_affinity(struct instance *i, int cpu);
void task_set_initial_preferred_worker(struct instance *i, struct task *t, bool is_client);
void worker_learn_cpu_affinity(struct task *t);
int notifier_write(int fd);
int worker_notify(struct worker *w);
int instance_notify(struct instance *i);
struct ntfyq * worker_notify_get_free_ntfyq(struct worker *w);
struct ntfyq * worker_notify_reserve_locked(struct worker *w);
void worker_notify_queue_locked(struct worker *w, struct ntfyq *tq, struct task *t, task_action_flag_t action);
bool task_queue_close(struct task *t);
bool task_notify_action(struct task *t, task_action_flag_t action, bool force_close_q, bool force_q);
int64_t task_calculate_io_timeout(int64_t us_from_now);
void task_activate_rd_timeout(struct worker *w, struct task *t);
void task_activate_wr_timeout(struct worker *w, struct task *t);
int task_create_event_flag(struct task *t, uint32_t flags);
int task_raise_event_flag(struct task *t, uint32_t flags);
int task_lower_event_flag(struct task *t, uint32_t flags);
void task_cancel_write(struct task *t);
void task_cancel_read(struct task *t);
void task_do_close_cb(struct task *t);
void task_do_readv_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno);
void task_do_read_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno);
void task_do_accept_cb(struct task *t, int64_t tfd, int cb_errno);
void task_do_writev_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno);
void task_do_write_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno);
void task_do_connect_cb(struct task *t, int64_t tfd, int result, int cb_errno);
void task_do_timeout_cb(struct task *t, task_action_flag_t action, int64_t timeout_us);
void task_shutdown_listen_children(struct task *parent);
void worker_cleanup(struct worker *w);
void worker_destroy(struct worker *w);
struct worker * worker_select_io_worker(struct instance *i, struct task *t, bool outgoing);
int instance_assign_worker(struct instance *i);
void worker_set_state(struct worker *w, worker_state_t new_state);
void task_update_timer(struct task *t);
struct task * task_create(struct instance *i, int type, int fd, struct worker *w, void *close_cb_data, void(*close_cb)(int64_t tfd, void *close_cb_data), bool outgoing);
ssize_t task_write_vector(struct task *t, bool wr_expires_modified, bool queued);
ssize_t task_write_buffer(struct task *t, bool wr_expires_modified, bool queued);
void task_handle_connect_event(struct task *t);
void task_handle_connecting_action(struct task *t, bool wr_expires_modified);
ssize_t task_read_vector(struct task *t, bool rd_expires_modified, bool queued);
ssize_t task_read_buffer(struct task *t, bool rd_expires_modified, bool queued);
int sock_set_nonblocking(int sock);
int sock_set_sndbuf(struct instance *i, int sock);
int sock_set_rcvbuf(struct instance *i, int sock);
void task_handle_listen_event(struct task *t);
void task_handle_wr_event(struct task *t);
void task_handle_rd_event(struct task *t);
void worker_check_timeouts(struct worker *w);
void worker_handle_instance(struct instance *i);
void worker_handle_event(struct worker *w, uint32_t events);
bool worker_handle_task_migration(struct worker *w, struct task *t);
void worker_process_one_action(struct worker *w, struct task *t, task_action_flag_t action);
void worker_process_notifyq(struct worker *w);
void worker_poll_listeners(struct worker *w);
int get_next_epoll_timeout_ms(struct worker *w);
void worker_handle_task_event(struct task *t, uint32_t revents);
void * worker_loop_io(void *arg);
void * worker_loop_blocking(void *arg);
int worker_start(struct worker *w);
struct worker * worker_create(struct instance *i, int worker_type);
void instance_listen_balance(struct task *t);
void instance_reap_workers(struct instance *i);
void instance_queue_shutdown(struct instance *i);
void instance_nuke_tasks(struct instance *i);
void instance_wait_workers(struct instance *i);
void instance_shutdown_workers(struct instance *i);
void instance_destroy(struct instance *i);
int instance_tfd_pool_init(struct instance *i, uint32_t pool_size);
struct instance * instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks);
int64_t TASK_get_us_time(int64_t *time_us, bool precise);
int TASK_timeout_destroy(int64_t tfd);
int TASK_timeout_cancel(int64_t tfd);
int TASK_timeout_set(int64_t tfd, int64_t us_from_now, void *timeout_cb_data, void(*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data));
int64_t TASK_timeout_create(int32_t ti, int64_t us_from_now, void *timeout_cb_data, void(*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data));
void TASK_socket_writev(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *wr_cb_data, void(*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data));
void TASK_socket_write(int64_t tfd, const void *wrbuf, size_t buflen, int64_t expires_in_us, void *wr_cb_data, void(*wr_cb)(int64_t tfd, const void *wrbuf, ssize_t result, void *wr_cb_data));
void TASK_socket_readv(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *rd_cb_data, void(*rdv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data));
void TASK_socket_read(int64_t tfd, void *rdbuf, size_t buflen, int64_t expires_in_us, void *rd_cb_data, void(*rd_cb)(int64_t tfd, void *rdbuf, ssize_t result, void *rd_cb_data));
int TASK_socket_get_fd(int64_t tfd);
int TASK_migrate(int64_t to_tfd, int64_t from_tfd);
int TASK_close(int64_t tfd);
int TASK_socket_shutdown(int64_t tfd, int how);
int TASK_socket_listen(int64_t tfd, void *accept_cb_data, void(*accept_cb)(int64_t tfd, void *accept_cb_data));
void TASK_socket_connect(int64_t tfd, struct sockaddr *addr, socklen_t addrlen, int64_t expires_in_us, void *connect_cb_data, void(*connect_cb)(int64_t tfd, int result, void *connect_cb_data));
int TASK_socket_bind(int64_t tfd, struct sockaddr *addr, socklen_t addrlen);
int TASK_socket_set_close_cb(int64_t tfd, void *close_cb_data, void(*close_cb)(int64_t tfd, void *close_cb_data));
int64_t TASK_socket_create(int32_t ti, int domain, int type, int protocol, void *close_cb_data, void(*close_cb)(int64_t tfd, void *close_cb_data));
int TASK_do_blocking_work(int32_t ti, void *work_data, void(*work_func)(void *work_data), void *work_cb_data, void(*work_cb_func)(int32_t ti, void *work_cb_data));
int TASK_debug_task(int64_t tfd);
int TASK_instance_destroy(int32_t ti);
int TASK_instance_wait(int32_t ti);
int TASK_instance_shutdown(int32_t ti, void *shutdown_data, void(*shutdown_cb)(intptr_t ti, void *shutdown_data));
int TASK_instance_start(int32_t ti);
int32_t TASK_instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks, int tcp_sndbuf_size, uint64_t flags);

//----------------------------------------------------------------------------------------------//
// Hot inline helpers
//----------------------------------------------------------------------------------------------//

static inline void
worker_lock(struct worker *w)
{
	pthread_mutex_lock(&w->lock);
} // worker_lock

static inline void
worker_unlock(struct worker *w)
{
	pthread_mutex_unlock(&w->lock);
} // worker_unlock

static inline void
task_ref(struct task *t)
{
	task_atomic_inc(&t->api_refs);
} // task_ref

static inline void
task_unref(struct task *t)
{
	if (__atomic_sub_fetch(&t->api_refs, 1, __ATOMIC_SEQ_CST) == 0) {
		task_maybe_free(t);
	}
} // task_unref

static inline void
task_lock(struct task *t, task_action_flag_t action)
{
	task_atomic_or(&t->active_flags, action);
} // task_lock

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

void worker_do_io_epoll(struct worker *w);
int64_t TASK_socket_register(int32_t ti, int sock, void *close_cb_data, void (*close_cb)(int64_t tfd, void *close_cb_data));

#endif	// __TASK_INTERNAL_H__
