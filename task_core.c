#include "task_internal.h"

// Per-library global state
__thread struct worker	*__thr_current_worker = NULL;
__thread int64_t		w__curtime_us = 0;

struct instance		*instances[TASK_MAX_INSTANCES];
bool			initialised = false;
size_t			__page_size;
pthread_mutex_t		creation_lock = PTHREAD_MUTEX_INITIALIZER;

// Per-instance slot API-entry references.  A lookup increments the reference
// for the instance index *before* it dereferences instances[ti], and keeps it
// until task_unlock().  TASK_instance_destroy() clears instances[ti] and then
// waits for these references to drain before freeing the instance, so an API
// call that raced with destruction cannot touch freed instance memory.
// The counters are sharded so unrelated API-calling threads do not all bounce
// one per-instance cache line.  Each thread sticks to one shard; destruction
// scans all shards, which is a cold path.
#define TASK_API_REF_SHARDS		64
#define TASK_API_REF_SHARD_MASK		(TASK_API_REF_SHARDS - 1)

struct api_slot_ref {
	_Atomic uint32_t	refs;
} __attribute__ ((aligned(64)));
static struct api_slot_ref
	task_api_slot_refs[TASK_MAX_INSTANCES][TASK_API_REF_SHARDS];

static __thread uint32_t	task_api_thread_shard;
static __thread bool		task_api_thread_shard_set;
static _Atomic uint32_t		task_api_thread_seq;

// Per-slot admission gate for instance-entry references.  It lives for the
// process lifetime so an API caller can test it without dereferencing the
// (possibly freed) instance pointer.  create/destroy close it before draining
// references, and create re-opens it after publishing the instance.
static _Atomic uint32_t	task_api_slot_open[TASK_MAX_INSTANCES];

// Persistent per-slot TFD generation state.  The per-instance task pool is
// recycled when an instance is destroyed and recreated, so a generation
// counter kept in the task slot itself would restart at 1 and a stale tfd
// could alias a task in the replacement instance.  These counters survive
// instance destruction (the tables are retained for the process lifetime)
// so the documented "134 million reuses of this slot" generation window is
// preserved across instance lifetimes as well.
struct tfd_generation_state {
	uint32_t			*gens;
	uint32_t			capacity;
};
static struct tfd_generation_state	task_tfd_generations[TASK_MAX_INSTANCES];

static uint32_t
task_next_generation(struct instance *i, uint32_t tfdi)
{
	struct tfd_generation_state *g = &task_tfd_generations[i->ti];
	uint32_t generation;

	assert(g->gens != NULL);
	assert(tfdi < g->capacity);

	generation = __atomic_add_fetch(&g->gens[tfdi], 1, __ATOMIC_RELAXED);
	return generation & TFD_ITERATION_MASK;
} // task_next_generation

static inline uint32_t
task_api_shard(void)
{
	if (!task_api_thread_shard_set) {
		task_api_thread_shard =
			__atomic_fetch_add(TASK_ATOMIC_PTR(&task_api_thread_seq), 1, __ATOMIC_RELAXED) &
			TASK_API_REF_SHARD_MASK;
		task_api_thread_shard_set = true;
	}
	return task_api_thread_shard;
} // task_api_shard

// An API call running on one of the instance's own IO workers is already
// protected by the worker lifetime: destruction stops and joins all workers
// before it waits on these counters or frees the instance.
static inline bool
task_api_ref_required(uint32_t ti)
{
	struct worker *w = __thr_current_worker;

	return !((w != NULL) && (w->instance != NULL) &&
		 ((uint32_t)w->instance->ti == ti));
} // task_api_ref_required

static inline void
task_api_enter(uint32_t ti)
{
	task_atomic_inc(&task_api_slot_refs[ti][task_api_shard()].refs);
} // task_api_enter

static inline void
task_api_exit(uint32_t ti)
{
	task_atomic_dec(&task_api_slot_refs[ti][task_api_thread_shard].refs);
} // task_api_exit

// Instance-entry references for APIs that take an instance index rather than
// a tfd.  The reference is only taken if the slot is published; once destroy
// publishes INSTANCE_SLOT_BUSY new callers fail immediately instead of
// keeping the API-reference drain permanently non-zero.
bool
task_instance_ref(uint32_t ti)
{
	struct instance *i;

	if (!__atomic_load_n(&task_api_slot_open[ti], __ATOMIC_SEQ_CST)) {
		return false;
	}

	// Increment first, then revalidate the slot pointer.  Destroy reserves the
	// slot (INSTANCE_SLOT_BUSY) before waiting for references, so either the
	// revalidation sees the reservation and we drop our reference without ever
	// dereferencing the instance, or destroy's wait observes our reference.
	for (;;) {
		i = (struct instance *)__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);
		if ((i == NULL) || (i == INSTANCE_SLOT_BUSY)) {
			return false;
		}
		task_api_enter(ti);
		if (__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST) == i) {
			return true;
		}
		task_api_exit(ti);
		return false;
	}
} // task_instance_ref

void
task_instance_unref(uint32_t ti)
{
	task_api_exit(ti);
} // task_instance_unref

// Lock-free free-list head encoding.  The tag makes the pop/push pair
// ABA-safe; task->free_next stores the next slot index rather than a pointer
// so concurrent readers never race on a plain pointer field.
#define TASK_FREE_EMPTY		(0xffffffffu)

static inline uint64_t
task_free_pack(uint32_t idx, uint32_t tag)
{
	return (((uint64_t)tag) << 32) | (uint64_t)idx;
} // task_free_pack

static inline uint32_t
task_free_idx(uint64_t head)
{
	return (uint32_t)head;
} // task_free_idx

static inline uint32_t
task_free_tag(uint64_t head)
{
	return (uint32_t)(head >> 32);
} // task_free_tag

// Each thread sticks to one free-list shard so its head stays hot in the local
// cache.  The first allocation in a new thread claims the next shard index; if
// that shard is empty, the pop path falls back to scanning the others.
static __thread uint32_t task_free_thread_shard;
static __thread bool task_free_thread_shard_set;
static _Atomic uint32_t task_free_thread_seq;

// ---------------------------------------------------------------------------------------------//
//					DEBUGGING STUFF						//
//----------------------------------------------------------------------------------------------//

void
task_dump(struct task *t)
{
	fprintf(stderr, "Task Dump for TFD = %u\n", (unsigned)TFD_TO_INDEX(t->tfd));
	fprintf(stderr, "Expiry us = %ld\n", (long)task_atomic_load(&t->tm_tt.expiry_us));
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
	case	TASK_TYPE_LISTEN:
		if (t->listen_child) {
			fprintf(stderr, "LISTEN_CHILD, State = ");
		} else {
			fprintf(stderr, "LISTEN_PARENT, State = ");
		}
		break;
	default:
		fprintf(stderr, "BAD TYPE, State = ");
		break;
	}
	switch (task_atomic_load(&t->state)) {
	case	TASK_STATE_UNUSED:
		fprintf(stderr, "UNUSED\n");
		break;
	case	TASK_STATE_ACTIVE:
		fprintf(stderr, "ACTIVE\n");
		break;
	case	TASK_STATE_DESTROY:
		fprintf(stderr, "DESTROY\n");
		break;
	case	TASK_STATE_FREEING:
		fprintf(stderr, "FREEING\n");
		break;
	default:
		fprintf(stderr, "BAD STATE\n");
		break;
	}
	fprintf(stderr, "FD = %d, migrations = %d\n",
			t->fd, t->migrations);
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
int64_t
get_time_us(time_precision_t prec)
{
	struct timespec ts[1];
	int r;

	if ((prec == TASK_TIME_COARSE) && (__thr_current_worker != NULL) && (w__curtime_us > 0)) {
		return w__curtime_us;
	}
	r = clock_gettime(CLOCK_MONOTONIC, ts);
	if (r == 0) {
		return (TASK_S_TO_US(ts->tv_sec) + TASK_NS_TO_US(ts->tv_nsec));
	}
	return w__curtime_us;
} // get_time_us



void
task_remove_list(register struct task **head, register struct task *t)
{
	register struct task *scan;

	if ((t == NULL) || (*head == NULL)) {
		return;
	}
	if (*head == t) {
		*head = t->task_next;
		return;
	}
	for (scan = *head; scan->task_next != NULL; scan = scan->task_next) {
		if (scan->task_next == t) {
			scan->task_next = t->task_next;
			t->task_next = NULL;
			return;
		}
	}
} // task_remove_list



void
task_init(struct instance *i, struct task *t)
{
	uint32_t tfdi = (uint32_t)(t - i->tfd_pool);
	uint32_t iteration = task_next_generation(i, tfdi);
	size_t tail_off = offsetof(struct task, addrlen);

	// Initialise the fields that can be read lock-free by task_lookup() and
	// task_maybe_free() with atomic stores.  In particular, do not memset
	// state/worker/tfd: those are the fields a stale caller may still load
	// while this slot is being recycled.
	task_atomic_store(&t->active_flags, 0);
	task_atomic_store(&t->type, TASK_TYPE_NONE);
	t->io_depth = 0;
	task_atomic_store(&t->fd, -1);
	t->epfd = 0;
	task_atomic_store(&t->events, 0);
	task_atomic_store(&t->notifyqlen, 0);
	// api_refs is intentionally preserved so a stale task_ref() always has a
	// matching task_unref().
	memset((char *)t + tail_off, 0, sizeof(struct task) - tail_off);
	memset(i->tfd_stride + tfdi, 0, sizeof(struct task_stride));

	t->stride = i->tfd_stride + tfdi;
	task_atomic_store(&t->tm_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
	task_atomic_store(&t->wr_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
	task_atomic_store(&t->rd_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
	t->wr_expires_in_us = TIMER_TIME_DESTROY;
	t->rd_expires_in_us = TIMER_TIME_DESTROY;
	task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
	task_atomic_store(&t->closed, 0);
	task_atomic_store(&t->worker, NULL);

	// Publish the new generation last.  A stale lookup that raced with the
	// FREEING transition will either see the old tfd or the new one, and the
	// TFD lock in task_lookup()/task_maybe_free() revalidates it.
	task_atomic_store(&t->tfd, TFD_MAKE(i->ti, iteration, tfdi));
	task_atomic_store(&t->state, TASK_STATE_UNUSED);
} // task_init



void
task_free(struct instance *i, struct task *t, bool published)
{
	struct worker *w = task_atomic_load(&t->worker);
	uint32_t tfdi = TFD_TO_INDEX(task_atomic_load(&t->tfd));

	if (w) {
		__atomic_sub_fetch(&w->num_tasks, 1, __ATOMIC_RELAXED);
	}

	/*
	 * A published slot is only ever recycled after close has completed.
	 * task_do_close_cb() detaches every timer node while running on the
	 * owning worker, so task_free() must not touch timer heap/colt state
	 * from a foreign thread.  The unpublished path is a failed
	 * task_create(), where no timer can have been armed yet.
	 */
	if (published) {
		assert(task_atomic_load(&t->state) == TASK_STATE_FREEING);
		assert((task_atomic_load(&t->active_flags) & (uint32_t)FLG_CL) == 0);
		assert(t->tm_tt.node == NULL);
		assert(t->rd_tt.node == NULL);
		assert(t->wr_tt.node == NULL);
		assert(t->tm_tt.next == NULL);
		assert(t->rd_tt.next == NULL);
		assert(t->wr_tt.next == NULL);
	} else {
		task_destroy_timeouts(t);
	}

	// task_init() runs under the TFD lock so a stale maybe_free()/lookup
	// blocks here rather than reading a half-initialised slot.  A task that
	// never left task_create() is not visible to any other thread and can
	// skip the lock (important when the caller already holds another TFD
	// lock, e.g. during listener setup).
	if (published) {
		itfd_lock(i, tfdi);
	}
	task_init(i, t);
	if (published) {
		itfd_unlock(i, tfdi);
	}

	// Publish the slot on its sharded lock-free free list.  The node is not
	// reachable until the CAS succeeds, so storing free_next first is safe;
	// the tag protects against ABA when a slot is popped and pushed
	// concurrently.
	for (;;) {
		uint32_t shard = tfdi & TASK_FREE_SHARD_MASK;
		uint64_t head = __atomic_load_n(TASK_ATOMIC_PTR(&i->free_heads[shard].head), __ATOMIC_ACQUIRE);
		uint64_t new_head;

		task_atomic_store(&t->free_next, (uint32_t)head);
		new_head = task_free_pack(tfdi, task_free_tag(head) + 1);
		if (__atomic_compare_exchange_n(TASK_ATOMIC_PTR(&i->free_heads[shard].head), &head, new_head, false,
						__ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
			break;
		}
	}
} // task_free



// Release a task slot back to the pool if nothing else refers to it.  The
// caller must not hold the TFD lock.  This is the single place where a task
// is recycled, so the state transition to UNUSED and the free-list insertion
// can never race with another release.
void
task_maybe_free(struct task *t)
{
	struct instance *i;
	struct worker *w;
	uint32_t tfdi;
	int64_t tfd;

	w = task_atomic_load(&t->worker);
	if (w == NULL) {
		return;
	}
	i = w->instance;

	// Only state/worker/tfd may be read without the TFD lock.  task_init()
	// atomically maintains those three while holding the same lock, so a
	// stale caller that saw DESTROY before the FREEING transition blocks on
	// the lock instead of racing with the slot reset.
	if (task_atomic_load(&t->state) != TASK_STATE_DESTROY) {
		return;
	}

	tfd = task_atomic_load(&t->tfd);
	tfdi = TFD_TO_INDEX(tfd);
	itfd_lock(i, tfdi);

	if ((task_atomic_load(&t->tfd) != tfd) ||
	    (task_atomic_load(&t->state) != TASK_STATE_DESTROY) ||
	    (task_atomic_load(&t->active_flags) != 0) ||
	    (task_atomic_load(&t->notifyqlen) != 0) ||
	    (task_atomic_load(&t->notifyqlen_locked) != 0) ||
	    (task_atomic_load(&t->api_refs) != 0) ||
	    (task_atomic_load(&t->closed) == 0) ||
	    (task_atomic_load(&t->rd_state) != TASK_READ_STATE_IDLE) ||
	    (task_atomic_load(&t->wr_state) != TASK_WRITE_STATE_IDLE)) {
		itfd_unlock(i, tfdi);
		return;
	}

	// FREEING keeps every other release path (and stale lookups) out while
	// task_free() resets and rejoins the slot to the free list.  In
	// particular, a concurrent task_maybe_free() must not touch t->worker
	// after task_init() has cleared it.
	task_atomic_store(&t->state, TASK_STATE_FREEING);
	itfd_unlock(i, tfdi);
	task_free(i, t, true);
} // task_maybe_free



// Drop an internal action flag.  If the task was in the DESTROY state and this
// was the last reference, the task is recycled.
void
task_action_done(struct task *t, register task_action_flag_t action)
{
	if (action) {
		task_atomic_and(&t->active_flags, ~(uint32_t)action);
	}
	task_maybe_free(t);
} // task_action_done



// Actions that only need a short-lived mutual-exclusion bit and are never
// queued to a worker.
bool
task_action_is_meta(task_action_flag_t action)
{
	return (action & (FLG_LU | FLG_LI | FLG_MG | FLG_BND | FLG_CCB |
			  FLG_TC | FLG_TD | FLG_TS | FLG_GFD | FLG_SD |
			  FLG_DBG)) != 0;
} // task_action_is_meta



// Acquire a task for a public API operation.  Read/write/connect use a
// lock-free reservation of the relevant side (CAS IDLE -> PENDING), which is
// the hot path and must not take the TFD lock.  Meta operations and close use
// the TFD lock because they mutate listener/timer/close state.
//
// On success an API reference has been taken; the caller must call
// task_unlock() exactly once.
struct task *
task_lookup(register int64_t tfd, register task_action_flag_t action)
{
	register uint32_t tfdi;
	register uint32_t tfd_iter;
	register uint32_t ti;
	register struct instance *i;
	register struct task *t;
	register struct worker *w;
	bool locked;

	// Validate the TFD before using any of its fields
	if (tfd < 0) {
		errno = ERANGE;
		return NULL;
	}

	tfdi = TFD_TO_INDEX(tfd);
	tfd_iter = TFD_TO_ITERATION(tfd);
	ti = TFD_TO_INSTANCE(tfd);

	if (ti >= TASK_MAX_INSTANCES) {
		errno = ERANGE;
		return NULL;
	}

	// Pin the instance slot before dereferencing instances[ti].  Destroy
	// clears instances[ti] and waits for this reference before freeing.
	bool api_ref = task_api_ref_required(ti);
	if (api_ref) {
		if (!task_instance_ref(ti)) {
			errno = EBADF;
			return NULL;
		}
	}

	i = (struct instance *)__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);
	if ((i == NULL) || (i == INSTANCE_SLOT_BUSY) || (i->magic != INSTANCE_MAGIC)) {
		if (api_ref) task_api_exit(ti);
		errno = EBADF;
		return NULL;
	}

	if ((instance_state_t)task_atomic_load(&i->state) > INSTANCE_STATE_RUNNING) {
		if (api_ref) task_api_exit(ti);
		errno = EOWNERDEAD;
		return NULL;
	}

	if (tfdi >= i->tfd_pool_size) {
		if (api_ref) task_api_exit(ti);
		errno = ERANGE;
		return NULL;
	}

	t = i->tfd_pool + tfdi;
	w = task_atomic_load(&t->worker);
	if (w == NULL) {
		if (api_ref) task_api_exit(ti);
		errno = EBADF;
		return NULL;
	}

	locked = ((action & (FLG_RD | FLG_WR | FLG_CO)) == 0);

	if (locked) {
		itfd_lock(i, tfdi);
		if ((task_atomic_load(&t->state) != TASK_STATE_ACTIVE) ||
		    (task_atomic_load(&t->tfd) != tfd) ||
		    (TFD_TO_ITERATION(task_atomic_load(&t->tfd)) != tfd_iter) ||
		    (task_atomic_load(&t->worker) == NULL)) {
			itfd_unlock(i, tfdi);
			if (api_ref) task_api_exit(ti);
			errno = EBADF;
			return NULL;
		}
	} else {
		// Pin the slot first: a concurrent close may otherwise recycle it
		// between the state check and the reservation below.  task_init()
		// deliberately preserves api_refs so a stale increment made here
		// always has a matching decrement below.
		task_ref(t);
		if ((task_atomic_load(&t->state) != TASK_STATE_ACTIVE) ||
		    (task_atomic_load(&t->tfd) != tfd) ||
		    (TFD_TO_ITERATION(task_atomic_load(&t->tfd)) != tfd_iter) ||
		    (task_atomic_load(&t->worker) == NULL)) {
			task_unref(t);
			if (api_ref) task_api_exit(ti);
			errno = EBADF;
			return NULL;
		}
	}

	if (action == FLG_CL) {
		// Claim the task for close.  A second closer fails with EBADF.
		task_atomic_store(&t->state, TASK_STATE_DESTROY);
		task_atomic_store(&t->rd_shut, 1);
		task_atomic_store(&t->wr_shut, 1);
		task_atomic_store(&t->rd_cancel, 1);
		task_atomic_store(&t->wr_cancel, 1);
		task_atomic_store(&t->tm_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
		task_atomic_store(&t->closed, 0);
	} else if (action & FLG_RD) {
		uint32_t expected = TASK_READ_STATE_IDLE;

		if (!__atomic_compare_exchange_n(&t->rd_state, &expected,
						 TASK_READ_STATE_PENDING, false,
						 __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
			task_unref(t);
			if (api_ref) task_api_exit(ti);
			errno = EINPROGRESS;
			return NULL;
		}
	} else if (action & (FLG_WR | FLG_CO)) {
		uint32_t expected = TASK_WRITE_STATE_IDLE;

		if (!__atomic_compare_exchange_n(&t->wr_state, &expected,
						 TASK_WRITE_STATE_PENDING, false,
						 __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
			task_unref(t);
			if (api_ref) task_api_exit(ti);
			errno = EINPROGRESS;
			return NULL;
		}
	} else if (task_action_is_meta(action)) {
		if (task_atomic_fetch_or(&t->meta_flags, (uint32_t)action) & (uint32_t)action) {
			itfd_unlock(i, tfdi);
			if (api_ref) task_api_exit(ti);
			errno = EINPROGRESS;
			return NULL;
		}

	}

	if (locked) {
		task_atomic_inc(&t->api_refs);
	}
	errno = 0;
	return t;
} // task_lookup



// Release a task acquired by task_lookup().  The lock is only held for meta
// operations and close; the IO fast path uses an atomic reference count and
// a CAS-reserved state instead.
void
task_unlock(struct task *t, register task_action_flag_t action)
{
	int64_t tfd = task_atomic_load(&t->tfd);
	uint32_t tfdi = TFD_TO_INDEX(tfd);
	uint32_t ti = TFD_TO_INSTANCE(tfd);
	uint32_t refs;
	bool locked = ((action & (FLG_RD | FLG_WR | FLG_CO)) == 0);
	struct instance *i = NULL;

	if (locked) {
		struct worker *w = task_atomic_load(&t->worker);

		assert(w != NULL);
		i = w->instance;
		if (task_action_is_meta(action)) {
			task_atomic_and(&t->meta_flags, ~(uint32_t)action);
		}
	}

	// A _PENDING state means the API failed before committing the operation
	if ((action & FLG_RD) && (task_atomic_load(&t->rd_state) == TASK_READ_STATE_PENDING)) {
		task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
	}
	if ((action & (FLG_WR | FLG_CO)) && (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_PENDING)) {
		task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
	}

	refs = __atomic_sub_fetch(&t->api_refs, 1, __ATOMIC_SEQ_CST);

	if (locked) {
		itfd_unlock(i, tfdi);
	}
	if ((refs == 0) && (task_atomic_load(&t->state) == TASK_STATE_DESTROY)) {
		task_maybe_free(t);
	}

	// Released last: instance destruction may proceed once this drops to 0.
	if (task_api_ref_required(ti)) {
		task_api_exit(ti);
	}
} // task_unlock




bool
task_claim_listen_child(struct instance *i, struct task *c, int64_t parent_tfd)
{
	uint32_t tfdi = TFD_TO_INDEX(task_atomic_load(&c->tfd));

	itfd_lock(i, tfdi);
	if ((task_atomic_load(&c->worker) == NULL) ||
	    (task_atomic_load(&c->type) != TASK_TYPE_LISTEN) ||
	    (c->listen_child == 0) ||
	    (task_atomic_load(&c->state) != TASK_STATE_ACTIVE) ||
	    (c->stride->parent_tfd != parent_tfd)) {
		itfd_unlock(i, tfdi);
		return false;
	}
	task_atomic_store(&c->state, TASK_STATE_DESTROY);
	task_atomic_store(&c->rd_shut, 1);
	task_atomic_store(&c->wr_shut, 1);
	task_atomic_store(&c->rd_cancel, 1);
	task_atomic_store(&c->wr_cancel, 1);
	task_atomic_store(&c->closed, 0);
	task_ref(c);
	itfd_unlock(i, tfdi);
	return true;
} // task_claim_listen_child



// Utterly blows a task away.  Intended only to be called in shutdown scenarios
// when the task's worker is dead, otherwise it's unsafe as all get out
void
task_nuke(struct instance *i, uint32_t tfdi)
{
	register struct task *t = i->tfd_pool + tfdi;

	task_destroy_timeouts(t);
	if ((t->fd >= 0) && (t->registered_fd == false)) {
		int flags[1] = {0};

		ioctl(t->fd, FIONBIO, flags);
		shutdown(t->fd, SHUT_RDWR);
		close(t->fd);
		t->fd = -1;
	}
	task_init(i, t);
} // task_nuke



// Finds a free task in the task pool, and returns the TFD for it
struct task *
task_get_free_task(struct instance *i)
{
	register struct task *t;

	if (((instance_state_t)task_atomic_load(&i->state) == INSTANCE_STATE_SHUTTING_DOWN)) {
		errno = EOWNERDEAD;
		return NULL;
	}

	// The inventory of the sharded free list is the authoritative bound; the
	// old advisory tfd_pool_used/*0.8 check required a contended seq_cst RMW
	// on every allocate/free and has been removed.

	if (!task_free_thread_shard_set) {
		task_free_thread_shard = __atomic_fetch_add(TASK_ATOMIC_PTR(&task_free_thread_seq), 1, __ATOMIC_RELAXED) &
					 TASK_FREE_SHARD_MASK;
		task_free_thread_shard_set = true;
	}

	for (uint32_t scanned = 0; scanned < TASK_FREE_SHARDS; scanned++) {
		uint32_t shard = (task_free_thread_shard + scanned) & TASK_FREE_SHARD_MASK;

		for (;;) {
			uint64_t head = __atomic_load_n(TASK_ATOMIC_PTR(&i->free_heads[shard].head), __ATOMIC_ACQUIRE);
			uint32_t tfdi = task_free_idx(head);
			uint32_t next;
			uint64_t new_head;

			if (tfdi == TASK_FREE_EMPTY) {
				break;
			}
			if (tfdi >= i->tfd_pool_size) {
				errno = EMFILE;
				return NULL;
			}

			t = i->tfd_pool + tfdi;
			next = task_atomic_load(&t->free_next);
			new_head = task_free_pack(next, task_free_tag(head) + 1);
			if (__atomic_compare_exchange_n(TASK_ATOMIC_PTR(&i->free_heads[shard].head), &head, new_head,
							false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
				assert(task_atomic_load(&t->state) == TASK_STATE_UNUSED);
				task_atomic_store(&t->state, TASK_STATE_ACTIVE);
				return t;
			}
		}
	}

	errno = EMFILE;
	return NULL;
} // task_get_free_task


// ---------------------------------------------------------------------------------------------//
// 				   CPU Affinity Managment API					//
// ---------------------------------------------------------------------------------------------//


int
get_cpu_of_sock(struct instance *i, int sock)
{
	int cpu;
	socklen_t cpusz = sizeof(cpu);

	if (i->cpus == NULL) {
		return -1;
	}

	if (getsockopt(sock, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &cpusz) < 0) {
		return -1;
	}

	if ((cpu < 0) || (cpu >= i->num_cpus)) {
		return -1;
	}

	pthread_mutex_lock(&i->cpulock);
	if (i->cpus[cpu].seen == true) {
		pthread_mutex_unlock(&i->cpulock);
		return cpu;
	}
	i->cpus[cpu].seen = true;
	i->num_cpus_seen++;
	pthread_mutex_unlock(&i->cpulock);

	if (i->num_cpus_seen == i->num_cpus) {
		i->all_cpus_seen = true;
	}
	return cpu;
} // get_cpu_of_sock



int
get_cpu_adjusted_for_direction(struct instance *i, int cpu, bool is_client)
{
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



struct worker *
get_affined_worker_from_direction(struct instance *i, bool is_client)
{
	struct worker *w = NULL;
	uint64_t least = UINT64_MAX;
	uint32_t nio = i->num_workers_io;

	// Try to find the next worker with the least tasks that matches our direction
	for (uint32_t wn = 0; wn < nio; wn++) {
		struct worker *tw = NULL;

		tw = i->io_workers[wn];
		if ((tw == NULL) ||
		    ((worker_state_t)task_atomic_load(&tw->state) > WORKER_STATE_RUNNING)) {
			continue;
		}
		if (is_client) {
			if ((tw->affined_cpu & 0x1)) {
				uint64_t ntasks = (uint64_t)task_atomic_load(&tw->num_tasks);
				if (ntasks < least) {
					least = ntasks;
					w = tw;
				}
			}
		} else {
			if ((tw->affined_cpu & 0x1) == 0) {
				uint64_t ntasks = (uint64_t)task_atomic_load(&tw->num_tasks);
				if (ntasks < least) {
					least = ntasks;
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
		if ((tw == NULL) ||
		    ((worker_state_t)task_atomic_load(&tw->state) > WORKER_STATE_RUNNING)) {
			continue;
		}
		if (is_client) {
			if ((tw->affined_cpu & 0x1) == 0) {
				uint64_t ntasks = (uint64_t)task_atomic_load(&tw->num_tasks);
				if (ntasks < least) {
					least = ntasks;
					w = tw;
				}
			}
		} else {
			if ((tw->affined_cpu & 0x1)) {
				uint64_t ntasks = (uint64_t)task_atomic_load(&tw->num_tasks);
				if (ntasks < least) {
					least = ntasks;
					w = tw;
				}
			}
		}
	}

	return w;
} // get_affined_worker_from_direction



void
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



void
set_one_workers_affinity(struct instance *i, int cpu)
{
	struct worker *w = NULL;
	int row = 0, tcpu = 0;
	bool outgoing;

	// Not ideal that we're holding a mutex for this amount of time
	// but we only do it once for each IO worker, ever, so it's okay

	pthread_mutex_lock(&i->cpulock);

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

	cpu = get_cpu_adjusted_for_direction(i, cpu, outgoing);

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
		pthread_mutex_unlock(&i->cpulock);
		return;
	}

	// A single-CPU system needs special handling because the pairs-of-CPUs
	// scan below would perform zero iterations and walk off the row array.
	if (i->num_cpus < 2) {
		for (row = 0; row < i->num_cpu_rows; row++) {
			if (i->cpus[0].workers[row] == NULL) {
				break;
			}
		}
		if (row >= i->num_cpu_rows) {
			i->all_io_workers_affined = true;
			pthread_mutex_unlock(&i->cpulock);
			return;
		}
		i->cpus[0].num_workers++;
		i->cpus[0].workers[row] = w;
		w->affined_cpu = 0;
		pthread_mutex_unlock(&i->cpulock);
		set_worker_cpu_affinity(w, 0);
		return;
	}

	// We found a worker to affine, now bind it to a CPU
	// Scan row by row.  Fill out a row fully before moving on
	for (row = 0; row < i->num_cpu_rows; row++) {
		int s, sl = (i->num_cpus + 1) / 2;

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
	if ((row >= i->num_cpu_rows) || (i->cpus[tcpu].workers == NULL) ||
	    (i->cpus[tcpu].workers[row] != NULL)) {
		// No free slot found (should not happen with a consistent layout)
		i->all_io_workers_affined = true;
		pthread_mutex_unlock(&i->cpulock);
		return;
	}
	i->cpus[tcpu].num_workers++;
	i->cpus[tcpu].workers[row] = w;
	w->affined_cpu = tcpu;

	// We can finally release the lock!
	pthread_mutex_unlock(&i->cpulock);

	// Actually set the affinity now of w to tcpu
	set_worker_cpu_affinity(w, tcpu);
} // set_one_workers_affinity



void
task_set_initial_preferred_worker(struct instance *i, struct task *t, bool is_client)
{
	struct worker *tw;

	if (i->flags & TASK_FLAGS_AFFINITY_DISABLE) {
		t->stride->preferred_worker = NULL;
		return;
	}

	// If tw is not NULL and it's not already our
	// worker then make it the preferred one
	// Incoming TCP socket CPU affinity will be
	// set when the task actually migrates
	tw = get_affined_worker_from_direction(i, is_client);

	t->stride->preferred_worker = NULL;
	if (tw && (t->worker == NULL)) {
		t->stride->preferred_worker = tw;

		// At least set the incoming affinity for the TCP connection
		// Doesn't matter if this fails.  It's only a hint to the TCP stack
		if (i->flags & TASK_FLAGS_AFFINITY_NET) {
			if (setsockopt(t->fd, SOL_SOCKET, SO_INCOMING_CPU, &tw->affined_cpu, sizeof(tw->affined_cpu)) < 0) {
				return;
			}
		}
		return;
	}

	if (tw && (tw != t->worker)) {
		t->stride->preferred_worker = tw;
		return;
	}

	if (tw == NULL) {
		struct worker *w = t->worker;

		// The caller may not have assigned a worker yet.  There is no
		// affinity hint to apply in that case.
		if (w == NULL) {
			return;
		}

		// At least set the incoming affinity for the TCP connection
		// Doesn't matter if this fails.  It's only a hint to the TCP stack
		if (i->flags & TASK_FLAGS_AFFINITY_NET) {
			if (setsockopt(t->fd, SOL_SOCKET, SO_INCOMING_CPU, &w->affined_cpu, sizeof(w->affined_cpu)) < 0) {
				return;
			}
		}
		return;
	}
} // task_set_initial_preferred_worker



void
worker_learn_cpu_affinity(struct task *t)
{
	struct instance *i = t->worker->instance;
	int cpu;

	if (i->flags & TASK_FLAGS_AFFINITY_DISABLE) {
		return;
	}

	if ((i->flags & TASK_FLAGS_AFFINITY_CPU) == 0){
		return;
	}

	// We need to continue observing
	if((cpu = get_cpu_of_sock(i, t->fd)) < 0) {
		return;
	}

	if (i->all_cpus_seen) {
		set_one_workers_affinity(i, cpu);
	}
} // worker_learn_cpu_affinity



int
notifier_write(int fd)
{
	uint64_t m = 1;
	ssize_t r;

	if (fd < 0) {
		return -1;
	}

	do {
		r = write(fd, &m, sizeof (m));
	} while ((r == -1) && (errno == EINTR));

	if (r != sizeof (m)) {
		return -1;
	}

	return 0;
} // notifier_write



// Notify worker to wake up.  This quickly breaks it out of the epoll_wait() call
// when we want it to notice something has changed (like us adding new tasks to it)
int
worker_notify(struct worker *w)
{
	if (w == NULL) {
		return -1;
	}

	// The pending-notification flag is per worker, not per thread.  A thread
	// may notify several different workers in succession; using thread-local
	// state here caused all but the first notification to be suppressed.
	// If two threads race and both write, eventfd coalesces the writes.
	if (task_atomic_load(&w->notified)) {
		return 0;
	}
	task_atomic_store(&w->notified, 1);

	if (notifier_write(w->evfd) < 0) {
		task_atomic_store(&w->notified, 0);
		return -1;
	}
	return 0;
} // worker_notify



int
instance_notify(struct instance *i)
{
	if (i == NULL) {
		return -1;
	}

	worker_notify((struct worker *)task_atomic_load(&i->instance_worker));
	return notifier_write(i->evfd);
} // instance_notify



struct ntfyq *
worker_notify_get_free_ntfyq(register struct worker *w)
{
	register struct ntfyq *freeq_head = NULL, *freeq_tail = NULL;
	register struct ntfyq *tq = NULL, *ntq;
	register size_t batch_size, n;

	// Grab an aligned system page of memory, and we'll dice it up ourselves
	if ((tq = aligned_alloc(__page_size, __page_size)) == NULL) {
		return NULL;
	}
	memset(tq, 0, __page_size);
	batch_size = __page_size / sizeof(struct ntfyq);

	// Add all our new entries to a local list.  We can minimise
	// the time we hold the worker lock for by doing it this way

	for (n = 2; n < batch_size; n++) {
		ntq = tq + n;
		if (freeq_tail == NULL) {
			freeq_head = ntq;
		} else {
			freeq_tail->next = ntq;
		}
		freeq_tail = ntq;
		ntq->next = NULL;
	}

	// We don't use the first entry, but instead stick it on a worker
	// list so we have a list of what memory to pass to free() later
	worker_lock(w);
	tq->next = w->notifyq_batches;
	w->notifyq_batches = tq;
	if (lockless_worker(w)) {
		// Don't need to hold the lock if we're on the worker
		worker_unlock(w);
		freeq_tail->next = w->freeq;
		w->freeq = freeq_head;
	} else {
		freeq_tail->next = w->freeq_locked;
		w->freeq_locked = freeq_head;
		worker_unlock(w);
	}

	return tq + 1;
} // worker_notify_get_free_ntfyq


// Reserve one notification node from a worker's locked queue.  Used by
// migration so the target queue cannot fail after the migration has already
// changed ownership of the task.
struct ntfyq *
worker_notify_reserve_locked(struct worker *w)
{
	struct ntfyq *tq;

	worker_lock(w);
	if ((tq = w->freeq_locked) != NULL) {
		w->freeq_locked = tq->next;
		worker_unlock(w);
		return tq;
	}
	worker_unlock(w);

	return worker_notify_get_free_ntfyq(w);
} // worker_notify_reserve_locked




void
worker_notify_queue_locked(struct worker *w, struct ntfyq *tq, struct task *t,
			   task_action_flag_t action)
{
	*tq = (struct ntfyq) { .t = t, .action = action, .next = NULL };
	task_atomic_inc(&t->notifyqlen_locked);

	worker_lock(w);
	if (w->notifyq_locked_tail == NULL) {
		w->notifyq_locked_head = tq;
	} else {
		w->notifyq_locked_tail->next = tq;
	}
	w->notifyq_locked_tail = tq;
	worker_unlock(w);

	worker_notify(w);
} // worker_notify_queue_locked



/*
 * Central deferred-close queue.
 *
 * A close request never allocates a notification node.  It pins the task with
 * FLG_CL, marks it DESTROY, and links the slot index onto its owner worker's
 * close list using task->free_next.  The owner drains that list at a safe
 * point after the current epoll/poll/notify batch.  task->free_next is not the
 * free-list link until the task is recycled, and FLG_CL prevents recycling.
 */
static void
task_close_link(struct worker *w, struct task *t, uint32_t tfdi)
{
	worker_lock(w);
	task_atomic_store(&t->free_next, task_atomic_load(&w->close_head));
	task_atomic_store(&w->close_head, tfdi);
	worker_unlock(w);

	if (!lockless_worker(w)) {
		worker_notify(w);
	}
} // task_close_link


// Common close transition.  The caller holds the task's TFD lock.
bool
task_defer_close_locked(struct task *t, uint32_t tfdi)
{
	struct worker *w;
	uint32_t state;

	if (t == NULL) {
		return false;
	}

	state = task_atomic_load(&t->state);
	if ((state == TASK_STATE_UNUSED) || (state == TASK_STATE_FREEING)) {
		return false;
	}

	if (state == TASK_STATE_ACTIVE) {
		task_atomic_store(&t->state, TASK_STATE_DESTROY);
		task_atomic_store(&t->rd_shut, 1);
		task_atomic_store(&t->wr_shut, 1);
		task_atomic_store(&t->rd_cancel, 1);
		task_atomic_store(&t->wr_cancel, 1);
		task_atomic_store(&t->closed, 0);
	}

	// Cancel the user timer immediately; IO timeouts are detached by
	// task_do_close_cb() on the owning worker.
	task_atomic_store(&t->tm_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
	t->tm_cancelled = 1;

	if (task_atomic_load(&t->closed) != 0) {
		return false;
	}

	// The pin is set while holding the TFD lock, so task_maybe_free() cannot
	// recycle the slot underneath us.  A second close request is a no-op.
	if (task_atomic_fetch_or(&t->active_flags, (uint32_t)FLG_CL) & (uint32_t)FLG_CL) {
		return true;
	}

	w = task_atomic_load(&t->worker);
	if (w == NULL) {
		// Shutdown's final nuke pass will reap this slot.  It is already
		// DESTROY and pinned, so it cannot be recycled before then.
		return false;
	}

	task_close_link(w, t, tfdi);
	return true;
} // task_defer_close_locked


// Defer a close for a task identified by its current slot index.
bool
task_defer_close_index(struct instance *i, uint32_t tfdi)
{
	struct task *t;
	int64_t tfd;
	bool result;

	if ((i == NULL) || (tfdi >= i->tfd_pool_size)) {
		return false;
	}

	t = i->tfd_pool + tfdi;
	itfd_lock(i, tfdi);
	tfd = task_atomic_load(&t->tfd);
	if ((TFD_TO_INDEX(tfd) != tfdi) || (TFD_TO_INSTANCE(tfd) != (uint32_t)i->ti)) {
		itfd_unlock(i, tfdi);
		return false;
	}
	result = task_defer_close_locked(t, tfdi);
	itfd_unlock(i, tfdi);
	return result;
} // task_defer_close_index


// Defer a close for a live task pointer.
bool
task_defer_close(struct task *t)
{
	struct worker *w;
	struct instance *i;
	uint32_t tfdi;

	if (t == NULL) {
		return false;
	}
	w = task_atomic_load(&t->worker);
	if (w == NULL) {
		return false;
	}
	i = w->instance;
	if (i == NULL) {
		return false;
	}
	tfdi = (uint32_t)(t - i->tfd_pool);
	if (tfdi >= i->tfd_pool_size) {
		return false;
	}
	return task_defer_close_index(i, tfdi);
} // task_defer_close


// Re-home an already-pinned close (legacy FLG_CL notification or a migration
// that changed the owner after the close was queued).
bool
task_close_forward(struct task *t)
{
	struct worker *w;
	struct instance *i;
	uint32_t tfdi;

	if ((t == NULL) || (task_atomic_load(&t->state) != TASK_STATE_DESTROY) ||
	    ((task_atomic_load(&t->active_flags) & (uint32_t)FLG_CL) == 0)) {
		return false;
	}
	w = task_atomic_load(&t->worker);
	if (w == NULL) {
		return false;
	}
	i = w->instance;
	if (i == NULL) {
		return false;
	}
	tfdi = (uint32_t)(t - i->tfd_pool);
	if (tfdi >= i->tfd_pool_size) {
		return false;
	}
	task_close_link(w, t, tfdi);
	return true;
} // task_close_forward


// Drain a worker's deferred-close list.  This is the only normal caller of
// task_do_close_cb(); it always runs on the owning worker.
void
worker_process_closes(struct worker *w)
{
	struct instance *i;
	uint32_t list;

	if ((w == NULL) || ((i = w->instance) == NULL)) {
		return;
	}

	for (;;) {
		worker_lock(w);
		list = task_atomic_load(&w->close_head);
		task_atomic_store(&w->close_head, TASK_CLOSE_EMPTY);
		worker_unlock(w);

		if (list == TASK_CLOSE_EMPTY) {
			return;
		}

		while (list != TASK_CLOSE_EMPTY) {
			uint32_t tfdi = list;
			struct task *t;
			struct worker *tw;

			if (tfdi >= i->tfd_pool_size) {
				assert(0);
				return;
			}
			t = i->tfd_pool + tfdi;

			// Read the link before close teardown can recycle the slot and
			// overwrite free_next.
			list = task_atomic_load(&t->free_next);

			if ((task_atomic_load(&t->state) != TASK_STATE_DESTROY) ||
			    ((task_atomic_load(&t->active_flags) & (uint32_t)FLG_CL) == 0)) {
				continue;
			}

			tw = task_atomic_load(&t->worker);
			if (tw != w) {
				// Migration raced with close.  Forward to the current
				// owner; the pin keeps the slot alive.
				if (tw != NULL) {
					task_close_link(tw, t, tfdi);
				}
				continue;
			}

			task_do_close_cb(t);
		}
	}
} // worker_process_closes


// Must be called with the task lock held. 
bool
task_notify_action(register struct task *t, register task_action_flag_t action, bool force_close_q, bool force_q)
{
	register struct worker *w = task_atomic_load(&t->worker);
	register struct ntfyq *tq = NULL;
	uint32_t state;

	(void)force_close_q;

	if (w == NULL) {		// This can be true sometimes during shutdown
		return false;
	}

	if (action == FLG_CL) {
		// Close is never queued as a notification.  This entry point exists
		// only for callers that still hold the TFD lock; route it through the
		// allocation-free deferred queue.
		return task_defer_close_locked(t, TFD_TO_INDEX(task_atomic_load(&t->tfd)));
	}

	state = task_atomic_load(&t->state);
	if (state != TASK_STATE_ACTIVE) {
		return false;
	}

	// Do not lock IO timeout flags.  Their presence is already implied by their parent
	// FLG_RD/FLG_WR flags. Also don't lock for connecting events, write flags cover this
	if ((action & (FLG_RD | FLG_WR | FLG_RT | FLG_WT | FLG_CN | FLG_TM)) == 0) {
		// Only call task_lock() if we actually need to
		if ((task_atomic_load(&t->active_flags) & action) == 0) {
			task_lock(t, action);
		}
	}

	if (lockless_worker(w)) {
		// Try to process the action immediately if it's allowable.  We always queue
		// closes to give other activities a chance to realise the task is finished
		if (force_q == false) {
			if (((action & (FLG_CN | FLG_MG | FLG_CL | FLG_WR | FLG_RD)) == 0)) {
				worker_process_one_action(w, t, action);
				return true;
			}
			if (action & (FLG_RD | FLG_WR)) {
				if (t->io_depth < TASK_MAX_IO_DEPTH) {
					worker_process_one_action(w, t, action);
					return true;
				}
			}
		}

		// Allocate a notification node.  Running out is reported as a
		// failure to the caller rather than aborting the process.
		if ((tq = w->freeq)) {
			w->freeq = tq->next;
		} else {
			tq = worker_notify_get_free_ntfyq(w);
			if (tq == NULL) {
				task_atomic_and(&t->active_flags, ~(uint32_t)action);
				return false;
			}
		}

		*tq = (struct ntfyq) { .t = t, .action = action, .next = NULL };
		task_atomic_inc(&t->notifyqlen);

		if (w->notifyq_tail == NULL) {
			w->notifyq_head = tq;
		} else {
			w->notifyq_tail->next = tq;
		}
		w->notifyq_tail = tq;
	} else {
		task_atomic_inc(&t->notifyqlen_locked);

		worker_lock(w);
		if ((tq = w->freeq_locked) != NULL) {
			w->freeq_locked = tq->next;
		} else {
			// Can't be holding the worker lock when
			// calling worker_notify_get_free_ntfyq
			worker_unlock(w);
			tq = worker_notify_get_free_ntfyq(w);
			if (tq == NULL) {
				task_atomic_dec(&t->notifyqlen_locked);
				task_atomic_and(&t->active_flags, ~(uint32_t)action);
				return false;
			}
			worker_lock(w);
		}

		*tq = (struct ntfyq) { .t = t, .action = action, .next = NULL };

		if (w->notifyq_locked_tail == NULL) {
			w->notifyq_locked_head = tq;
		} else {
			w->notifyq_locked_tail->next = tq;
		}
		w->notifyq_locked_tail = tq;
		worker_unlock(w);
	}

	// A notification queued by the owning worker itself will be seen before
	// it next blocks in epoll_wait(), so do not make it write to its own
	// eventfd.  Foreign callers still need to wake the owner.
	if (!lockless_worker(w)) {
		worker_notify(w);
	}
	return true;
} // task_notify_action


// Queue a close.  This never allocates and never tears down inline.
bool
task_queue_close(struct task *t)
{
	return task_defer_close(t);
} // task_queue_close



// Destroy all the notifications on the worker's notification queue
void
worker_cleanup(struct worker *w)
{
	struct ntfyq *owned, *locked, *tq;

	// Bulk grab the queues to process.  This minimises lock contention/churn
	owned = w->notifyq_head;
	w->notifyq_head = NULL;
	w->notifyq_tail = NULL;
	w->freeq = NULL;

	worker_lock(w);
	locked = w->notifyq_locked_head;
	w->notifyq_locked_head = NULL;
	w->notifyq_locked_tail = NULL;
	w->freeq_locked = NULL;
	worker_unlock(w);

	// Process both lists.  At shutdown every live task is routed through the
	// deferred close queue; teardown happens below, after the notification
	// batch has been consumed.  The notification count is dropped *after* the
	// close has been queued so the slot stays alive for the whole batch.
	for (int pass = 0; pass < 2; pass++) {
		for (tq = (pass == 0) ? owned : locked; tq != NULL; tq = tq->next) {
			struct task *t = tq->t;
			task_action_flag_t action = tq->action;

			if (t == NULL) {
				continue;
			}

			// The task may have migrated since this action was queued.
			// Do not touch its new owner's state from this thread; the new
			// owner will process its own close, or the final nuke will reap
			// the slot.
			if (t->worker == w) {
				if (action == FLG_CL) {
					// Legacy close notification: move it to the close queue.
					(void)task_close_forward(t);
				} else {
					// Queue the close first, then drop the action.  FLG_CL
					// keeps the task pinned across both steps.
					(void)task_defer_close(t);
					task_action_done(t, action);
				}
			}

			if (pass == 0) {
				task_atomic_dec(&t->notifyqlen);
			} else {
				task_atomic_dec(&t->notifyqlen_locked);
			}
			if (task_atomic_load(&t->state) == TASK_STATE_DESTROY) {
				task_maybe_free(t);
			}
		}
	}

	// Now run teardown for everything queued above.
	worker_process_closes(w);

	// Notification batches are deliberately *not* freed here.  During
	// shutdown another worker may still notify a task owned by this worker;
	// queued nodes may point into these batches.  worker_destroy() frees
	// them once every worker thread has stopped.
} // worker_cleanup



// Free up a worker's state entirely
void
worker_destroy(struct worker *w)
{
	struct instance *i;
	struct ntfyq *tq;

	if (w == NULL) return;
	i = w->instance;
	bool was_started = w->started;

	// Make sure the thread has actually exited before freeing its state.
	// The self-join guard covers the hard-shutdown path.
	if (w->started) {
		if (!pthread_equal(pthread_self(), w->thr)) {
			pthread_join(w->thr, NULL);
		}
		w->started = 0;
	}

	// A worker that ran must never be destroyed with a deferred close still
	// pinned: that task would have no route to teardown.  A worker that was
	// created but never started has no thread to drain its list; its tasks are
	// reaped by instance_nuke_tasks() after every worker has been released, so
	// the stale index list is simply discarded here.
	if (was_started) {
		assert(task_atomic_load(&w->close_head) == TASK_CLOSE_EMPTY);
	} else {
		task_atomic_store(&w->close_head, TASK_CLOSE_EMPTY);
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
	if (w->evfd >= 0) {
		close(w->evfd);
		w->evfd = -1;
	}
	// Free any notification batches not already released by worker_cleanup()
	while ((tq = w->notifyq_batches) != NULL) {
		w->notifyq_batches = tq->next;
		free(tq);
	}
	if (w->timer_queue) {
		pheap_destroy(w->timer_queue, NULL);
		w->timer_queue = NULL;
	}
	if (w->gepfd >= 0) {
		close(w->gepfd);
		w->gepfd = -1;
	}
	if (w->pollfds) {
		free(w->pollfds);
		w->pollfds = NULL;
		w->max_pollfds = 0;
	}
	if (w->events) {
		free(w->events);
		w->events = NULL;
		w->max_events = 0;
	}

	pthread_mutex_destroy(&w->lock);

	free(w->poll_tasks);
	free(w);
	w = NULL;
} // worker_destroy



// Selects an io worker to send a task to.  We just do a round robin selection
struct worker *
worker_select_io_worker(struct instance *i, struct task *t, bool outgoing)
{
	struct worker *w = NULL;
	int n, nio;
	uint64_t least = UINT64_MAX;

	// Try CPU affinity method for worker selection first
	if (t && i->all_io_workers_affined && (t->fd >= 0)) {
		task_set_initial_preferred_worker(i, t, outgoing);
		if ((w = t->stride->preferred_worker) != NULL) {
			t->stride->preferred_worker = NULL;
			return w;
		}
	}

	// Select the worker with the least number of tasks
	nio = i->num_workers_io;
	for (n = 0; n < nio; n++) {
		struct worker *tw = i->io_workers[n];

		if (tw == NULL) {
			continue;
		}
		if ((worker_state_t)task_atomic_load(&tw->state) > WORKER_STATE_RUNNING) {
			continue;
		}
		uint64_t ntasks = (uint64_t)task_atomic_load(&tw->num_tasks);
		if (ntasks < least) {
			least = ntasks;
			w = tw;
		}
	}
	return w;
} // worker_select_io_worker



// Choose the first running IO worker we find to attach to
int
instance_assign_worker(struct instance *i)
{
	struct worker *w;

	if ((w = worker_select_io_worker(i, NULL, false)) == NULL) {
		// No available workers
		task_atomic_store(&i->instance_worker, NULL);
		errno = ECHILD;
		return -1;
	}
	task_atomic_store(&i->instance_worker, w);
	return 0;
} // instance_assign_worker



void
worker_set_state(struct worker *w, worker_state_t new_state)
{
	struct instance *i;

	// First handle a hard instance shutdown scenario without trying to lock
	// a NULL instance pointer.
	if ((i = w->instance) == NULL) {
		task_atomic_store(&w->old_state, (uint32_t)task_atomic_load(&w->state));
		task_atomic_store(&w->state, (uint32_t)(new_state));
		if (new_state == WORKER_STATE_DEAD) {
			worker_destroy(w);
		}
		return;
	}

	instance_lock(i);

	// A blocking worker may finish work while shutdown is moving it to
	// WORKER_STATE_SHUTTING_DOWN.  Do not let its normal IDLE/NOTIFYING
	// transition pull it back off the shutdown list; the worker loop will
	// observe SHUTTING_DOWN and proceed to DEAD instead.
	if (((new_state == WORKER_STATE_IDLE) ||
	     (new_state == WORKER_STATE_NOTIFYING)) &&
	    (task_atomic_load(&w->state) == WORKER_STATE_SHUTTING_DOWN)) {
		instance_unlock(i);
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
		if (i->num_blocking_idle > 0) {
			i->num_blocking_idle--;
		}
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

	task_atomic_store(&w->old_state, (uint32_t)task_atomic_load(&w->state));

	switch(new_state) {
	case WORKER_STATE_LIMBO:
		// Do nothing. It shouldn't be attached to any list
		break;
	case WORKER_STATE_CREATED:
		TAILQ_INSERT_TAIL(&i->workers_created, w, list);
		break;
	case WORKER_STATE_IDLE:
		if (TAILQ_EMPTY(&i->workers_idle)) {
			i->worker_idle_empty_reaped = 0;
			i->worker_idle_empty_time_us = get_time_us(TASK_TIME_PRECISE);
		}
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
		// A dead blocking worker no longer counts against the pool limit
		if ((w->type == WORKER_TYPE_BLOCKING) && (i->num_blocking_workers > 0)) {
			i->num_blocking_workers--;
		}
		break;
	default:
		assert(0);
	}

	task_atomic_store(&w->state, (uint32_t)(new_state));

	// Move the instance to a new worker if we need to
	if ((task_atomic_load(&i->instance_worker) != NULL) &&
	    (task_atomic_load(&i->instance_worker) == w)) {
		if (task_atomic_load(&w->state) != WORKER_STATE_RUNNING) {
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



int
worker_start(struct worker *w)
{
	if ((w->type != WORKER_TYPE_IO) && (w->type != WORKER_TYPE_BLOCKING)) {
		errno = EINVAL;
		return -1;
	}

	if (w->type == WORKER_TYPE_BLOCKING) {
		// Only create the thread once.  Reused idle workers already have a
		// thread blocked on their eventfd and only need a wakeup.
		if (!w->started) {
			worker_set_state(w, WORKER_STATE_BLOCKING);
			if (pthread_create(&w->thr, NULL, worker_loop_blocking, w) != 0) {
				goto worker_start_failed;
			}
			w->started = 1;
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
		w->started = 1;
		worker_notify(w);
		return 0;
	} 

worker_start_failed:
	worker_set_state(w, WORKER_STATE_DEAD);
	errno = ECHILD;
	return -1;
} // worker_start



// Create a new worker state and initialise it
struct worker *
worker_create(struct instance *i, int worker_type)
{
	register struct worker *w = NULL;
	register size_t sz;
	pthread_mutexattr_t attr;
	int num_pages = 3;

	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP);

	if ((worker_type != WORKER_TYPE_IO) && (worker_type != WORKER_TYPE_BLOCKING)) {
		errno = EINVAL;
		pthread_mutexattr_destroy(&attr);
		return NULL;
	}

	for (sz = 1; sz < sizeof(struct worker); sz += sz);
	// Create the worker state itself
	if ((w = aligned_alloc(sz, sz)) == NULL) {
		pthread_mutexattr_destroy(&attr);
		return NULL;
	}
	memset(w, 0, sz);
	// Initialise the fields worker_destroy() needs before any allocation can
	// fail.  memset() would otherwise leave type 0 (IO) and instance NULL.
	w->instance = i;
	w->type = worker_type;
	w->affined_cpu = -1;
	w->gepfd = -1;
	w->evfd = -1;
	task_atomic_store(&w->close_head, TASK_CLOSE_EMPTY);
	pthread_mutex_init(&w->lock, &attr);

	// Now initialise the worker state
	task_atomic_store(&w->state, (uint32_t)(WORKER_STATE_LIMBO));

	if (w->type == WORKER_TYPE_IO) {
		// Only IO workers need epoll/poll/listener/timer state.
		if ((w->events = aligned_alloc(__page_size, (__page_size * num_pages))) == NULL) {
			goto worker_create_failed;
		}
		w->max_events = (__page_size * num_pages) / sizeof(struct epoll_event);
		w->num_events = w->max_events;

		if ((w->pollfds = aligned_alloc(__page_size, __page_size)) == NULL) {
			goto worker_create_failed;
		}
		w->max_pollfds = __page_size / sizeof(struct pollfd);
		if ((w->poll_tasks = calloc(w->max_pollfds, sizeof(struct task *))) == NULL) {
			goto worker_create_failed;
		}

		if ((w->gepfd = epoll_create1(EPOLL_CLOEXEC)) < 0) {
			goto worker_create_failed;
		}

		// Create event fd for task event loop notifications
		if ((w->evfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
			// eventfd will set errno
			goto worker_create_failed;
		} else {
			// Register worker notification event fd against the worker epoll fd
			// Do not use EPOLLET here since we don't do selective re-arming
			struct epoll_event ev = {.data.u64 = TASK_EPOLL_EVENTFD, .events = EPOLLIN };

			if (epoll_ctl(w->gepfd, EPOLL_CTL_ADD, w->evfd, &ev) < 0) {
				// epoll_ctl will set errno
				goto worker_create_failed;
			}
		}
	} else if (w->type == WORKER_TYPE_BLOCKING) {
		// Blocking workers use a blocking eventfd and just wait on that instead of epoll_wait
		if ((w->evfd = eventfd(0, EFD_CLOEXEC)) < 0) {
			// eventfd will set errno
			goto worker_create_failed;
		}
	} else {
		errno = EINVAL;
		goto worker_create_failed;
	}

	// Create timer queue for IO workers.  Use the default keys as intptr_t
	// comparison function.
	if ((w->type == WORKER_TYPE_IO) && ((w->timer_queue = pheap_create(NULL)) == NULL)) {
		errno = ENOMEM;
		goto worker_create_failed;
	}

	// Success!
	pthread_mutexattr_destroy(&attr);
	worker_set_state(w, WORKER_STATE_CREATED);
	return w;

	// Failure :(
worker_create_failed:
	pthread_mutexattr_destroy(&attr);
	worker_destroy(w);
	return NULL;
} // worker_create



// Run through list of workers.  Remove anything marked as dead and destroy its state
void
instance_reap_workers(struct instance *i)
{
	struct worker_list workers_to_destroy;
	struct worker *w, *wn;

	TAILQ_INIT(&workers_to_destroy);

	// Move all workers to destroy scan list
	if (!TAILQ_EMPTY(&i->workers_dead)) {
		instance_lock(i);
		TAILQ_CONCAT(&workers_to_destroy, &i->workers_dead, list);
		instance_unlock(i);
	}

	// Move workers that are still alive back to the worker list
	// Destroy anything that's dead
	w = TAILQ_FIRST(&workers_to_destroy);
	while (w != NULL) {
		wn = TAILQ_NEXT(w, list);
		worker_destroy(w);
		w = wn;
	}
} // instance_reap_workers



// Queue a close action for every live task.  The task's owning worker performs
// the actual fd/timer cleanup, preserving the single-owner access model instead
// of mutating another worker's state from the shutdown thread.
void
instance_queue_shutdown(struct instance *i)
{
	for (uint32_t n = 0; n < i->tfd_pool_size; n++) {
		// Close requests are allocation-free and pin DESTROY tasks.  If a
		// task cannot be queued because its worker is already gone, the
		// final nuke pass reaps it.
		(void)task_defer_close_index(i, n);
	}
} // instance_queue_shutdown



// Nuke any task that survived the worker cleanup phase.  This must only be
// called after all workers have been reaped, otherwise t->worker would be a
// dangling pointer.
void
instance_nuke_tasks(struct instance *i)
{
	for (uint32_t n = 0; n < i->tfd_pool_size; n++) {
		struct task *t = i->tfd_pool + n;

		if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) || (task_atomic_load(&t->state) == TASK_STATE_DESTROY)) {
			// Workers are gone.  Do not try to touch their timer queues.
			task_atomic_store(&t->worker, NULL);
			task_nuke(i, n);
		}
	}
} // instance_nuke_tasks



// Wait until all workers have transitioned to DEAD.  Worker state changes
// notify the instance eventfd, so this blocks without polling.
void
instance_wait_workers(struct instance *i)
{
	for (;;) {
		bool busy;

		instance_lock(i);
		busy = !TAILQ_EMPTY(&i->workers_created) ||
		       !TAILQ_EMPTY(&i->workers_running) ||
		       !TAILQ_EMPTY(&i->workers_blocking) ||
		       !TAILQ_EMPTY(&i->workers_notify) ||
		       !TAILQ_EMPTY(&i->workers_idle) ||
		       !TAILQ_EMPTY(&i->workers_shutdown);
		instance_unlock(i);

		if (!busy) {
			return;
		}

		if (i->evfd < 0) {
			poll(NULL, 0, 1000);
			continue;
		}

		uint64_t c;
		ssize_t len = read(i->evfd, (void *)&c, sizeof(c));
		if (len < 0) {
			if ((errno == EINTR) || (errno == EAGAIN)) {
				continue;
			}
			/* Never close the instance eventfd from a wait path: other
			 * threads may be notifying it.  instance_destroy() owns it. */
			poll(NULL, 0, 1);
		}
	}
} // instance_wait_workers



// Wait for all in-flight task API calls against this instance to finish.
// Lookups increment the slot reference before dereferencing instances[ti], so
// after instances[ti] has been cleared this is a complete barrier against both
// existing and newly arriving callers.
static void
instance_wait_api_refs(uint32_t ti)
{
	for (;;) {
		uint64_t total = 0;

		for (uint32_t shard = 0; shard < TASK_API_REF_SHARDS; shard++) {
			total += __atomic_load_n(TASK_ATOMIC_PTR(&task_api_slot_refs[ti][shard].refs),
					      __ATOMIC_SEQ_CST);
		}
		if (total == 0) {
			return;
		}
		sched_yield();
	}
} // instance_wait_api_refs



// Ask all workers to shutdown.
void
instance_shutdown_workers(struct instance *i)
{
	struct worker_list *lists[] = {
		&i->workers_created,
		&i->workers_running,
		&i->workers_blocking,
		&i->workers_idle,
		&i->workers_notify,
	};
	struct worker **notify = NULL;
	size_t num_notify = 0, notify_capacity = 0;
	bool notify_under_lock = false;
	struct worker *w;
	size_t count = 0;

	instance_lock(i);

	// Count first so we can keep the list transition atomic with respect to
	// worker_set_state().  Never move unprocessed workers to an untracked
	// local list: a worker can finish blocking work and transition itself to
	// IDLE/NOTIFYING while the instance lock is dropped.
	for (size_t n = 0; n < sizeof(lists) / sizeof(lists[0]); n++) {
		TAILQ_FOREACH(w, lists[n], list) {
			count++;
		}
	}
	if (count > 0) {
		notify_capacity = count;
		notify = malloc(notify_capacity * sizeof(*notify));
		if (notify == NULL) {
			// Notification is safe under the instance lock: worker eventfds
			// are non-blocking (or an eventfd write cannot block in practice).
			notify_under_lock = true;
		}
	}

	for (size_t n = 0; n < sizeof(lists) / sizeof(lists[0]); n++) {
		struct worker_list *list = lists[n];

		while ((w = TAILQ_FIRST(list))) {
			bool wake = true;

			TAILQ_REMOVE(list, w, list);

			if (list == &i->workers_idle) {
				if (i->num_blocking_idle > 0) {
					i->num_blocking_idle--;
				}
			}

			if ((task_atomic_load(&w->state) == WORKER_STATE_CREATED) && !w->started) {
				// The thread was never created, so there is nothing to wake.
				// Move it straight to DEAD so the shutdown waiter does not
				// block forever.
				task_atomic_store(&w->old_state, (uint32_t)WORKER_STATE_CREATED);
				task_atomic_store(&w->state, (uint32_t)WORKER_STATE_DEAD);
				if ((w->type == WORKER_TYPE_BLOCKING) &&
				    (i->num_blocking_workers > 0)) {
					i->num_blocking_workers--;
				}
				TAILQ_INSERT_TAIL(&i->workers_dead, w, list);
				wake = false;
			} else {
				task_atomic_store(&w->state, (uint32_t)WORKER_STATE_SHUTTING_DOWN);
				TAILQ_INSERT_TAIL(&i->workers_shutdown, w, list);
			}

			if (!wake) {
				continue;
			}
			if (notify_under_lock) {
				worker_notify(w);
			} else {
				notify[num_notify++] = w;
			}
		}
	}

	instance_unlock(i);

	for (size_t n = 0; n < num_notify; n++) {
		worker_notify(notify[n]);
	}
	free(notify);
} // instance_shutdown_workers



void
instance_destroy(struct instance *i)
{
	if ((i == NULL) || (i->magic != INSTANCE_MAGIC)) {
		return;
	}
	if (i->evfd >= 0) {
		close(i->evfd);
		i->evfd = -1;
	}
	pthread_mutex_destroy(&i->listeners_lock);
	pthread_mutex_destroy(&i->lock);
	pthread_mutex_destroy(&i->cpulock);
	if (i->io_workers) {
		free(i->io_workers);
		i->io_workers = NULL;
	}
	if (i->tfd_pool) {
		free((void *)i->tfd_pool);
		i->tfd_pool = NULL;
	}
	if (i->tfd_stride) {
		free((void *)i->tfd_stride);
		i->tfd_stride = NULL;
	}
#ifndef USE_TICKET_LOCKS
	for (uint32_t n = 0; n < TASK_MAX_TFD_LOCKS + 1; n++) {
		pthread_mutex_destroy(&(i->tfd_locks[n].lock));
	}
#endif
	if (i->tfd_locks_real) {
		// Hack to get around compiler warning stubborness over passing volatile
		// pointers to free().  Hey, don't judge me!  It gets the job done!
		union { volatile void *a; void *b;} whatevs;
		whatevs.a = i->tfd_locks_real;
		free(whatevs.b);
		i->tfd_locks_real = NULL;
		i->tfd_locks = NULL;
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
	memset(i, 0, sizeof(struct instance));
	i->magic = INSTANCE_MAGIC;
	i->state = INSTANCE_STATE_FREE;
	free(i);
} // instance_destroy



// ---------------------------------------------------------------------------------------------//
// 				Task FD Pool Managment API					//
// ---------------------------------------------------------------------------------------------//

int
instance_tfd_pool_init(struct instance *i, uint32_t pool_size)
{
	register size_t num_pages;
	uint32_t n;
#ifndef USE_TICKET_LOCKS
	pthread_mutexattr_t attr;

	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP);
#endif

	// Don't allow less than 100 tfd entries for pool size.  Do the growth
	// arithmetic in uint64_t: max_tasks is a uint32_t supplied by the API and
	// the old uint32_t +=/*= sequence wrapped for large values, silently
	// creating a tiny pool instead of rejecting the request.
	uint64_t wanted = (pool_size < 100) ? 100 : pool_size;

	wanted += 100;	// To allow for any listener and timer tasks
	wanted = (wanted * 130) / 100;	// 130% for hashing efficiency
	if ((wanted > UINT32_MAX) ||
	    (wanted > (SIZE_MAX / sizeof(struct task))) ||
	    (wanted > (SIZE_MAX / sizeof(struct task_stride)))) {
		errno = EINVAL;
#ifndef USE_TICKET_LOCKS
		pthread_mutexattr_destroy(&attr);
#endif
		return -1;
	}
	pool_size = (uint32_t)wanted;

	i->tfd_pool_size = pool_size;

	// Grow the persistent per-slot generation table if necessary.  The table is
	// intentionally not freed with the instance: keeping it alive preserves
	// tfd uniqueness across destroy/recreate cycles that reuse the same ti.
	{
		struct tfd_generation_state *g = &task_tfd_generations[i->ti];

		if (g->capacity < pool_size) {
			uint32_t *ng = realloc(g->gens, (size_t)pool_size * sizeof(*ng));

			if (ng == NULL) {
#ifndef USE_TICKET_LOCKS
				pthread_mutexattr_destroy(&attr);
#endif
				return -1;
			}
			memset(ng + g->capacity, 0,
			       (size_t)(pool_size - g->capacity) * sizeof(*ng));
			g->gens = ng;
			g->capacity = pool_size;
		}
	}

	// Allocate the "stride" task area before the "active" area
	num_pages = pool_size * sizeof(struct task_stride);
	num_pages += (__page_size - 1);
	num_pages /= __page_size;

	if ((i->tfd_stride = aligned_alloc(__page_size, num_pages * __page_size)) == NULL) {
		i->tfd_stride = NULL;
#ifndef USE_TICKET_LOCKS
		pthread_mutexattr_destroy(&attr);
#endif
		return -1;
	}
	memset(i->tfd_stride, 0, num_pages * __page_size);

	// Allocate the "active" task tfd_pool space now
	num_pages = pool_size * sizeof(struct task);
	num_pages += (__page_size - 1);
	num_pages /= __page_size;

	if ((i->tfd_pool = aligned_alloc(__page_size, num_pages * __page_size)) == NULL) {
		i->tfd_pool = NULL;
#ifndef USE_TICKET_LOCKS
		pthread_mutexattr_destroy(&attr);
#endif
		return -1;
	}
	memset(i->tfd_pool, 0, num_pages * __page_size);
	uint32_t free_head[TASK_FREE_SHARDS];

	for (uint32_t shard = 0; shard < TASK_FREE_SHARDS; shard++) {
		free_head[shard] = TASK_FREE_EMPTY;
	}
	for (n = 0; n < pool_size; n++) {
		uint32_t tfdi = pool_size - (n + 1);
		uint32_t shard = tfdi & TASK_FREE_SHARD_MASK;
		struct task *t = i->tfd_pool + tfdi;

		task_init(i, t);
		task_atomic_store(&t->free_next, free_head[shard]);
		free_head[shard] = tfdi;
	}
	for (uint32_t shard = 0; shard < TASK_FREE_SHARDS; shard++) {
		__atomic_store_n(TASK_ATOMIC_PTR(&i->free_heads[shard].head),
				 task_free_pack(free_head[shard], 0), __ATOMIC_RELEASE);
	}

	// Allocate the lock storage now.  We 
	num_pages = (TASK_MAX_TFD_LOCKS + 1) * sizeof(tfd_lock_t);
	num_pages += (__page_size - 1);
	num_pages /= __page_size;

	if ((i->tfd_locks_real = aligned_alloc(__page_size, num_pages * __page_size)) == NULL) {
		i->tfd_locks = NULL;
#ifndef USE_TICKET_LOCKS
		pthread_mutexattr_destroy(&attr);
#endif
		return -1;
	}
	union { volatile void *a; void *b;} whatevs;
	whatevs.a = i->tfd_locks_real;
	memset(whatevs.b, 0, num_pages * __page_size);
	for (n = 0; n < TASK_MAX_TFD_LOCKS + 1; n++) {
#ifdef USE_TICKET_LOCKS
		ticket_init(&(i->tfd_locks_real[n].lock));
#else
		pthread_mutex_init(&(i->tfd_locks_real[n].lock), &attr);
#endif
	}
	i->tfd_locks = i->tfd_locks_real + 1;
#ifndef USE_TICKET_LOCKS
	pthread_mutexattr_destroy(&attr);
#endif
	return 0;
} // instance_tfd_pool_init



struct instance *
instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks)
{
	uint32_t ti;
	struct instance *i = NULL;
	pthread_mutexattr_t attr;

	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP);

	// Reserve an empty slot under the global lock, then construct the
	// instance without holding it.  The BUSY sentinel makes the slot
	// unavailable to another creator and unhittable by API callers.
	pthread_mutex_lock(&creation_lock);
	for (ti = 0; ti < TASK_MAX_INSTANCES; ti++) {
		if (__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST) == NULL) {
			__atomic_store_n(&instances[ti], INSTANCE_SLOT_BUSY, __ATOMIC_SEQ_CST);
			break;
		}
	}
	pthread_mutex_unlock(&creation_lock);

	if (ti >= TASK_MAX_INSTANCES) {
		errno = ENOSPC;
		pthread_mutexattr_destroy(&attr);
		return NULL;
	}

	size_t sz;
	for (sz = 1; sz < sizeof(struct instance); sz += sz);
	if ((i = aligned_alloc(sz, sz)) == NULL) {
		goto instance_creation_fail;
	}
	memset(i, 0, sz);

	// memset() leaves fd fields at zero, and zero is a valid descriptor.
	// Make them explicitly invalid before any fallible constructor step so a
	// later cleanup cannot close stdin/stdout/stderr.
	i->evfd = -1;

	i->magic = INSTANCE_MAGIC;
	i->curtime_us = get_time_us(TASK_TIME_PRECISE);
	i->worker_idle_empty_time_us = i->curtime_us;
	i->ti = ti;

	i->state = INSTANCE_STATE_CREATED;
	i->thr = pthread_self();


	pthread_mutex_init(&i->lock, &attr);
	pthread_mutex_init(&i->listeners_lock, &attr);
	pthread_mutex_init(&i->cpulock, &attr);

	TAILQ_INIT(&i->workers_created);
	TAILQ_INIT(&i->workers_running);
	TAILQ_INIT(&i->workers_blocking);
	TAILQ_INIT(&i->workers_idle);
	TAILQ_INIT(&i->workers_notify);
	TAILQ_INIT(&i->workers_shutdown);
	TAILQ_INIT(&i->workers_dead);

	if ((i->io_workers = (struct worker **)calloc(num_workers_io, sizeof(struct worker *))) == NULL) {
		goto instance_creation_fail;
	}

	i->num_workers_io = num_workers_io;	// May get modified downwards later
	i->num_blocking_workers = 0;
	i->num_blocking_idle = 0;

	if (instance_tfd_pool_init(i, max_tasks) < 0) {
		goto instance_creation_fail;
	}

	// CPU/Worker affinity setup
	i->num_cpus_seen = 0;
	if (num_workers_io > 1) {
		i->num_cpus = get_nprocs_conf();
		if (i->num_cpus > 0) {
			size_t cpus_size = (size_t)i->num_cpus * sizeof(struct cpuinfo);

			cpus_size = (cpus_size + 31) & ~(size_t)31;
			i->cpus = aligned_alloc(32, cpus_size);
			if (i->cpus != NULL) {
				int rows = (num_workers_io + i->num_cpus - 1) / i->num_cpus;

				memset(i->cpus, 0, cpus_size);
				i->num_cpu_rows = rows;
				for (int n = 0; n < i->num_cpus; n++) {
					i->cpus[n].workers = (struct worker **)calloc(rows, sizeof(struct worker *));
					if (i->cpus[n].workers == NULL) {
						goto instance_creation_fail;
					}
				}
			}
		}
	}

	i->max_blocking_workers = max_blocking_workers;

	if ((i->evfd = eventfd(0, EFD_CLOEXEC)) < 0) {
		goto instance_creation_fail;
	}

	pthread_mutex_lock(&creation_lock);
	__atomic_store_n(&instances[ti], i, __ATOMIC_SEQ_CST);
	__atomic_store_n(&task_api_slot_open[ti], 1, __ATOMIC_SEQ_CST);
	pthread_mutex_unlock(&creation_lock);
	pthread_mutexattr_destroy(&attr);

	return i;

instance_creation_fail:
	if (i) {
		instance_destroy(i);
	}
	pthread_mutex_lock(&creation_lock);
	if (__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST) == INSTANCE_SLOT_BUSY) {
		__atomic_store_n(&instances[ti], NULL, __ATOMIC_SEQ_CST);
	}
	pthread_mutex_unlock(&creation_lock);
	pthread_mutexattr_destroy(&attr);
	return NULL;
} // instance_create



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
	int result = -1;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = ERANGE;
		return -1;
	}

	if (!task_instance_ref((uint32_t)ti)) {
		errno = EBADF;
		return -1;
	}
	i = (struct instance *)__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);

	if ((i == NULL) || (i == INSTANCE_SLOT_BUSY) || (i->magic != INSTANCE_MAGIC)) {
		errno = EBADF;
		goto out;
	}

	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		goto out;
	}

	if (work_func == NULL) {
		errno = EINVAL;
		goto out;
	}

	// If we're not allowed to spawn blocking workers then we
	// have to do it ourselves.  This is NOT ideal at all!!
	if (i->max_blocking_workers == 0) {
		// Do not keep an instance reference across user work: the user code
		// must be able to call back into the instance without deadlocking a
		// concurrent shutdown drain.
		task_instance_unref((uint32_t)ti);
		work_func(work_data);
		if (work_cb_func) {
			work_cb_func(ti, work_cb_data);
		}
		return 0;
	}

	// We need to hold onto the instance lock while checking, so we cheat
	// a bit with worker states here, instead of using worker_set_state()
	instance_lock(i);

	task_atomic_store(&i->curtime_us, get_time_us(TASK_TIME_PRECISE));

	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_SHUTTING_DOWN) {
		instance_unlock(i);
		errno = EOWNERDEAD;
		goto out;
	}

	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_RUNNING) {
		do_start = 1;
	}

	if((w = TAILQ_FIRST(&i->workers_idle))) {
		// Publish the work before making the worker BLOCKING.  The worker
		// thread can observe the state change without reading its eventfd,
		// and must never see BLOCKING with stale work pointers.
		w->work_func = work_func;
		w->work_data = work_data;
		w->work_cb_func = work_cb_func;
		w->work_cb_data = work_cb_data;

		// Reserve the worker while keeping its state machine bookkeeping
		// consistent with the list it is actually on.  The worker thread
		// is blocked on read(evfd) and will see WORKER_STATE_BLOCKING
		// when the notification wakes it.
		if (i->num_blocking_idle > 0) {
			i->num_blocking_idle--;
		}
		TAILQ_REMOVE(&i->workers_idle, w, list);
		task_atomic_store(&w->old_state, (uint32_t)task_atomic_load(&w->state));
		task_atomic_store(&w->state, (uint32_t)(WORKER_STATE_BLOCKING));
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
			goto out;
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
			if (i->num_blocking_workers > 0) {
				i->num_blocking_workers--;
			}
			instance_unlock(i);

			errno = EAGAIN;
			goto out;
		}
	}

	// A newly created worker has no thread yet, so publishing the work here is
	// safe.  A reused idle worker was populated under the instance lock above.
	if (w->work_func == NULL) {
		w->work_func = work_func;
		w->work_data = work_data;
		w->work_cb_func = work_cb_func;
		w->work_cb_data = work_cb_data;
	}

	// If we don't start it now, it'll get started when TASK_instance_start() is called
	if (do_start) {
		if (worker_start(w) < 0) {
			errno = EAGAIN;
			goto out;
		}
	}

	result = 0;

out:
	task_instance_unref((uint32_t)ti);
	return result;
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
	void (*shutdown_cb)(intptr_t ti, void *data);
	void *shutdown_data;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = ERANGE;
		return -1;
	}

	// Destroying the instance from one of its own workers would wait for the
	// worker that is currently executing this call.
	if ((__thr_current_worker != NULL) && (__thr_current_worker->instance != NULL) &&
	    (__thr_current_worker->instance->ti == ti)) {
		errno = EDEADLK;
		return -1;
	}

	pthread_mutex_lock(&creation_lock);

	i = __atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);

	if ((i == NULL) || (i == INSTANCE_SLOT_BUSY)) {
		errno = EBADF;
		pthread_mutex_unlock(&creation_lock);
		return -1;
	}

	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		pthread_mutex_unlock(&creation_lock);
		return -1;
	}

	if ((i->state != INSTANCE_STATE_CREATED) &&
	    (i->state != INSTANCE_STATE_RUNNING) &&
	    (((instance_state_t)task_atomic_load(&i->state) != INSTANCE_STATE_SHUTTING_DOWN))) {
		errno = EBADF;
		pthread_mutex_unlock(&creation_lock);
		return -1;
	}

	// Save the shutdown notification before the instance is freed and reserve
	// the slot so no new API caller can find the instance.
	shutdown_cb = i->shutdown_cb;
	shutdown_data = i->shutdown_data;
	__atomic_store_n(&task_api_slot_open[ti], 0, __ATOMIC_SEQ_CST);
	__atomic_store_n(&instances[ti], INSTANCE_SLOT_BUSY, __ATOMIC_SEQ_CST);
	pthread_mutex_unlock(&creation_lock);

	instance_lock(i);
	__atomic_store_n(&i->state, INSTANCE_STATE_SHUTTING_DOWN, __ATOMIC_SEQ_CST);
	instance_unlock(i);

	// Drain entry references before scanning the pool.  Creation-style APIs
	// that already entered get to publish their task first.
	instance_wait_api_refs((uint32_t)ti);

	// Queue closes before stopping the workers so every close is processed by
	// the task's owning worker.  Deferred close is idempotent.
	instance_queue_shutdown(i);
	instance_shutdown_workers(i);
	instance_wait_workers(i);

	// All worker-held references are gone now.  Wait for any remaining public
	// API entry references before the instance and its worker state are freed.
	instance_wait_api_refs((uint32_t)ti);

	instance_reap_workers(i);
	instance_nuke_tasks(i);
	instance_destroy(i);

	pthread_mutex_lock(&creation_lock);
	if (__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST) == INSTANCE_SLOT_BUSY) {
		__atomic_store_n(&instances[ti], NULL, __ATOMIC_SEQ_CST);
	}
	pthread_mutex_unlock(&creation_lock);

	if (shutdown_cb) {
		shutdown_cb(ti, shutdown_data);
	}
	return 0;
} // TASK_instance_destroy



// Waits for instance to fully shutdown
int
TASK_instance_wait(int32_t ti)
{
	struct instance *i;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = ERANGE;
		return -1;
	}

	if ((__thr_current_worker != NULL) && (__thr_current_worker->instance != NULL) &&
	    (__thr_current_worker->instance->ti == ti)) {
		errno = EDEADLK;
		return -1;
	}

	i = __atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);

	if ((i == NULL) || (i == INSTANCE_SLOT_BUSY) || (i->magic != INSTANCE_MAGIC)) {
		errno = EBADF;
		return -1;
	}


	// Nothing has been started yet, so there is nothing to wait for
	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_CREATED) {
		return 0;
	}

	for (;;) {
		worker_handle_instance(i);

		if (i->evfd < 0) {
			if (poll(NULL, 0, 1000) < 0) {
				if (errno != EINTR) {
					break;
				}
			}
		} else {
			int len;
			uint64_t c;

			// Wait for something to happen! This is a blocking read
			if ((len = read(i->evfd, (void *)&c, sizeof(c))) < 0) {
				if ((errno == EINTR) || (errno == EAGAIN)) {
					continue;
				}
				/* Leave eventfd lifetime to instance_destroy(). */
				poll(NULL, 0, 1);
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
	instance_wait_api_refs((uint32_t)ti);
	instance_reap_workers(i);

	// If user wanted to be notified that we just shutdown.  Clear the
	// callback and data before invoking it so a reentrant lifecycle call
	// from the callback cannot see (and free) the same context twice.
	void (*shutdown_cb)(intptr_t, void *) = i->shutdown_cb;
	void *shutdown_data = i->shutdown_data;
	i->shutdown_cb = NULL;
	i->shutdown_data = NULL;
	if (shutdown_cb) {
		shutdown_cb(ti, shutdown_data);
	}

	return 0;
} // TASK_instance_wait



int
TASK_instance_shutdown(int32_t ti, void *shutdown_data, void (*shutdown_cb)(intptr_t ti, void *shutdown_data))
{
	struct instance *i;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = ERANGE;
		return -1;
	}

	i = __atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);

	if ((i == NULL) || (i == INSTANCE_SLOT_BUSY)) {
		errno = EBADF;
		return -1;
	}

	if (i->magic != INSTANCE_MAGIC) {
		errno = EBADF;
		return -1;
	}

	instance_lock(i);
	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_SHUTTING_DOWN) {
		instance_unlock(i);
		return 0;
	}

	i->shutdown_cb = shutdown_cb;
	i->shutdown_data = shutdown_data;
	__atomic_store_n(&i->state, INSTANCE_STATE_SHUTTING_DOWN, __ATOMIC_SEQ_CST);
	instance_unlock(i);

	// Stop admitting new entry references, then let every creation-style API
	// call that already entered publish its task before the shutdown scan
	// decides which tasks are live.
	__atomic_store_n(&task_api_slot_open[ti], 0, __ATOMIC_SEQ_CST);
	instance_wait_api_refs((uint32_t)ti);

	// Queue the closes first, then stop the workers.  worker_cleanup() will
	// process the queued closes on each task's owning thread.
	instance_queue_shutdown(i);
	instance_shutdown_workers(i);
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
		errno = ERANGE;
		return -1;
	}

	i = __atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST);

	if ((i == NULL) || (i == INSTANCE_SLOT_BUSY) || (i->magic != INSTANCE_MAGIC)) {
		errno = EBADF;
		return -1;
	}

	if (((instance_state_t)task_atomic_load(&i->state) == INSTANCE_STATE_SHUTTING_DOWN)) {
		errno = EOWNERDEAD;
		return -1;
	}


	// Start all workers waiting to start.  A failed pthread_create() used
	// to be silently ignored, leaving a partially running instance.  Report
	// it; TASK_instance_destroy() can still stop and reap whatever started.
	while ((w = TAILQ_FIRST(&i->workers_created))) {
		if (worker_start(w) < 0) {
			errno = ECHILD;
			return -1;
		}
	}

	// Assign the instance to a worker for instance notifications
	instance_lock(i);
	if (instance_assign_worker(i) < 0) {
		instance_unlock(i);
		errno = ECHILD;
		return -1;
	}
	w = (struct worker *)task_atomic_load(&i->instance_worker);
	task_atomic_store(&i->state, INSTANCE_STATE_RUNNING);
	instance_unlock(i);

	// Wake the newly assigned instance worker.  Notifications that arrived
	// before it was assigned only wrote i->evfd, which no worker monitors.
	instance_notify(i);
	return 0;
} // TASK_instance_start



int32_t
TASK_instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks, int tcp_sndbuf_size, uint64_t flags)
{
	struct instance *i = NULL;
	int num_io_to_spawn = 0;

	if ((num_workers_io < 0) || (max_blocking_workers < 0) ||
	    (max_tasks == 0) || (tcp_sndbuf_size < 0)) {
		errno = EINVAL;
		return -1;
	}

	pthread_mutex_lock(&creation_lock);
	if (!initialised) {
		struct sigaction sa[1];
		int32_t ti;

#ifdef DEBUG_SIZES
		printf("sizeof(tfd_lock_t)=%lu\n", sizeof(tfd_lock_t));
		printf("sizeof(struct task)=%lu\n", sizeof(struct task));
		printf("sizeof(struct task_stride)=%lu\n", sizeof(struct task_stride));
		printf("sizeof(socklen_t)=%lu\n", sizeof(socklen_t));
#endif

		__page_size = sysconf(_SC_PAGESIZE);
		memset(sa, 0, sizeof(struct sigaction));
		sa->sa_handler = SIG_IGN;
		if (sigaction(SIGPIPE, sa, NULL) < 0) {
			pthread_mutex_unlock(&creation_lock);
			return -1;
		}

		for(ti = 0; ti < TASK_MAX_INSTANCES; ti++) {
			instances[ti] = NULL;
		}
		initialised = true;
	}
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

	// instance_create() reserves and publishes a slot without holding
	// creation_lock across allocation or worker creation.
	if ((i = instance_create(num_io_to_spawn, max_blocking_workers, max_tasks)) == NULL) {
		errno = ENOMEM;
		return -1;
	}
	i->flags = flags;

	if (tcp_sndbuf_size == 0) {
		tcp_sndbuf_size = (1024 * 1024 * 1024) / max_tasks;
	}
	i->per_task_sndbuf = tcp_sndbuf_size;

	// Create our workers now
	i->num_workers_io = num_io_to_spawn;
	uint32_t nio = 0;
	for (int n = 0; n < num_io_to_spawn; n++) {
		struct worker *w;

		if ((w = worker_create(i, WORKER_TYPE_IO))) {
			i->io_workers[nio++] = w;
		}
	}
	i->num_workers_io = nio;

	if (i->num_workers_io == 0) {
		// The instance is published, so abort through the same teardown
		// path destroy uses, without holding creation_lock across waits.
		uint32_t ti = (uint32_t)i->ti;

		pthread_mutex_lock(&creation_lock);
		__atomic_store_n(&task_api_slot_open[ti], 0, __ATOMIC_SEQ_CST);
		__atomic_store_n(&instances[ti], INSTANCE_SLOT_BUSY, __ATOMIC_SEQ_CST);
		pthread_mutex_unlock(&creation_lock);

		instance_wait_api_refs(ti);
		instance_shutdown_workers(i);
		instance_wait_workers(i);
		instance_wait_api_refs(ti);
		instance_reap_workers(i);
		instance_nuke_tasks(i);
		instance_destroy(i);

		pthread_mutex_lock(&creation_lock);
		if (__atomic_load_n(&instances[ti], __ATOMIC_SEQ_CST) == INSTANCE_SLOT_BUSY) {
			__atomic_store_n(&instances[ti], NULL, __ATOMIC_SEQ_CST);
		}
		pthread_mutex_unlock(&creation_lock);
		errno = ENOMEM;
		return -1;
	}

	return i->ti;
} // TASK_instance_create

