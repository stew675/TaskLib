#include "task_internal.h"



void
worker_timer_attach_list(struct worker *w, struct task_timer *tt)
{
	// Now attach it (at the tail)
	if (w->colt != NULL) {
		tt->next = w->colt;
		tt->prev = w->colt->prev;
		tt->prev->next = tt;
		tt->next->prev = tt;
	} else {
		tt->next = tt;
		tt->prev = tt;
		w->colt = tt;
	}
	w->colt_len++;
} // worker_timer_attach_list



void
worker_timer_detach_list(struct worker *w, struct task_timer *tt)
{
	// Need to detach from colt first
	if (tt->next != tt) {
		tt->next->prev = tt->prev;
		tt->prev->next = tt->next;
		// If it's a head node, advance the head
		if (w->colt == tt) {
			w->colt = tt->next;
		}
	} else {
		if (w->colt == tt) {
			w->colt = NULL;
		}
	}
	tt->next = NULL;
	w->colt_len--;
} // worker_timer_detach_list



void
worker_timer_attach_heap(struct worker *w, struct task_timer *tt)
{
	// Okay then, it needs to go into the paired heap directly
	if (tt->node == NULL) {
		tt->node = pheap_insert(w->timer_queue, (void *)tt->expiry_us, (void *)tt);
		assert(tt->node != NULL);
	} else {
		pheap_set_key(w->timer_queue, tt->node, (void *)tt->expiry_us);
		pheap_attach_node(w->timer_queue, tt->node);
	}
} // worker_timer_attach_heap



void
worker_timer_detach_heap(struct worker *w, struct task_timer *tt)
{
	if(tt->node) {
		// Make sure it's detached from the heap
		pheap_detach_node(w->timer_queue, tt->node);
	}
} // worker_timer_detach_heap



void
worker_timer_update(struct worker *w, struct task_timer *tt)
{
	if (tt->expiry_us < 0) {
		// Nothing to do
		return;
	}

	if ((tt->expiry_us - w__curtime_us) < TASK_MIN_COLT_TIME_US) {
		// It should go to the paired heap
		if (tt->next != NULL) {
			worker_timer_detach_list(w, tt);
		}
		worker_timer_attach_heap(w, tt);
	} else {
		// It should go to the cool off list
		if (tt->next == NULL) {
			worker_timer_detach_heap(w, tt);
			worker_timer_attach_list(w, tt);
		}
	}
} // worker_timer_update



// I am both horrified, and yet morbidly proud of, the following function
struct task *
worker_get_task_from_tt(struct worker *w, struct task_timer *tt)
{
	size_t offset;

	// Hacky as all get out! It only works because all our task structures are pre-allocated
	// in a single contiguous block of memory, but it allows us to get by without needing an
	// explicit pointer to the task within the struct task_timer
	offset = ((char *)tt) - ((char *)w->instance->tfd_pool);
	offset /= sizeof(struct task);
	return w->instance->tfd_pool + offset;
} // worker_get_task_from_tt



// Scans 1/25th of the list at a time.  Since all new entries are added at the tail, and the list
// is circular, this algorithm just advances the colt pointer along the list.  Eventually we will
// just wrap and continue
void
worker_timer_scan_colt(register struct worker *w) {
	register struct task_timer *first, *colt, *tt;
	register size_t n, to_scan;

	first = NULL;
	colt = w->colt;
	w->colt = NULL;
	to_scan = (w->colt_len / 25) + 1;	// Always scan at least 1
	for (n = 0; n < to_scan; n++) {
		if ((tt = colt) == NULL) {
			break;
		}

		if (first == NULL) {
			first = tt;
		} else if (tt == first) {
			break;
		}

		if ((__atomic_load_n(&tt->expiry_us, __ATOMIC_RELAXED) - w__curtime_us) > TASK_MIN_COLT_TIME_US) {
			colt = colt->next;
			continue;
		}

		// It should go to the paired heap. Detach and insert
		if (first == tt) {
			first = NULL;
		}

		// Remove from the head
		if (tt != tt->next) {
			tt->next->prev = tt->prev;
			tt->prev->next = tt->next;
			colt = tt->next;
		} else {
			colt = NULL;
		}
		tt->next = NULL;
		w->colt_len--;
		worker_timer_attach_heap(w, tt);
	}
	assert(w->colt == NULL);
	w->colt = colt;

	// Scan every 40ms
	w->colt_next_us = w__curtime_us + 40000;
} // worker_timer_scan_colt



void
task_destroy_timeouts(struct task *t)
{
	struct worker *w = t->worker;

	if (w == NULL) {
		return;
	}

	// Destroy all timeouts
	__atomic_store_n(&t->tm_tt.expiry_us, TIMER_TIME_DESTROY, __ATOMIC_RELAXED);
	t->tm_cancelled = true;
	if (t->tm_tt.next) {
		worker_timer_detach_list(w, &t->tm_tt);
	}
	if (t->tm_tt.node) {
		pheap_release_node(w->timer_queue, t->tm_tt.node);
		t->tm_tt.node = NULL;
	}

	t->wr_tt.expiry_us = TIMER_TIME_DESTROY;
	t->wt_cancelled = true;
	if (t->wr_tt.next) {
		worker_timer_detach_list(w, &t->wr_tt);
	}
	if (t->wr_tt.node) {
		pheap_release_node(w->timer_queue, t->wr_tt.node);
		t->wr_tt.node = NULL;
	}

	t->rd_tt.expiry_us = TIMER_TIME_DESTROY;
	t->rt_cancelled = true;
	if (t->rd_tt.next) {
		worker_timer_detach_list(w, &t->rd_tt);
	}
	if (t->rd_tt.node) {
		pheap_release_node(w->timer_queue, t->rd_tt.node);
		t->rd_tt.node = NULL;
	}
} // task_destroy_timeouts



// Calculate the expiry timeout for a socket operation
int64_t
task_calculate_io_timeout(register int64_t us_from_now)
{
	if (us_from_now < 5000000) {
		if (us_from_now < 0) {
			return TIMER_TIME_CANCEL;
		}
		return (get_time_us(TASK_TIME_PRECISE) + us_from_now);
	}
	if (us_from_now > TASK_TIMEOUT_ONE_YEAR) {
		return w__curtime_us + TASK_TIMEOUT_ONE_YEAR;
	}
	// Set to the current worker time plus the us_from_now plus half
	// the worker's maximum epoll timeout.  It'll be close enough
	return (w__curtime_us + us_from_now + (TASK_MAX_EPOLL_WAIT_MS * 500));
} // task_calculate_io_timeout



void
task_activate_rd_timeout(struct worker *w, struct task *t)
{
	t->rd_expires_modified = false;
	if (t->rd_expires_in_us < 0) {
		t->rt_cancelled = true;
		return;
	}
	t->rd_tt.expiry_us = task_calculate_io_timeout(t->rd_expires_in_us);
	t->rd_expires_in_us = TIMER_TIME_DESTROY;

	if (w__curtime_us > t->rd_tt.expiry_us) {
		// It's already expired.  Handle that now
		task_do_timeout_cb(t, FLG_RT, t->rd_tt.expiry_us);
		return;
	}

	worker_timer_update(w, &t->rd_tt);
	t->rt_cancelled = false;
} // task_activate_rd_timeout



void
task_activate_wr_timeout(struct worker *w, struct task *t)
{
	t->wr_expires_modified = false;
	if (t->wr_expires_in_us < 0) {
		t->wt_cancelled = true;
		return;
	}
	t->wr_tt.expiry_us = task_calculate_io_timeout(t->wr_expires_in_us);
	t->wr_expires_in_us = TIMER_TIME_DESTROY;

	if (w__curtime_us > t->wr_tt.expiry_us) {
		// It's already expired.  Handle that now
		task_do_timeout_cb(t, FLG_WT, t->wr_tt.expiry_us);
		return;
	}

	worker_timer_update(w, &t->wr_tt);
	t->wt_cancelled = false;
} // task_activate_wr_timeout



void
task_do_timeout_cb(struct task *t, task_action_flag_t action, int64_t timeout_us)
{
	int64_t tfd = t->tfd;
	void (*tm_cb)(int64_t tfd, int64_t lateness_us, void *tm_cb_data);
	void *tm_cb_data;
	uint32_t tfdi;

	switch (action) {
	case	FLG_TM:
		tfdi = TFD_TO_INDEX(tfd);
		tfd_lock(t, tfdi);
		t->tm_cancelled = 1;
		tm_cb = t->tm_cb;
		tm_cb_data = t->tm_cb_data;
		tfd_unlock(t, tfdi);

		if (tm_cb != NULL) {
			int64_t lateness_us = get_time_us(TASK_TIME_PRECISE) - timeout_us;

			errno = ETIMEDOUT;
			tm_cb(tfd, lateness_us, tm_cb_data);
			errno = 0;
			return;
		}
		// A timer without a callback just stays allocated until closed.
		return;

	case	FLG_RT:
		if (task_atomic_load(&t->rd_state) == TASK_READ_STATE_BUFFER) {
			task_do_read_cb(t, tfd, -1, ETIMEDOUT);
		} else if (task_atomic_load(&t->rd_state) == TASK_READ_STATE_VECTOR) {
			task_do_readv_cb(t, tfd, -1, ETIMEDOUT);
		}
		return;

	case	FLG_WT:
		if (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_BUFFER) {
			task_do_write_cb(t, tfd, -1, ETIMEDOUT);
		} else if (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_CONNECT) {
			task_do_connect_cb(t, tfd, -1, ETIMEDOUT);
		} else if (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_VECTOR) {
			task_do_writev_cb(t, tfd, -1, ETIMEDOUT);
		}
		return;

	default:
		break;
	}

	assert(0);
} // task_do_timeout_cb



void
task_update_timer(struct task *t)
{
	struct worker *w = t->worker;
	uint32_t tfdi = TFD_TO_INDEX(t->tfd);
	int64_t expiry_us;

	tfd_lock(t, tfdi);

	// The task may have been closed while the FLG_TM action was queued.
	if (task_atomic_load(&t->state) != TASK_STATE_ACTIVE) {
		tfd_unlock(t, tfdi);
		return;
	}

	expiry_us = __atomic_load_n(&t->tm_tt.expiry_us, __ATOMIC_RELAXED);
	if (expiry_us < 0) {
		t->tm_cancelled = 1;
		if (t->tm_tt.next) {
			worker_timer_detach_list(w, &t->tm_tt);
		}
		if (t->tm_tt.node) {
			worker_timer_detach_heap(w, &t->tm_tt);
		}
		tfd_unlock(t, tfdi);
		return;
	}

	if (w__curtime_us > expiry_us) {
		tfd_unlock(t, tfdi);
		task_do_timeout_cb(t, FLG_TM, expiry_us);
		return;
	}

	t->tm_cancelled = 0;
	worker_timer_update(w, &t->tm_tt);
	tfd_unlock(t, tfdi);
} // task_update_timer



// Process anything that has expired on the worker's timeout queue
void
worker_check_timeouts(struct worker *w)
{
	// Update the worker time
	w__curtime_us = get_time_us(TASK_TIME_PRECISE);

	// Pickup new timer heap entries from the timer cool-off lists as needed
	if (w__curtime_us > w->colt_next_us) {
		worker_timer_scan_colt(w);
	}

	while(true) {
		void *data, *timer_node;
		int64_t expiry_us;

		timer_node = pheap_get_min_node(w->timer_queue, (void **)&expiry_us, (void **)&data);
		if (timer_node == NULL) {
			w->num_events = w->max_events;
			break;
		}

		if (expiry_us > w__curtime_us) {
			register int64_t num_events;

			// XXX - These 250/5 values are total fudge values, but "work"
			num_events = ((expiry_us - w__curtime_us) / 250);
			if (num_events < 5) {
				num_events = 5;
			} else if (num_events > w->max_events) {
				num_events = w->max_events;
			}
			w->num_events = num_events;
			break;
		}

		// Okay, someone has expired.  The game is afoot Watson!

		// First detach the timer_node
		pheap_detach_node(w->timer_queue, timer_node);

		// Determine which timeout fired
		register struct task_timer *tt = (struct task_timer *)data;
		register struct task *t = worker_get_task_from_tt(w, tt);
		uint32_t tfdi = TFD_TO_INDEX(t->tfd);
		task_action_flag_t action;
		bool cancelled;

		tfd_lock(t, tfdi);
		if (task_atomic_load(&t->state) != TASK_STATE_ACTIVE) {
			tfd_unlock(t, tfdi);
			continue;
		}

		// The expiry may have been changed (re-armed) or cancelled while the
		// node was queued.  If it no longer matches the key that was popped,
		// drop the stale node; the pending FLG_TM will re-attach it.
		if (__atomic_load_n(&tt->expiry_us, __ATOMIC_RELAXED) != expiry_us) {
			tfd_unlock(t, tfdi);
			continue;
		}

		if (&t->tm_tt == tt) {
			cancelled = t->tm_cancelled;
			action = FLG_TM;
		} else if (&t->rd_tt == tt) {
			cancelled = t->rt_cancelled;
			action = FLG_RT;
		} else if (&t->wr_tt == tt) {
			cancelled = t->wt_cancelled;
			action = FLG_WT;
		} else {
			tfd_unlock(t, tfdi);
			assert(0);
			continue;
		}

		if (cancelled) {
			tfd_unlock(t, tfdi);
			continue;
		}

		switch (action) {
		case FLG_TM:
			t->tm_cancelled = 1;
			break;
		case FLG_RT:
			t->rt_cancelled = 1;
			break;
		case FLG_WT:
			t->wt_cancelled = 1;
			break;
		default:
			break;
		}
		tfd_unlock(t, tfdi);

		task_do_timeout_cb(t, action, expiry_us);
	}
} // worker_check_timeouts



// ---------------------------------------------------------------------------------------------//
//			EXTERNALLY VISABLE API CALLS BELOW THIS POINT				//
// ---------------------------------------------------------------------------------------------//

// ---------------------------------------------------------------------------------------------//
// 				Task Library Timeouts API					//
// ---------------------------------------------------------------------------------------------//

int64_t
TASK_get_us_time(int64_t *time_us, bool precise)
{
	int64_t now_us;
	if (precise) {
		now_us = get_time_us(TASK_TIME_PRECISE);
	} else {
		now_us = get_time_us(TASK_TIME_COARSE);
	}
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

	t->tm_cancelled = 1;
	__atomic_store_n(&t->tm_tt.expiry_us, TIMER_TIME_DESTROY, __ATOMIC_RELAXED);

	bool queued;
	if (t->type == TASK_TYPE_TIMER) {
		// Standalone timer tasks have no fd or other owner, so destroy the
		// task itself.  For socket-backed tasks this behaves as a cancel.
		task_atomic_store(&t->state, TASK_STATE_DESTROY);
		task_atomic_store(&t->closed, 0);
		queued = task_notify_action(t, FLG_CL, true, true);
		task_unlock(t, FLG_TD);
		if (!queued) {
			if (!lockless_worker(task_atomic_load(&t->worker))) {
				errno = ENOMEM;
				return -1;
			}
			task_lock(t, FLG_CL);
			task_do_close_cb(t);
		}
	} else {
		queued = task_notify_action(t, FLG_TM, false, true);
		task_unlock(t, FLG_TD);
		if (!queued) {
			errno = EOWNERDEAD;
			return -1;
		}
	}
	return 0;
} // TASK_timeout_destroy



// Cancels the timeout callback against the given tfd
int
TASK_timeout_cancel(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_TC);

	if (t == NULL) return -1;	// errno already set

	// task_update_timer() decides based on the expiry value, so a bare
	// tm_cancelled flag would just be cleared when the FLG_TM action runs
	t->tm_cancelled = 1;
	__atomic_store_n(&t->tm_tt.expiry_us, TIMER_TIME_CANCEL, __ATOMIC_RELAXED);
	bool queued = task_notify_action(t, FLG_TM, false, true);
	task_unlock(t, FLG_TC);
	if (!queued) {
		errno = EOWNERDEAD;
		return -1;
	}
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

	us_from_now = (us_from_now < 0) ? 0 : us_from_now;
	us_from_now = (us_from_now > TASK_TIMEOUT_ONE_YEAR) ? TASK_TIMEOUT_ONE_YEAR : us_from_now;
	__atomic_store_n(&t->tm_tt.expiry_us, get_time_us(TASK_TIME_PRECISE) + us_from_now, __ATOMIC_RELAXED);
	t->tm_cb = timeout_cb;
	t->tm_cb_data = timeout_cb_data;
	t->tm_cancelled = 0;
	bool queued = task_notify_action(t, FLG_TM, false, true);
	task_unlock(t, FLG_TS);
	if (!queued) {
		errno = EOWNERDEAD;
		return -1;
	}
	return 0;
} // TASK_timeout_set



// Creates an arbitrary timeout task that exists independently of any socket
// The task is assigned to the current worker context automatically, or if
// this is not known, then to a random IO worker.
// The time from now cannot be negative or more than one year into the future
int64_t
TASK_timeout_create(int32_t ti, int64_t us_from_now, void *timeout_cb_data,
		   void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data))
{
	struct instance *i;
	struct task *t;

	if ((ti < 0) || (ti >= TASK_MAX_INSTANCES)) {
		errno = ERANGE;
		return -1;
	}

	i = instances[ti];

	if ((i == NULL) || i->magic != INSTANCE_MAGIC) {
		errno = EINVAL;
		return -1;
	}


	if (((instance_state_t)task_atomic_load(&i->state) == INSTANCE_STATE_SHUTTING_DOWN)) {
		errno = EOWNERDEAD;
		return -1;
	}

	if ((t = task_create(i, TASK_TYPE_TIMER, -1, NULL, NULL, NULL, false)) == NULL) {
		return -1;
	}

	us_from_now = (us_from_now < 0) ? 0 : us_from_now;
	us_from_now = (us_from_now > TASK_TIMEOUT_ONE_YEAR) ? TASK_TIMEOUT_ONE_YEAR : us_from_now;
	__atomic_store_n(&t->tm_tt.expiry_us, get_time_us(TASK_TIME_PRECISE) + us_from_now, __ATOMIC_RELAXED);
	t->tm_cb = timeout_cb;
	t->tm_cb_data = timeout_cb_data;
	t->tm_cancelled = 0;

	if (!task_notify_action(t, FLG_TM, false, true)) {
		task_atomic_store(&t->state, TASK_STATE_DESTROY);
		task_atomic_store(&t->closed, 0);
		if (!task_queue_close(t)) {
			errno = ENOMEM;
			return -1;
		}
		errno = EOWNERDEAD;
		return -1;
	}
	return task_atomic_load(&t->tfd);
} // TASK_timeout_create


