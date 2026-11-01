#include "task_internal.h"



// Check for any blocking thread notifications and make the appropriate
// callbacks and then move the blocking thread back to the idle state
void
worker_handle_instance(struct instance *i)
{
	// Process blocking worker callbacks
	task_atomic_store(&i->curtime_us, get_time_us(TASK_TIME_COARSE));
	{
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
				work_cb_func(i->ti, work_cb_data);
				instance_lock(i);
			}

			// The worker was temporarily off all lists while its callback
			// ran.  If shutdown started during that window it would have
			// been missed by instance_shutdown_workers(), so put it
			// straight onto the shutdown list instead of idling it.
			if (((instance_state_t)task_atomic_load(&i->state) == INSTANCE_STATE_SHUTTING_DOWN)) {
				w->old_state = w->state;
				task_atomic_store(&w->state, (uint32_t)(WORKER_STATE_SHUTTING_DOWN));
				TAILQ_INSERT_TAIL(&i->workers_shutdown, w, list);
				worker_notify(w);
			} else {
				// Place the worker on the idle queue, but don't
				// notify it. It doesn't have anything to do yet.
				// Keep old_state/counters consistent with the list move.
				if (TAILQ_EMPTY(&i->workers_idle)) {
					i->worker_idle_empty_reaped = 0;
					i->worker_idle_empty_time_us = get_time_us(TASK_TIME_PRECISE);
				}
				w->old_state = w->state;
				task_atomic_store(&w->state, (uint32_t)(WORKER_STATE_IDLE));
				i->num_blocking_idle++;
				TAILQ_INSERT_TAIL(&i->workers_idle, w, list);
			}
		}
		instance_unlock(i);
	}

	// Start reaping workers from the idle queue if it hasn't been
	// empty for 5 minutes. After that, reap 1 thread every 5s.  All counter
	// reads and writes happen under the instance lock because
	// TASK_instance_wait() may call this concurrently with the instance IO
	// worker.
	for (;;) {
		struct worker *w = NULL;
		int64_t idle_reap_expiry_us;
		int64_t now_us;

		instance_lock(i);
		if (TAILQ_EMPTY(&i->workers_idle)) {
			i->worker_idle_empty_reaped = 0;
			i->worker_idle_empty_time_us = task_atomic_load(&i->curtime_us);
			instance_unlock(i);
			break;
		}

		if (i->worker_idle_empty_time_us == 0) {
			i->worker_idle_empty_time_us = task_atomic_load(&i->curtime_us);
		}
		idle_reap_expiry_us = i->worker_idle_empty_time_us;
		idle_reap_expiry_us += TASK_S_TO_US(300);
		idle_reap_expiry_us += (i->worker_idle_empty_reaped * TASK_S_TO_US(5));
		now_us = task_atomic_load(&i->curtime_us);
		if (now_us < idle_reap_expiry_us) {
			instance_unlock(i);
			break;
		}

		if ((w = TAILQ_FIRST(&i->workers_idle))) {
			w->old_state = w->state;
			task_atomic_store(&w->state, (uint32_t)(WORKER_STATE_SHUTTING_DOWN));
			i->num_blocking_idle--;
			TAILQ_REMOVE(&i->workers_idle, w, list);
			TAILQ_INSERT_TAIL(&i->workers_shutdown, w, list);
		}
		i->worker_idle_empty_reaped++;
		instance_unlock(i);

		if (w) {
			worker_notify(w);
		}
	}
} // worker_handle_instance



// We're being notified that something happened.  Typically this would be new tasks
// having being added. Read off the notification event.
void
worker_handle_event(struct worker *w, uint32_t events)
{
	uint64_t c;

	if (w->evfd < 0) {
		return;
	}

	if (events & (EPOLLERR | EPOLLHUP)) {
		task_atomic_store(&w->notified, 0);
		/* Do not close the eventfd here.  Foreign notifiers may still be
		 * writing to it, and fd numbers can be reused.  worker_destroy()
		 * closes it only after the worker thread has exited. */
		worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
		return;
	}

	// Pull off the event notification from the input queue
	while (true) {
		ssize_t len;

		len = read(w->evfd, (void *)&c, sizeof(c));
		if (len < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EAGAIN) {
				task_atomic_store(&w->notified, 0);
				break;
			}
			// Serious read error.  Leave the eventfd alone; it is owned by
			// worker_destroy() after the thread has stopped.
			task_atomic_store(&w->notified, 0);
			worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
			break;
		}

		// We picked up the notification
		task_atomic_store(&w->notified, 0);
		break;
	}
} // worker_handle_event



bool
worker_handle_task_migration(struct worker *w, struct task *t)
{
	struct instance *i = w->instance;
	struct worker *tw = t->stride->preferred_worker;
	struct ntfyq *tq;

	// Reserve the completion notification before committing ownership.  If
	// this fails, abort the migration with the task still entirely owned by
	// this worker rather than leaving timers detached on an unreachable
	// target.
	if ((tq = worker_notify_reserve_locked(tw)) == NULL) {
		t->stride->preferred_worker = NULL;
		task_action_done(t, FLG_MG);
		return false;
	}

	t->migrations++;
	t->stride->preferred_worker = NULL;

	// Decouple any timeout from this worker *before* publishing the new
	// owner.  Otherwise a concurrent timer update or close can be routed to
	// the target worker while the node is still linked into this worker's
	// paired heap.
	if (t->tm_tt.next) {
		worker_timer_detach_list(w, &t->tm_tt);
	}
	if (t->rd_tt.next) {
		worker_timer_detach_list(w, &t->rd_tt);
	}
	if (t->wr_tt.next) {
		worker_timer_detach_list(w, &t->wr_tt);
	}
	worker_timer_detach_heap(w, &t->tm_tt);
	worker_timer_detach_heap(w, &t->rd_tt);
	worker_timer_detach_heap(w, &t->wr_tt);

	// Cancel any epoll_wait on this worker, and move to new
#ifdef USE_EPOLLET
	t->events &= ~(EPOLLET);		// Don't do an ADD with EPOLLET set
#endif
	// Always update epfd, even if the task is not currently armed.  Otherwise
	// a later task_raise_event_flag() would arm the fd in the old worker's
	// epoll set while t->worker points at the new worker.
	if (task_atomic_load(&t->fd) >= 0) {
		int was_in_epoll = task_atomic_load(&t->in_epoll);
		struct epoll_event ev = { .data.u64 = (uint64_t)task_atomic_load(&t->tfd), .events = t->events };

		if (was_in_epoll) {
			epoll_ctl(t->epfd, EPOLL_CTL_DEL, task_atomic_load(&t->fd), NULL);
			task_atomic_store(&t->in_epoll, 0);
		}
		t->epfd = tw->gepfd;
		if (was_in_epoll && (epoll_ctl(t->epfd, EPOLL_CTL_ADD, task_atomic_load(&t->fd), &ev) == 0)) {
			task_atomic_store(&t->in_epoll, 1);
#ifdef USE_EPOLLET
			t->events |= EPOLLET;	// Set EPOLLET in the task ev flags now.
#endif
		}
	}

	// Move a listener's list entry to the new worker.  The list is shared
	// with poll() and with child-listener creation, so serialise it with the
	// per-instance listeners lock.
	if (task_atomic_load(&t->type) == TASK_TYPE_LISTEN) {
		pthread_mutex_lock(&i->listeners_lock);
		task_remove_list(&w->listeners, t);
		t->task_next = tw->listeners;
		tw->listeners = t;
		pthread_mutex_unlock(&i->listeners_lock);
	}

	// Publish the new owner only after every piece of old-worker-owned state
	// has been detached.  Queued notifications still sitting on the old
	// worker are forwarded by worker_process_one_action() once they run.
	task_atomic_store(&t->worker, tw);
	__atomic_sub_fetch(&w->num_tasks, 1, __ATOMIC_RELAXED);
	__atomic_add_fetch(&tw->num_tasks, 1, __ATOMIC_RELAXED);

	// All queued notifications automatically get forwarded to the new task worker

	if ((tw->affined_cpu >= 0) && (i->flags & TASK_FLAGS_AFFINITY_NET)) {
		// Doesn't matter if this fails.  It's only a hint to the TCP stack
		(void)setsockopt(task_atomic_load(&t->fd), SOL_SOCKET, SO_INCOMING_CPU,
				 &tw->affined_cpu, sizeof(tw->affined_cpu));
	}

	// Wake up the target worker with the receiver side of the migration.
	// FLG_MG stays claimed until the receiver has run, so a second
	// TASK_migrate() cannot overwrite preferred_worker in the meantime.
	worker_notify_queue_locked(tw, tq, t, FLG_MG);
	return true;
} // worker_handle_task_migration



void
worker_process_one_action(register struct worker *w, register struct task *t, register task_action_flag_t action)
{
	// If this worker does not match the task's worker, then that's probably because
	// the task recently migrated.  Send the notification to the correct worker
	if(t->worker != w) {
		if (action == FLG_CL) {
			// Close is deferred to the current owner's close queue.
			(void)task_close_forward(t);
			return;
		}
		// Forward the action directly to the correct worker.  Pass the
		// force_close_q flag to allow FLG_CL actions through even when
		// the task is in DESTROY state
		if (task_notify_action(t, action, true, true) == false) {
			// It didn't get forwarded.  Drop only the notification
			// reference; never run close on a task this worker no longer
			// owns (the final shutdown nuke will reap it).
			if ((action & (FLG_CN | FLG_WR | FLG_RD | FLG_WT | FLG_RT)) == 0) {
				task_action_done(t, action);
			}
		}
		return;
	}

	// Check if the action got cancelled.  If so, just ignore it
	// Allow close, connecting and io timeouts through though
	if ((action & (FLG_CL | FLG_RD | FLG_WR | FLG_CN | FLG_WT | FLG_RT | FLG_TM)) == 0) {
		if ((task_atomic_load(&t->active_flags) & action) == 0) {
			return;
		}
	}

	// It's a change action from here on.  Process the action we got

	// IO actions are usually the most common. Test for them first
	if (action & (FLG_RD | FLG_WR)) {
		if (action == FLG_WR) {
			if (task_atomic_load(&t->wr_state) != TASK_WRITE_STATE_IDLE) {
				task_handle_wr_event(t);	// Releases the task lock
			}
		} else {
			if (task_atomic_load(&t->rd_state) != TASK_READ_STATE_IDLE) {
				task_handle_rd_event(t);	// Releases the task lock
			}
		}
		return;
	}

	if (action & (FLG_RT | FLG_WT)) {
		if (action == FLG_WT) {
			task_activate_wr_timeout(w, t);
		} else {
			task_activate_rd_timeout(w, t);
		}
		return;
	}

	if (action == FLG_CN) {
		task_handle_connecting_action(t, t->wr_expires_modified);
		return;
	}

	if (action == FLG_CL) {
		// Deferred close: link the pinned task onto this worker's close
		// queue.  It will be torn down after the current batch completes.
		(void)task_close_forward(t);
		return;
	}

	if (action == FLG_TM) {
		task_update_timer(t);			// Releases the task lock
		return;
	}

	// Handle a migration notification
	if (action == FLG_MG) {
		if (t->stride->preferred_worker != NULL) {
			// We're the sender worker.  On success the receiver owns the
			// FLG_MG claim and will release it once its timers are attached.
			if (t->stride->preferred_worker != w) {
				(void)worker_handle_task_migration(w, t);
			} else {
				// Target changed back to this worker.  Nothing to move.
				t->stride->preferred_worker = NULL;
				task_action_done(t, FLG_MG);
			}
			return;
		}

		// We're the receiver worker.  Re-attach any timer nodes as needed.
		// Releasing FLG_MG last allows the next migration to start.
		worker_timer_update(w, &t->tm_tt);
		worker_timer_update(w, &t->rd_tt);
		worker_timer_update(w, &t->wr_tt);
		task_action_done(t, FLG_MG);
		return;
	}

	// Bad action type
	assert(0);
} // worker_process_one_action



void
worker_process_notifyq(register struct worker *w)
{
#ifdef TFD_POOL_DEBUG
	// Update straggler debug list
	if (w == (struct worker *)task_atomic_load(&w->instance->instance_worker)) {
		register uint32_t n, k;
		register struct instance *i = w->instance;

		for (n = 0; n < NUM_STRAGGLERS; n++) i->stragglers[n] = UINT32_MAX;
		for (n = 0, k = 0; k < i->tfd_pool_size; k++) {
			if (i->tfd_pool[k].type == TASK_TYPE_IO) {
				i->stragglers[n++] = k;
				if (n == NUM_STRAGGLERS)
					break;
			}
		}
	}
#endif

	// Process nothing if we're shutting down
	// Everything will get cleaned up when the worker shuts down
	if (((instance_state_t)task_atomic_load(&w->instance->state) > INSTANCE_STATE_RUNNING) ||
	    ((worker_state_t)task_atomic_load(&w->state) > WORKER_STATE_RUNNING)) {
		return;
	}

	for (uint32_t locked = 0; locked < 2; locked++) {
		register struct ntfyq *tq = NULL, *notifyq = NULL;
		struct ntfyq *lockedq = NULL;


		// Check the notifyq lists
		if (locked) {
			worker_lock(w);
			notifyq = w->notifyq_locked_head;
			if (notifyq != NULL) {
				w->notifyq_locked_head = NULL;
				w->notifyq_locked_tail = NULL;
			}
			worker_unlock(w);
			if (notifyq == NULL) {
				continue;
			}
			lockedq = notifyq;
		} else {
			if ((notifyq = w->notifyq_head) == NULL) {
				continue;
			}
			w->notifyq_head = NULL;
			w->notifyq_tail = NULL;
		}

		w__curtime_us = get_time_us(TASK_TIME_PRECISE);

		while ((tq = notifyq)) {
			register struct task *t = tq->t;
			register task_action_flag_t action = tq->action;

			notifyq = tq->next;
			if (!locked) {
				// Return the node to the worker's own free list
				tq->next = w->freeq;
				w->freeq = tq;
			}

			// Flush legacy close notifications into the allocation-free
			// deferred close queue.  Everything else follows the normal
			// action path.
			if (action == FLG_CL) {
				(void)task_close_forward(t);
			} else if (task_atomic_load(&t->state) != TASK_STATE_ACTIVE) {
				task_action_done(t, action);
			} else {
				worker_process_one_action(w, t, action);
			}

			// Drop the notification reference only after the action has
			// been processed, keeping the task alive across the callback.
			if (locked) {
				task_atomic_dec(&t->notifyqlen_locked);
			} else {
				task_atomic_dec(&t->notifyqlen);
			}
			if (task_atomic_load(&t->state) == TASK_STATE_DESTROY) {
				task_maybe_free(t);
			}
		}

		// Concat any remainder back to the actual lists. Put the
		// just processed entries at the front of the freeq
		if (locked) {
			worker_lock(w);
			while ((tq = lockedq) != NULL) {
				lockedq = tq->next;
				tq->next = w->freeq_locked;
				w->freeq_locked = tq;
			}
			worker_unlock(w);
		}
	}
} // worker_process_notifyq



void
worker_poll_listeners(register struct worker *w)
{
	struct instance *i = w->instance;
	register struct task *t;
	register int numfds;

	// Take a snapshot of the listener list and pin every entry.  A listener
	// callback may close (and therefore remove/recycle) itself, so the list
	// must not be walked while callbacks run and the snapshot must keep each
	// task alive until its callback returns.
	pthread_mutex_lock(&i->listeners_lock);
	for (numfds = 0, t = w->listeners; (numfds < w->max_pollfds) && (t != NULL); numfds++, t = t->task_next) {
		w->poll_tasks[numfds] = t;
		task_ref(t);
	}
	pthread_mutex_unlock(&i->listeners_lock);

	for (int n = 0; n < numfds; n++) {
		w->pollfds[n].fd = task_atomic_load(&w->poll_tasks[n]->fd);
		w->pollfds[n].events = POLLIN;
		w->pollfds[n].revents = 0;
	}

	if (poll(w->pollfds, numfds, 0) <= 0) {
		for (int n = 0; n < numfds; n++) {
			task_unref(w->poll_tasks[n]);
		}
		return;
	}

	for (int n = 0; n < numfds; n++) {
		if (w->pollfds[n].revents & POLLIN) {
			task_handle_listen_event(w->poll_tasks[n]);
		}
		task_unref(w->poll_tasks[n]);
	}
} // worker_poll_listeners



// Determine the maximum time to wait in epoll_wait()
int
get_next_epoll_timeout_ms(struct worker *w)
{
	int64_t time_to_wait_us = 0, expiry_us = 0;
	int time_to_wait_ms = TASK_MAX_EPOLL_WAIT_MS;
	void *timer_node;

	worker_check_timeouts(w);	// Updates the worker time for us

	// If we have tasks in the notifyq's or the deferred-close queue to
	// process, don't wait in epoll()
	if (task_atomic_load(&w->close_head) != TASK_CLOSE_EMPTY) {
		return 0;
	}
	if (w->notifyq_head != NULL) {
		return 0;
	}
	if (w->notifyq_locked_head != NULL) {
		return 0;
	}

	// Grab the first item on the timer queue.  The time until it expires is the
	// time that we will wait for.  We won't wait for longer than TASK_MAX_EPOLL_WAIT_MS.
	// The reason why we might want to break out of epoll_wait() early is to keep
	// the worker state "warm" in the CPU caches
	timer_node = (void *)pheap_get_min_node(w->timer_queue, (void **)&expiry_us, NULL);

	if(timer_node) {
		// The 333 and 667 below respectively represent 1/3 and
		// 2/3 of a millisecond, expressed in microseconds
		time_to_wait_us = expiry_us - w__curtime_us;
		if (time_to_wait_us <= 333) {
			time_to_wait_ms = 0;
		} else if (TASK_US_TO_MS(time_to_wait_us - 667) > TASK_MAX_EPOLL_WAIT_MS) {
			time_to_wait_ms = TASK_MAX_EPOLL_WAIT_MS;
		} else {
			time_to_wait_ms = (int)TASK_US_TO_MS(time_to_wait_us - 333);
			if ((time_to_wait_ms == 0) && (time_to_wait_us >= 667)) {
				time_to_wait_ms = 1;
			}
		}
	}

	return time_to_wait_ms;
} // get_next_epoll_timeout_ms



void
worker_handle_task_event(struct task *t, uint32_t revents)
{
	bool handled = false;

	// Handle ERROR/HUP events first
	if (revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
		// ERROR/HUP means the connection is finished, but the peer may have
		// queued data before the error or FIN.  Never pre-set rd_shut: let
		// the read path drain data and observe EOF/error from the syscall.
		if (revents & (EPOLLERR | EPOLLHUP)) {
worker_do_io_epoll_fail:
			// The write side must observe the failure immediately.  The
			// read side is deliberately left open until read() returns 0
			// or an error.
			task_atomic_store(&t->wr_shut, 1);
			task_atomic_store(&t->in_epoll, 0);
			epoll_ctl(t->epfd, EPOLL_CTL_DEL, t->fd, NULL);
			task_atomic_store(&t->events, 0);
			if (task_atomic_load(&t->rd_state) != TASK_READ_STATE_IDLE) {
				handled = true;
				task_handle_rd_event(t);
			}
			// Signal the write side too.  A pending writer must not wait
			// for its timeout when the read side handled the error first.
			if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) &&
			    (task_atomic_load(&t->wr_state) != TASK_WRITE_STATE_IDLE)) {
				handled = true;
				task_handle_wr_event(t);
			}
			// If nobody was waiting, leave the task active.  A later read
			// must still be allowed to drain data that arrived with the
			// FIN/error; the user owns the eventual close.
			(void)handled;
			return;
		}

		// Must be a RDHUP then.  Lower the event flags and drive the read
		// path, which will see the queued bytes and then EOF.  Do not set
		// rd_shut from the event hint.
		if (task_atomic_load(&t->rd_state) != TASK_READ_STATE_IDLE) {
			task_lower_event_flag(t, EPOLLIN | EPOLLRDHUP);
			task_handle_rd_event(t);
			return;
		}
		task_lower_event_flag(t, EPOLLIN | EPOLLRDHUP);
		revents &= ~(EPOLLRDHUP | EPOLLIN);
		if (revents == 0) {
			return;
		}
	}

	// Nice normal events.  Yay!
	if (revents & EPOLLIN) {
		if (revents & EPOLLOUT) {
			// Write is active too, just queue that
			task_lower_event_flag(t, EPOLLIN | EPOLLOUT);
			task_handle_wr_event(t);
		} else {
			task_lower_event_flag(t, EPOLLIN);
		}
		if (task_atomic_load(&t->state) == TASK_STATE_ACTIVE) {
			task_handle_rd_event(t);
		}
		return;
	} else if (revents & EPOLLOUT) {
		task_lower_event_flag(t, EPOLLOUT);
		task_handle_wr_event(t);
		return;
	}

	// If we're here, we've gotten some event that we don't handle.  Just
	// ignore it.  If EPOLLONESHOT is enabled though, we need to re-arm
#ifdef USE_EPOLLONESHOT
	if (task_raise_event_flag(t, 0) < 0) {
		goto worker_do_io_epoll_fail;
	}
#endif
} // worker_handle_task_event



// The main io worker loop.  One instance exists per io worker thread
void *
worker_loop_io(void *arg)
{
	struct worker *w = (struct worker *)arg;

	__thr_current_worker = w;
	w__curtime_us = get_time_us(TASK_TIME_PRECISE);

	while((worker_state_t)task_atomic_load(&w->state) == WORKER_STATE_RUNNING) {
		worker_do_io_epoll(w);
		if (w->poll_listeners) {
			worker_poll_listeners(w);
		}
		worker_process_notifyq(w);
		worker_process_closes(w);

		// The instance worker is responsible for draining blocking-worker
		// notifications and reaping idle blocking workers.  Do it every
		// loop, not only when the worker eventfd happens to fire, so a
		// notification posted before instance_assign_worker() cannot be
		// lost.
		if (w == (struct worker *)task_atomic_load(&w->instance->instance_worker)) {
			worker_handle_instance(w->instance);
		}
	}

	// Time to cleanup
	worker_cleanup(w);
	worker_set_state(w, WORKER_STATE_DEAD);
	return NULL;
} // worker_loop_io



// The main blocking worker loop.  One instance exists per blocking worker thread
void *
worker_loop_blocking(void *arg)
{
	struct worker *w = (struct worker *)arg;

	__thr_current_worker = NULL;
	w__curtime_us = get_time_us(TASK_TIME_PRECISE);

	while ((task_atomic_load(&w->state) == WORKER_STATE_IDLE) ||
	       (task_atomic_load(&w->state) == WORKER_STATE_NOTIFYING) ||
	       (task_atomic_load(&w->state) == WORKER_STATE_BLOCKING)) {
		uint64_t c;

		// If work is already pending (a new worker, or one just reserved by
		// TASK_do_blocking_work()), run it immediately.  Otherwise wait for
		// the dispatcher to notify us.
		if (w->state != WORKER_STATE_BLOCKING) {
			ssize_t len;

			task_atomic_store(&w->notified, 0);
			len = read(w->evfd, (void *)&c, sizeof(c));
			if (len < 0) {
				if (errno == EINTR) {
					continue;
				}
				perror("read");
				worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
				break;
			}
			task_atomic_store(&w->notified, 0);

			if ((w->state == WORKER_STATE_SHUTTING_DOWN) ||
			    (w->state == WORKER_STATE_DEAD)) {
				break;
			}
		}

		// A spurious wakeup can leave us with no work to do
		if (w->state != WORKER_STATE_BLOCKING) {
			continue;
		}

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



void
worker_do_io_epoll(register struct worker *w)
{
	register int wait_time = 0, nfds;

	// Determine the initial time we want to be waiting in epoll for
	// Wait for something to happen!
	w->poll_listeners = true;
	while (true) {
		wait_time = get_next_epoll_timeout_ms(w);
		nfds = epoll_wait(w->gepfd, w->events, w->num_events, wait_time);
		if (nfds < 0) {
			if (errno == EINTR) {
				continue;
			}
			perror("epoll_wait");
			worker_set_state(w, WORKER_STATE_SHUTTING_DOWN);
			return;
		}
		break;
	}

	// Check our timeouts again
	worker_check_timeouts(w);

	if (nfds < w->num_events) {
		// No need to explicitly poll the listeners
		// if we didn't cap-out for active events
		w->poll_listeners = false;

		if (nfds == 0) {
			return;
		}
	}

	// Scan through the list of all the events we've received.  Epoll events
	// carry a generation-tagged TFD cookie, never a raw task pointer, so a
	// stale event for a recycled slot is rejected rather than delivered to the
	// new occupant.
	for (int n = 0; n < nfds; n++) {
		uint64_t cookie = w->events[n].data.u64;
		uint32_t revents = w->events[n].events;
		struct task *t;

		if (cookie == TASK_EPOLL_EVENTFD) {
			worker_handle_event(w, revents);
			continue;
		}

		t = worker_event_task((struct instance *)w->instance, cookie);
		if (t == NULL) {
			continue;	// stale cookie for a recycled slot
		}
		if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) &&
		    task_atomic_load(&t->in_epoll)) {
			worker_handle_task_event(t, revents);
		}
		task_unref(t);
	}
} // worker_do_epoll


// Resolve an epoll cookie to a pinned task.  The reference is taken before the
// generation validation so task_init()'s preserved api_refs always has a
// matching unref, exactly like task_lookup()'s lock-free path.
struct task *
worker_event_task(struct instance *i, uint64_t cookie)
{
	uint32_t tfdi;
	struct task *t;

	if ((i == NULL) || (cookie == TASK_EPOLL_EVENTFD)) {
		return NULL;
	}
	tfdi = TFD_TO_INDEX((int64_t)cookie);
	if (tfdi >= i->tfd_pool_size) {
		return NULL;
	}
	t = i->tfd_pool + tfdi;
	task_ref(t);
	if (task_atomic_load(&t->tfd) != (int64_t)cookie) {
		task_unref(t);
		return NULL;
	}
	return t;
} // worker_event_task

