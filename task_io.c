#include "task_internal.h"



// Creates the given event flag(s)
int
task_create_event_flag(struct task *t, uint32_t flags)
{
	int res;
#ifdef USE_EPOLLONESHOT
	struct epoll_event ev = { .data.u64 = (uint64_t)task_atomic_load(&t->tfd), .events = (EPOLLONESHOT | EPOLLRDHUP | flags) };
#else
	struct epoll_event ev = { .data.u64 = (uint64_t)task_atomic_load(&t->tfd), .events = (EPOLLRDHUP | flags) };
#endif

	if ((res = epoll_ctl(t->epfd, EPOLL_CTL_ADD, t->fd, &ev)) < 0) {
		if (errno != EEXIST) {
			task_atomic_store(&t->in_epoll, 0);
			return -1;
		}
		if ((res = epoll_ctl(t->epfd, EPOLL_CTL_MOD, t->fd, &ev)) < 0) {
			task_atomic_store(&t->in_epoll, 0);
			return -1;
		}
	}
	task_atomic_store(&t->in_epoll, 1);
	t->io_depth = 0;

#ifdef USE_EPOLLET
	// Only turn on EPOLLET AFTER the add, or we race
	// with the kernel on the initial event notification
	task_atomic_store(&t->events, (uint32_t)(ev.events | EPOLLET));
#else
	task_atomic_store(&t->events, (uint32_t)ev.events);
#endif

	return 0;
} // task_create_event_flag



// Raises the given event flag(s)
int
task_raise_event_flag(register struct task *t, register uint32_t flags)
{
	if (task_atomic_load(&t->in_epoll)) {
		register uint32_t events = task_atomic_load(&t->events) | flags;
		struct epoll_event ev = { .data.u64 = (uint64_t)task_atomic_load(&t->tfd), .events = events };
		int res;

		if ((res = epoll_ctl(t->epfd, EPOLL_CTL_MOD, t->fd, &ev)) == 0) {
			task_atomic_store(&t->events, events);
			t->io_depth = 0;
		}
		return res;
	}
	return task_create_event_flag(t, flags);		// We need to do EPOLL_CTL_ADD instead
} // task_raise_event_flag



// Lowers the given event flag(s)
int
task_lower_event_flag(register struct task *t, register uint32_t flags)
{

	// If we're not in epoll, we have nothing to do
	if (task_atomic_load(&t->in_epoll)) {
		register uint32_t events = task_atomic_load(&t->events) & ~(flags);
#ifdef USE_EPOLLONESHOT
		// If EPOLLONESHOT was set, we can bypass the call to epoll_ctl
		// if there's no other IN/OUT flag still set
		if ((events & (EPOLLIN | EPOLLOUT)) == 0) {
			task_atomic_store(&t->events, events);
			return 0;
		}
#endif
		{	struct epoll_event ev = {.data.u64 = (uint64_t)task_atomic_load(&t->tfd), .events = events };
			register int res;

			if ((res = epoll_ctl(t->epfd, EPOLL_CTL_MOD, t->fd, &ev)) == 0) {
				task_atomic_store(&t->events, events);
			}
			return res;
		}
	}
	return 0;
} // task_lower_event_flag



// ---------------------------------------------------------------------------------------------//
// 				    Callback Managment API					//
// ---------------------------------------------------------------------------------------------//

void
task_cancel_write(struct task *t)
{
	t->wr_shut = true;
	t->wr_cancel = true;
	t->wr_expires_modified = false;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
	t->wt_cancelled = true;
} // task_cancel_write



void
task_cancel_read(struct task *t)
{
	t->rd_shut = true;
	t->rd_cancel = true;
	t->rd_expires_modified = false;
	task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
	t->rt_cancelled = true;
} // task_cancel_read



void
task_do_close_cb(struct task *t)
{
	struct worker *w = t->worker;
	struct instance *i = w->instance;
	void (*cb)(int64_t tfd, void *close_cb_data);
	void *cb_data;
	int64_t tfd = t->tfd;
	uint32_t tfdi = TFD_TO_INDEX(tfd);

	// Serialise the final cleanup with any public API call that may still be
	// in flight (bind/listen/timer update).  Once the state has been marked
	// DESTROY and closed has been set, no new API call can acquire the task.
	tfd_lock(t, tfdi);
	if (task_atomic_load(&t->closed)) {
		tfd_unlock(t, tfdi);
		task_action_done(t, FLG_CL);
		return;
	}

	task_atomic_store(&t->state, TASK_STATE_DESTROY);
	t->rd_shut = 1;
	t->wr_shut = 1;
	t->rd_cancel = 1;
	t->wr_cancel = 1;
	t->closed = 1;
	t->tm_cancelled = 1;

	cb = task_atomic_load(&t->stride->close_cb);
	cb_data = task_atomic_load(&t->stride->close_cb_data);
	task_atomic_store(&t->stride->close_cb, NULL);
	task_atomic_store(&t->stride->close_cb_data, NULL);
	tfd_unlock(t, tfdi);

	// From here on the task is guaranteed to stay allocated: state is DESTROY
	// and the FLG_CL action is still held.  No public API can acquire it.

	// If it's a parent listener, ask all of its child listeners to close.
	if ((t->type == TASK_TYPE_LISTEN) && (t->listen_child == 0)) {
		task_shutdown_listen_children(t);
	}

	// If it's a listener, remove it from the worker's listeners list
	if (t->type == TASK_TYPE_LISTEN) {
		pthread_mutex_lock(&i->listeners_lock);
		task_remove_list(&w->listeners, t);
		pthread_mutex_unlock(&i->listeners_lock);
	}

	// If it has an active fd, cancel all the activity on it and close it if we are allowed to
	if (t->fd >= 0) {
		// If it's a user registered socket, do not close it, just de-register it from epoll
		if (t->registered_fd) {
			epoll_ctl(t->epfd, EPOLL_CTL_DEL, t->fd, NULL);
		} else {
			shutdown(t->fd, SHUT_RDWR);
			close(t->fd);
		}
		task_atomic_store(&t->in_epoll, 0);
		task_atomic_store(&t->events, 0);
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
	t->stride->accept_cb = NULL;
	t->stride->connect_cb = NULL;

	// Cancel the timeouts
	task_destroy_timeouts(t);

	// If we're a child listener, don't make the close callback.  The caller
	// only knows about the parent listener and won't know what to do with a
	// a child listener since it isn't even aware of its existence.
	//
	// The callback is made before the FLG_CL action is dropped, so the task
	// slot stays allocated for its duration.
	if ((cb != NULL) && (t->listen_child == 0) &&
	    (((instance_state_t)task_atomic_load(&i->state) != INSTANCE_STATE_SHUTTING_DOWN))) {
		errno = 0;
		cb(tfd, cb_data);
	}

	task_action_done(t, FLG_CL);
} // task_do_close_cb



void
task_do_readv_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno)
{
	void (*cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data) = t->rdv_cb;
	void *cb_data = t->rd_cb_data;
	const struct iovec *iov = t->rdv_iov;
	int iovcnt = t->rdv_iovcnt;

	t->rt_cancelled = true;
	t->rd_expires_modified = false;
	task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);

	// If we don't have a callback, just terminate the write peacefully
	if ((cb == NULL) || t->rd_cancel || (task_atomic_load(&t->state) != TASK_STATE_ACTIVE)) {
		task_action_done(t, FLG_NONE);	// Forces a task release check
		return;
	}

	errno = cb_errno;
	cb(tfd, iov, iovcnt, result, cb_data);
} // task_do_readv_cb



void
task_do_read_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno)
{
	void (*cb)(int64_t tfd, void *buf, ssize_t result, void *rd_cb_data) = t->rd_cb;
	void *cb_data = t->rd_cb_data;
	void *rdbuf = t->rd_buf;

	t->rt_cancelled = true;
	t->rd_expires_modified = false;
	task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);

	// If we don't have a callback, just terminate the read peacefully
	if ((cb == NULL) || t->rd_cancel || (task_atomic_load(&t->state) != TASK_STATE_ACTIVE)) {
		task_action_done(t, FLG_NONE);	// Forces a task release check
		return;
	}

	errno = cb_errno;
	cb(tfd, rdbuf, result, cb_data);
} // task_do_read_cb



void
task_do_accept_cb(struct task *t, int64_t tfd, int cb_errno)
{
	void (*cb)(int64_t tfd, void *accept_cb_data) = t->stride->accept_cb;
	void *cb_data = t->stride->accept_cb_data;

	t->stride->accept_cb = NULL;
	t->stride->accept_cb_data = NULL;

	if ((cb == NULL) || t->rd_cancel || (task_atomic_load(&t->state) != TASK_STATE_ACTIVE)) {
		(void)task_queue_close(t);
		return;
	}

	errno = cb_errno;
	cb(tfd, cb_data);		// t is still locked at this point
} // task_do_accept_cb



void
task_do_writev_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno)
{
	void (*cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data) = t->wrv_cb;
	const struct iovec *iov = t->wrv_iov;
	void *cb_data = t->wr_cb_data;
	int iovcnt = t->wrv_iovcnt;

	t->wt_cancelled = true;
	t->wr_expires_modified = false;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);

	// If we don't have a callback, just terminate the write peacefully
	if ((cb == NULL) || t->wr_cancel || (task_atomic_load(&t->state) != TASK_STATE_ACTIVE)) {
		task_action_done(t, FLG_NONE);	// Forces a task release check
		return;
	}

	errno = cb_errno;
	cb(tfd, iov, iovcnt, result, cb_data);
} // task_do_writev_cb



void
task_do_write_cb(struct task *t, int64_t tfd, ssize_t result, int cb_errno)
{
	void (*cb)(int64_t tfd, const void *buf, ssize_t result, void *wr_cb_data) = t->wr_cb;
	void *cb_data = t->wr_cb_data;
	const void *wrbuf = t->wr_buf;

	t->wt_cancelled = true;
	t->wr_expires_modified = false;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);

	// If we don't have a callback, just terminate the write peacefully
	if ((cb == NULL) || t->wr_cancel || (task_atomic_load(&t->state) != TASK_STATE_ACTIVE)) {
		task_action_done(t, FLG_NONE);	// Forces a task release check
		return;
	}

	errno = cb_errno;
	cb(tfd, wrbuf, result, cb_data);
} // task_do_write_cb



void
task_do_connect_cb(struct task *t, int64_t tfd, int result, int cb_errno)
{
	void (*cb)(int64_t tfd, int result, void *connect_cb_data) = t->stride->connect_cb;
	void *cb_data = t->stride->connect_cb_data;

	t->wt_cancelled = true;
	t->wr_expires_modified = false;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);

	// If there's no callback for the connection, just terminate
	if ((cb == NULL) || t->wr_cancel || (task_atomic_load(&t->state) != TASK_STATE_ACTIVE)) {
		task_action_done(t, FLG_NONE);	// Forces a task release check
		return;
	}

	errno = cb_errno;
	cb(tfd, result, cb_data);
} // task_do_connect_cb



// Queue a close action for every child listener belonging to the given parent.
// Child listeners are identified by their parent_tfd field rather than a linked
// list of raw task pointers.  This avoids dangling pointers if a child is closed
// independently, and avoids borrowing notification nodes from another worker's
// free list.  Listener close is a rare operation, so an O(pool) scan is fine.
void
task_shutdown_listen_children(struct task *parent)
{
	struct instance *i = parent->worker->instance;
	int64_t ptfd = parent->tfd;

	if (i == NULL) {
		return;
	}

	for (uint32_t n = 0; n < i->tfd_pool_size; n++) {
		struct task *c = i->tfd_pool + n;

		if (c == parent) {
			continue;
		}

		// Validate the child and mark it DESTROY under its own TFD lock.
		// This avoids acting on a slot that is concurrently recycled.
		if (!task_claim_listen_child(i, c, ptfd)) {
			continue;
		}

		// task_queue_close() only runs teardown inline when this is the
		// child's owning worker.  If the notification pool is exhausted and
		// we are not the owner, shutdown's nuke pass is responsible for it.
		(void)task_queue_close(c);
		task_unref(c);
	}
} // task_shutdown_listen_children



struct task *
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
	case TASK_TYPE_LISTEN:
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
	if ((t = task_get_free_task(i)) == NULL) {
		return NULL;
	}

	// Publish the fd before worker selection.  worker_select_io_worker()
	// uses it for the CPU-affinity / SO_INCOMING_CPU path.
	task_atomic_store(&t->fd, fd);

	// Assign a worker
	if (w == NULL) {
		if ((w = worker_select_io_worker(i, t, outgoing)) == NULL) {
			task_free(i, t, false);
			errno = ECHILD;
			return NULL;
		}
	}
	t->worker = w;
	t->epfd = w->gepfd;
	t->type = type;
	task_atomic_store(&t->stride->close_cb, close_cb);
	task_atomic_store(&t->stride->close_cb_data, close_cb_data);
	__atomic_add_fetch(&w->num_tasks, 1, __ATOMIC_RELAXED);

	// task_get_free_task() has already published the slot as ACTIVE.
	return t;
} // task_create



// Build a stack-local iovec window covering at most maxlen bytes starting at
// bufpos within the caller's iovec.  Used to apply the same TASK_MAX_IO_UNIT
// fairness budget to the vector IO calls that the buffer IO calls already use.
static int
task_build_iov_window(const struct iovec *iov, int iovcnt, size_t bufpos,
		      size_t maxlen, struct iovec *out)
{
	size_t skip = bufpos;
	int n = 0, outcnt = 0;

	while ((n < iovcnt) && (skip >= iov[n].iov_len)) {
		skip -= iov[n].iov_len;
		n++;
	}
	if (n >= iovcnt) {
		return 0;
	}

	while ((n < iovcnt) && (maxlen > 0)) {
		size_t len = iov[n].iov_len - skip;

		if (len > maxlen) {
			len = maxlen;
		}
		if (len > 0) {
			out[outcnt].iov_base = ((char *)iov[n].iov_base) + skip;
			out[outcnt].iov_len = len;
			outcnt++;
			maxlen -= len;
		}
		skip = 0;
		n++;
	}
	return outcnt;
} // task_build_iov_window



ssize_t
task_write_vector(register struct task *t, register bool wr_expires_modified, register bool queued)
{
	register size_t wrv_bufpos = t->wrv_bufpos, wrv_buflen = t->wrv_buflen;
	register const struct iovec *wrv_iov = t->wrv_iov;
	register int wrv_iovcnt = t->wrv_iovcnt;
	register size_t max_can_do = TASK_MAX_IO_UNIT;
	int64_t tfd = t->tfd;

	if (t->wr_shut) {
		if (queued) {
			task_do_writev_cb(t, tfd, -1, EPIPE);	
			return 0;
		}
		errno = EPIPE;
		return -1;
	}

	// Keep trying to write until we're done, or we're blocked
	while (wrv_bufpos < wrv_buflen) {
		ssize_t written;
		size_t window;

		// Restrict the amount that can be written in one scheduling slice
		if (max_can_do == 0) {
			t->wrv_bufpos = wrv_bufpos;	// Need to record bufpos for later
			if (task_notify_action(t, FLG_WR, false, true)) {
				return 0;
			}
			max_can_do = SIZE_MAX;
		}

		window = wrv_buflen - wrv_bufpos;
		if (window > max_can_do) {
			window = max_can_do;
		}

		if ((wrv_bufpos == 0) && (window == wrv_buflen)) {
			// We have no offset and everything fits in this slice.  Just
			// use what was passed to us for speed.
			written = writev(t->fd, wrv_iov, wrv_iovcnt);
		} else {
			struct iovec iov[IOV_MAX];
			int iovcnt = task_build_iov_window(wrv_iov, wrv_iovcnt,
							   wrv_bufpos, window, iov);

			assert(iovcnt > 0);
			written = writev(t->fd, iov, iovcnt);
		}

		if (written < 0) {
			if (errno == EAGAIN) {
				// Write blocked for now, raise EPOLLOUT and wait to be unblocked
				t->wrv_bufpos = wrv_bufpos;	// Need to record bufpos for later
				if (task_raise_event_flag(t, EPOLLOUT) == 0) {
					if (wr_expires_modified) {
						task_notify_action(t, FLG_WT, false, false);
					}
					errno = 0;
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other write or event flag raising error.  Inform caller
			if (wrv_bufpos > 0) {
				// If we had written something before the error.  Inform caller of how
				// much that was.  They'll have to catch the error on their next write
				errno = 0;
				if (queued) {
					task_do_writev_cb(t, tfd, (ssize_t)wrv_bufpos, 0);
					return 0;
				}
				return (ssize_t)wrv_bufpos;
			}

			if (queued) {
				task_do_writev_cb(t, tfd, -1, errno);
				return 0;
			}
			return -1;
		}

		// We wrote something!
		wrv_bufpos += written;
		if (max_can_do != SIZE_MAX) {
			if ((size_t)written >= max_can_do) {
				max_can_do = 0;
			} else {
				max_can_do -= written;
			}
		}
	}

	// We wrote it all.  Make the callback
	if (queued) {
		task_do_writev_cb(t, tfd, (ssize_t)wrv_bufpos, 0);
		return 0;
	}
	errno = 0;
	return (ssize_t)wrv_bufpos;
} // task_write_vector



// Attempt to flush out anything left in our task write buffer
ssize_t
task_write_buffer(register struct task *t, register bool wr_expires_modified, register bool queued)
{
	register size_t max_can_do = TASK_MAX_IO_UNIT;
	register size_t wr_bufpos = t->wr_bufpos, wr_buflen = t->wr_buflen;
	register const char *wr_buf = t->wr_buf;
	int64_t tfd = t->tfd;

	if (t->wr_shut) {
		if (queued) {
			task_do_write_cb(t, tfd, -1, EPIPE);
			return 0;
		}
		errno = EPIPE;
		return -1;
	}

	// Keep trying to write until we're done, or we're blocked
	while (wr_bufpos < wr_buflen) {
		register size_t to_write;
		register ssize_t written;

		// Restrict the amount that can be written in one go for fairness
		if (max_can_do == 0) {
			t->wr_bufpos = wr_bufpos;	// Need to record current position for next time
			if (task_notify_action(t, FLG_WR, false, true)) {
				return 0;
			}
			max_can_do = SIZE_MAX;
		}
		to_write = wr_buflen - wr_bufpos;
		if (to_write > max_can_do) {
			to_write = max_can_do;
		}

		written = write(t->fd, wr_buf + wr_bufpos, to_write);
		if (written < 0) {
			if (errno == EAGAIN) {
				// Write blocked for now, raise EPOLLOUT and wait to be unblocked
				t->wr_bufpos = wr_bufpos;	// Need to record current position for next time
				if (task_raise_event_flag(t, EPOLLOUT) == 0) {
					if (wr_expires_modified) {
						task_notify_action(t, FLG_WT, false, false);
					}
					errno = 0;
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other write or event flag raising error.  Inform caller
			if (wr_bufpos > 0) {
				// If we had written something before the error.  Inform caller of how
				// much that was.  They'll have to catch the error on their next write
				if (queued) {
					task_do_write_cb(t, tfd, (ssize_t)wr_bufpos, 0);
					return 0;
				}
				errno = 0;
				return (ssize_t)wr_bufpos;
			}

			if (queued) {
				task_do_write_cb(t, tfd, -1, errno);
				return 0;
			}
			return -1;
		}

		// We wrote something!
		wr_bufpos += written;
		max_can_do -= written;
	}

	// We read it all.  Make the callback
	if (queued) {
		task_do_write_cb(t, tfd, (ssize_t)wr_bufpos, 0);
		return 0;
	}
	errno = 0;
	return (ssize_t)wr_bufpos;
} // task_write_buffer



void
task_handle_connect_event(struct task *t)
{
	struct instance *i = t->worker->instance;
	int err;
	socklen_t len = sizeof(int);

	assert(t->type == TASK_TYPE_CONNECT);

	if (t->wr_shut) {
		task_do_connect_cb(t, t->tfd, -1, EPIPE);
		return;
	}

	if (!i->all_io_workers_affined) {
		worker_learn_cpu_affinity(t);
	}

	// Turn task into a regular IO task and make callback
	t->type = TASK_TYPE_IO;

	// Determine result of the connect
	errno = 0;
	if (getsockopt(t->fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0) {
		errno = err;
	}

	if (errno != 0) {
		task_do_connect_cb(t, t->tfd, -1, errno);
	} else {
		task_do_connect_cb(t, t->tfd, 1, 0);
	}
} // task_handle_connect_event



void
task_handle_connecting_action(register struct task *t, register bool wr_expires_modified)
{
	// Start the connect
	while (1) {
		if (connect(t->fd, (struct sockaddr *)&t->stride->addr, t->addrlen) == 0) {
			task_handle_connect_event(t);
			return;
		}

		if (errno == EINTR) {
			// Try again
			continue;
		}

		if ((errno == EINPROGRESS) || (errno == EAGAIN)) {
			if (task_raise_event_flag(t, EPOLLOUT) == 0) {
				// We queued it, return
				if (wr_expires_modified) {
					task_notify_action(t, FLG_WT, false, false);
				}
				errno = 0;
				return;
			}
		}
		break;
	}

	// Connect failure of some kind. Notify the caller
	task_do_connect_cb(t, t->tfd, -1, errno);
} // task_handle_connecting_action



ssize_t
task_read_vector(register struct task *t, register bool rd_expires_modified, register bool queued)
{
	register size_t rdv_bufpos = t->rdv_bufpos, rdv_buflen = t->rdv_buflen;
	register const struct iovec *rdv_iov = t->rdv_iov;
	register int rdv_iovcnt = t->rdv_iovcnt;
	register size_t max_can_do = TASK_MAX_IO_UNIT;
	int64_t tfd = t->tfd;

	if (t->rd_shut) {
		if (queued) {
			task_do_readv_cb(t, tfd, -1, EPIPE);	
			return 0;
		}
		errno = EPIPE;
		return -1;
	}

	// Read what we can
	while (rdv_bufpos < rdv_buflen) {
		ssize_t reddin;
		size_t window;

		// Restrict the amount that can be read in one scheduling slice,
		// matching the buffer read path.
		if (max_can_do == 0) {
			if (queued) {
				task_do_readv_cb(t, tfd, (ssize_t)rdv_bufpos, 0);	
				return 0;
			}
			errno = 0;
			return (ssize_t)rdv_bufpos;
		}

		window = rdv_buflen - rdv_bufpos;
		if (window > max_can_do) {
			window = max_can_do;
		}

		if ((rdv_bufpos == 0) && (window == rdv_buflen)) {
			// We have no offset and everything fits in this slice.  Just
			// use what was passed to us for speed.
			reddin = readv(t->fd, rdv_iov, rdv_iovcnt);
		} else {
			struct iovec iov[IOV_MAX];
			int iovcnt = task_build_iov_window(rdv_iov, rdv_iovcnt,
							   rdv_bufpos, window, iov);

			assert(iovcnt > 0);
			reddin = readv(t->fd, iov, iovcnt);
		}

		if (reddin < 0) {
			// We read until we're blocked.
			if (errno == EAGAIN) {
				// Make a callback now if we got anything at all
				// Do not raise EPOLLIN again until user asks us to read more
				if (rdv_bufpos > 0) {
					if (queued) {
						task_do_readv_cb(t, tfd, (ssize_t)rdv_bufpos, 0);	
						return 0;
					}
					errno = 0;
					return (ssize_t)rdv_bufpos;
				}

				// We got nothing at all.  Raise EPOLLIN and wait for something
				if (task_raise_event_flag(t, EPOLLIN) == 0) {
					// Activate the read timeout if needed
					if (rd_expires_modified) {
						task_notify_action(t, FLG_RT, false, false);
					}
					errno = 0;
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other read or event flag raise error.
			if (queued) {
				task_do_readv_cb(t, tfd, -1, errno);	
				return 0;
			}
			return -1;
		}

		// End of file response check
		if (reddin == 0) {
			task_lower_event_flag(t, EPOLLIN);
			t->rd_shut = true;

			// Notify first if we have anything in the buffer.  The actual EOF
			// condition will have to get picked up on the next read attempt
			if (rdv_bufpos > 0) {
				if (queued) {
					task_do_readv_cb(t, tfd, (ssize_t)rdv_bufpos, 0);	
					return 0;
				}
				errno = 0;
				return (ssize_t)rdv_bufpos;
			}

			// We got nothing at all.
			if (queued) {
				task_do_readv_cb(t, tfd, -1, EPIPE);	
				return 0;
			}
			errno = EPIPE;
			return -1;
		}

		// We read something!
		rdv_bufpos += reddin;
		if (max_can_do != SIZE_MAX) {
			if ((size_t)reddin >= max_can_do) {
				max_can_do = 0;
			} else {
				max_can_do -= reddin;
			}
		}
	}

	// We read it all.  Make the callback
	if (queued) {
		task_do_readv_cb(t, tfd, (ssize_t)rdv_bufpos, 0);	
		return 0;
	}
	errno = 0;
	return (ssize_t)rdv_bufpos;
} // task_read_vector



// Attempt to flush out anything left in our task write buffer
ssize_t
task_read_buffer(register struct task *t, register bool rd_expires_modified, register bool queued)
{
	register size_t max_can_do = TASK_MAX_IO_UNIT;
	register size_t rd_bufpos = t->rd_bufpos, rd_buflen = t->rd_buflen;
	register char *rd_buf = t->rd_buf;
	int64_t tfd = t->tfd;

	if (t->rd_shut) {
		if (queued) {
			task_do_read_cb(t, tfd, -1, EPIPE);		
			return 0;
		}
		errno = EPIPE;
		return -1;
	}

	// Read what we can
	while (rd_bufpos < rd_buflen) {
		register size_t to_read;
		register ssize_t reddin;

		// Restrict the amount that can be read in one go for fairness
		if (max_can_do == 0) {
			if (queued) {
				task_do_read_cb(t, tfd, (ssize_t)rd_bufpos, 0);	
				return 0;
			}
			errno = 0;
			return (ssize_t)rd_bufpos;
		}

		to_read = rd_buflen - rd_bufpos;
		if (to_read > max_can_do) {
			to_read = max_can_do;
		}

		reddin = read(t->fd, rd_buf + rd_bufpos, to_read);
		if (reddin < 0) {
			// We read until we're blocked.
			if (errno == EAGAIN) {
				// Make a callback now if we got anything at all
				// Do not raise EPOLLIN again until user asks us to read more
				if (rd_bufpos > 0) {
					if (queued) {
						task_do_read_cb(t, tfd, (ssize_t)rd_bufpos, 0);	
						return 0;
					}
					errno = 0;
					return (ssize_t)rd_bufpos;
				}

				// We got nothing at all.  Raise EPOLLIN and wait for something
				if (task_raise_event_flag(t, EPOLLIN) == 0) {
					if (rd_expires_modified) {
						task_notify_action(t, FLG_RT, false, false);
					}
					errno = 0;
					return 0;
				}
			}

			if (errno == EINTR) {
				continue;
			}

			// Some other read or event flag raise error.
			if (queued) {
				task_do_read_cb(t, tfd, -1, errno);	
				return 0;
			}
			return -1;
		}

		// End of file response check
		if (reddin == 0) {
			task_lower_event_flag(t, EPOLLIN);	// Ensure poll event flag is lowered
			t->rd_shut = true;

			// Notify first if we have anything in the buffer.  The actual EOF
			// condition will have to get picked up on the next read attempt
			if (rd_bufpos > 0) {
				if (queued) {
					task_do_read_cb(t, tfd, (ssize_t)rd_bufpos, 0);	
					return 0;
				}
				errno = 0;
				return (ssize_t)rd_bufpos;
			}

			// We got nothing at all.
			if (queued) {
				task_do_read_cb(t, tfd, -1, EPIPE);		
				return 0;
			}
			errno = EPIPE;
			return -1;
		}

		// We read something!
		rd_bufpos += reddin;
		max_can_do -= reddin;
	}

	// We read it all.  Make the callback
	if (queued) {
		task_do_read_cb(t, tfd, rd_bufpos, 0);		
		return 0;
	}
	errno = 0;
	return (ssize_t)rd_bufpos;
} // task_read_buffer



// Fast setting of non-blocking state on a socket
int
sock_set_nonblocking(int sock)
{
	int flags[1] = {1};

	return ioctl(sock, FIONBIO, flags);
} // sock_set_nonblocking



int
sock_set_sndbuf(struct instance *i, int sock)
{
	int sndbuf_size = i->per_task_sndbuf;

	// Placing an upper limit of 1MB on outgoing buffer size appears to ensure
	// that the TCP stack doesn't go into hysteresis with huge numbers of clients
	if (sndbuf_size > (TASK_MAX_IO_UNIT * 32)) {
		sndbuf_size = (TASK_MAX_IO_UNIT * 32);
	} else if (sndbuf_size < TASK_MAX_IO_UNIT) {
		sndbuf_size = TASK_MAX_IO_UNIT;
	}
	return setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(sndbuf_size));
} // sock_set_sndbuf



int
sock_set_rcvbuf(struct instance *i, int sock)
{
	int rcvbuf_size = i->per_task_sndbuf;

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
void
task_handle_listen_event(struct task *t)
{
	struct instance *i = t->worker->instance;

	while ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) && (t->fd >= 0)) {
		int cfd;
		struct task *t_new;
		struct sockaddr_storage		addr;
		socklen_t			addrlen;

		// Attempt to accept.  Break out of loop if nothing is there to pick up.
		// accept4() gives us CLOEXEC and non-blocking atomically, avoiding an
		// fd leak window between accept() and fcntl().
		addrlen = sizeof(addr);
		if ((cfd = accept4(t->fd, (struct sockaddr *)&addr, &addrlen,
				   SOCK_NONBLOCK | SOCK_CLOEXEC)) < 0) {
			task_action_t ta;

			switch(errno) {
			case EAGAIN:
			case EMFILE:
			case ENFILE:
			case ECONNABORTED:
			case EPROTO:
			case EPERM:
			case ENOBUFS:
			case ENOMEM:
				// Transient/soft conditions.  Abort this accept batch but
				// keep the listener alive and re-arm it below.
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
			}
#ifdef USE_EPOLLONESHOT
			if (ta == TASK_ACTION_ABORT) {
				// Re-arm the one-shot listener event.  Without this the
				// listener silently stops accepting after a transient
				// condition such as EMFILE.
				if (task_raise_event_flag(t, EPOLLIN) < 0) {
					perror("task_raise_event_flag");
					goto task_handle_listen_event_fail;
				}
			}
#endif
			if (ta == TASK_ACTION_ABORT) {
				break;
			}
			goto task_handle_listen_event_fail;
		}

		//  If we get a new connection, but can't inform anyone, just close it
		if (t->stride->accept_cb == NULL) {
			close(cfd);
			continue;
		}

		// We've got a new connection!
		if (sock_set_nonblocking(cfd) < 0) {
			close(cfd);
			t->stride->accept_cb(-1, t->stride->accept_cb_data);
			continue;
		}
		if (sock_set_sndbuf(i, cfd) < 0) {
			perror("sock_set_sndbuf");
		}
		if (sock_set_rcvbuf(i, cfd) < 0) {
			perror("sock_set_rcvbuf");
		}
		if (sock_set_linger(cfd, 0) < 0) {
			perror("sock_set_linger");
		}

		// Inherit the close callback from the accept task until the user sets their
		// own.  The caller can distingish between the two by inspecting the tfd that
		// arrives on the accept close callback.  If the tfd matches the accept tfd,
		// then the close is on the accept task, otherwise it is for the new task
		void *inherited_cb_data = task_atomic_load(&t->stride->close_cb_data);
		void (*inherited_cb)(int64_t, void *) = task_atomic_load(&t->stride->close_cb);

		if ((t_new = task_create(i, TASK_TYPE_IO, cfd, NULL, inherited_cb_data,
					 inherited_cb, false)) == NULL) {
			// We failed to create a task structure for it :(  Inform user
			// of the failure so they're aware of us dropping connections
			if (t->stride->accept_cb) {
				// t is still locked at this point
				t->stride->accept_cb(-1, t->stride->accept_cb_data);
			}
			close(cfd);
			continue;
		}

		// Record the peer address against the accepted connection, and learn
		// CPU affinity from the accepted socket rather than the listener
		t_new->addrlen = addrlen;
		memcpy(&t_new->stride->addr, &addr, addrlen);
		if (!i->all_io_workers_affined) {
			worker_learn_cpu_affinity(t_new);
		}

		t_new->stride->accept_cb = t->stride->accept_cb;
		t_new->stride->accept_cb_data = t->stride->accept_cb_data;

		// Although we can queue it so that new task starts on the correct worker, there is
		// a small delay for the correct thread to activate.  In order to keep TTFB latency
		// as low as possible, it's faster to make the callback on this mismatched thread
		// right now, and then move the task to the correct thread after the I/O starts
		// Also, we don't need to grab any lock this way, because no one else knows about
		// this new task until we make the callback, so it can't disappear on us
		task_do_accept_cb(t_new, t_new->tfd, 0);
	}
	return;

task_handle_listen_event_fail:
	// Major accept failure.  Request a deferred close on the owning worker.
	task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
	(void)task_defer_close(t);
} // task_handle_listen_event



void
task_handle_wr_event(struct task *t)
{
	if (t->wr_cancel) {
		return;
	}

	// Write stuff out as needed
	if (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_BUFFER) {
		task_write_buffer(t, t->wr_expires_modified, true);	
	} else if (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_VECTOR) {
		task_write_vector(t, t->wr_expires_modified, true);	
	} else if (task_atomic_load(&t->wr_state) == TASK_WRITE_STATE_CONNECT) {
		task_handle_connect_event(t);				
	} else {
		// Do nothing
	}
} // task_handle_wr_event



void
task_handle_rd_event(struct task *t)
{
	if (t->rd_cancel) {
		return;
	}

	// Read in whatever as directed
	if (task_atomic_load(&t->rd_state) == TASK_READ_STATE_BUFFER) {
		task_read_buffer(t, t->rd_expires_modified, true);	
	} else if (task_atomic_load(&t->rd_state) == TASK_READ_STATE_VECTOR) {
		task_read_vector(t, t->rd_expires_modified, true);	
	} else if (task_atomic_load(&t->rd_state) == TASK_READ_STATE_LISTEN) {
		task_handle_listen_event(t);				
	} else {
		// Do nothing
	}
} // task_handle_rd_event



// Creates a duplicate set of listeners for the given task
// across all available IO workers for the given instance.
void
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
	sock_type |= SOCK_NONBLOCK | SOCK_CLOEXEC;

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
		if (((worker_state_t)task_atomic_load(&w->state) != WORKER_STATE_CREATED) &&
		    ((worker_state_t)task_atomic_load(&w->state) != WORKER_STATE_RUNNING)) {
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
		if (bind(nfd, (const struct sockaddr *)&t->stride->addr, t->addrlen) < 0) {
			perror("instance_listen_balance->bind");
			close(nfd);
			continue;
		}

		// Now listen on it
		if (listen(nfd, TASK_LISTEN_BACKLOG) < 0) {
			perror("instance_listen_balance->listen");
			close(nfd);
			continue;
		}

		// Create a child task to look after the socket. Ensure to specify the worker for the new task
		void *inherited_cb_data = task_atomic_load(&t->stride->close_cb_data);
		void (*inherited_cb)(int64_t, void *) = task_atomic_load(&t->stride->close_cb);

		if ((nt = task_create(i, TASK_TYPE_LISTEN, nfd, w, inherited_cb_data,
				      inherited_cb, false)) == NULL) {
			perror("instance_listen_balance->task_create");
			close(nfd);
			continue;
		}

		// Convert new task to an listener type and inform task to expect incoming events
		memcpy(&nt->stride->addr, &t->stride->addr, t->addrlen);
		nt->listen_child = 1;
		nt->stride->parent_tfd = t->tfd;
		nt->addrlen = t->addrlen;
		nt->stride->accept_cb = t->stride->accept_cb;
		nt->stride->accept_cb_data = t->stride->accept_cb_data;
		nt->rd_expires_in_us = TIMER_TIME_DESTROY;
		task_atomic_store(&nt->rd_state, TASK_READ_STATE_LISTEN);
		if (task_raise_event_flag(nt, EPOLLIN) < 0) {
			perror("instance_listen_balance->task_raise_event_flag");
			task_lower_event_flag(nt, (EPOLLIN | EPOLLOUT));
			// Let the owning worker close the fd and free the task
			(void)task_queue_close(nt);
			continue;
		}

		// Add to the listeners list of the target worker
		pthread_mutex_lock(&i->listeners_lock);
		nt->task_next = w->listeners;
		w->listeners = nt;
		pthread_mutex_unlock(&i->listeners_lock);
	}
} // instance_listen_balance



//----------------------------------------------------------------------------------------------//
// 				Task Library Socket IO API					//
//----------------------------------------------------------------------------------------------//

// Will write the entire contents of the supplied buffers to the given tfd, or die trying
void
TASK_socket_writev(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *wr_cb_data,
		  void (*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_WR);
	size_t wrv_buflen = 0;
	bool queued;
	int n;

	if (t == NULL) {
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
		return;
	}

	if (t->wr_shut) {
		task_unlock(t, FLG_WR);
		errno = EPIPE;
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
		return;
	}

	if (t->wr_cancel) {
		task_unlock(t, FLG_WR);
		errno = EOWNERDEAD;
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
		return;
	}

	if (t->fd < 0) {
		task_unlock(t, FLG_WR);
		errno = EBADF;
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
		return;
	}

	// Validate the iov arguments and determine total write length
	if ((iov == NULL) || (iovcnt < 0) || (iovcnt > IOV_MAX)) {
		task_unlock(t, FLG_WR);
		errno = EINVAL;
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
		return;
	}

	for (n = 0; n < iovcnt; n++) {
		if (iov[n].iov_base == NULL) {
			task_unlock(t, FLG_WR);
			errno = EINVAL;
			if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
			return;
		}
		if (iov[n].iov_len > (SIZE_MAX - wrv_buflen)) {
			task_unlock(t, FLG_WR);
			errno = EOVERFLOW;
			if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
			return;
		}
		wrv_buflen += iov[n].iov_len;
	}

	if (wrv_buflen == 0) {
		task_unlock(t, FLG_WR);
		errno = EINVAL;
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
		return;
	}

	t->wrv_iov = iov;
	t->wrv_bufpos = 0;
	t->wrv_cb = wrv_cb;
	t->wrv_iovcnt = iovcnt;
	t->wr_cb_data = wr_cb_data;
	t->wrv_buflen = wrv_buflen;
	t->wr_expires_in_us = expires_in_us;
	t->wr_expires_modified = 1;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_VECTOR);

	// The fast path is only safe on the task's own worker.  Any other
	// caller queues the operation so that all task state and epoll
	// manipulation stays on a single thread.
	if (lockless_worker(t->worker) && (t->io_depth < TASK_MAX_IO_DEPTH)) {
		ssize_t result;

		t->io_depth++;
		task_unlock(t, FLG_WR);
		if ((result = task_write_vector(t, true, false)) == 0) {
			return;		// Operation is queued
		}
		t->wr_expires_modified = 0;
		task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
		if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) && (t->wr_cancel == 0) && wrv_cb) {
			wrv_cb(tfd, iov, iovcnt, result, wr_cb_data);
		}
		return;
	}
	if (lockless_worker(task_atomic_load(&t->worker))) {
		t->io_depth = 0;
	}

	queued = task_notify_action(t, FLG_WR, false, true);
	if (!queued) {
		t->wr_expires_modified = 0;
		task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
	}
	task_unlock(t, FLG_WR);
	if (!queued) {
		errno = EOWNERDEAD;
		if (wrv_cb) wrv_cb(tfd, iov, iovcnt, -1, wr_cb_data);
	}
} // TASK_socket_writev



void
TASK_socket_write(int64_t tfd, const void *wrbuf, size_t buflen, int64_t expires_in_us, void *wr_cb_data,
		 void (*wr_cb)(int64_t tfd, const void *wrbuf, ssize_t result, void *wr_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_WR);
	bool queued;

	if (t == NULL) {
		if (wr_cb) wr_cb(tfd, wrbuf, -1, wr_cb_data);
		return;
	}

	if (t->wr_shut) {
		task_unlock(t, FLG_WR);
		errno = EPIPE;
		if (wr_cb) wr_cb(tfd, wrbuf, -1, wr_cb_data);
		return;
	}

	if (t->wr_cancel) {
		task_unlock(t, FLG_WR);
		errno = EOWNERDEAD;
		if (wr_cb) wr_cb(tfd, wrbuf, -1, wr_cb_data);
		return;
	}

	if (t->fd < 0) {
		task_unlock(t, FLG_WR);
		errno = EBADF;
		if (wr_cb) wr_cb(tfd, wrbuf, -1, wr_cb_data);
		return;
	}

	if (buflen == 0) {
		task_unlock(t, FLG_WR);
		errno = EINVAL;
		if (wr_cb) wr_cb(tfd, wrbuf, -1, wr_cb_data);
		return;
	}

	t->wr_bufpos = 0;
	t->wr_cb = wr_cb;
	t->wr_buf = wrbuf;
	t->wr_buflen = buflen;
	t->wr_cb_data = wr_cb_data;
	t->wr_expires_in_us = expires_in_us;
	t->wr_expires_modified = 1;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_BUFFER);

	if (lockless_worker(t->worker) && (t->io_depth < TASK_MAX_IO_DEPTH)) {
		ssize_t result;

		t->io_depth++;
		task_unlock(t, FLG_WR);
		if ((result = task_write_buffer(t, true, false)) == 0) {
			return;		// Operation is queued
		}
		t->wr_expires_modified = 0;
		task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
		if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) && (t->wr_cancel == 0) && wr_cb) {
			wr_cb(tfd, wrbuf, result, wr_cb_data);
		}
		return;
	}
	if (lockless_worker(task_atomic_load(&t->worker))) {
		t->io_depth = 0;
	}

	queued = task_notify_action(t, FLG_WR, false, true);
	if (!queued) {
		t->wr_expires_modified = 0;
		task_atomic_store(&t->wr_state, TASK_WRITE_STATE_IDLE);
	}
	task_unlock(t, FLG_WR);
	if (!queued) {
		errno = EOWNERDEAD;
		if (wr_cb) wr_cb(tfd, wrbuf, -1, wr_cb_data);
	}
} // TASK_socket_write



void
TASK_socket_readv(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *rd_cb_data,
		 void (*rdv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_RD);
	size_t rdv_buflen = 0;
	bool queued;
	int n;

	if (t == NULL) {
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
		return;
	}

	if (t->rd_shut) {
		task_unlock(t, FLG_RD);
		errno = EPIPE;
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
		return;
	}

	if (t->rd_cancel) {
		task_unlock(t, FLG_RD);
		errno = EOWNERDEAD;
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
		return;
	}

	if (t->fd < 0) {
		task_unlock(t, FLG_RD);
		errno = EBADF;
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
		return;
	}

	// Validate the iov arguments and determine total read length
	if ((iov == NULL) || (iovcnt < 0) || (iovcnt > IOV_MAX)) {
		task_unlock(t, FLG_RD);
		errno = EINVAL;
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
		return;
	}

	for (n = 0; n < iovcnt; n++) {
		if (iov[n].iov_base == NULL) {
			task_unlock(t, FLG_RD);
			errno = EINVAL;
			if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
			return;
		}
		if (iov[n].iov_len > (SIZE_MAX - rdv_buflen)) {
			task_unlock(t, FLG_RD);
			errno = EOVERFLOW;
			if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
			return;
		}
		rdv_buflen += iov[n].iov_len;
	}

	if (rdv_buflen == 0) {
		task_unlock(t, FLG_RD);
		errno = EINVAL;
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
		return;
	}

	t->rdv_iov = iov;
	t->rdv_bufpos = 0;
	t->rdv_cb = rdv_cb;
	t->rdv_iovcnt = iovcnt;
	t->rdv_buflen = rdv_buflen;
	t->rd_cb_data = rd_cb_data;
	t->rd_expires_in_us = expires_in_us;
	t->rd_expires_modified = 1;
	task_atomic_store(&t->rd_state, TASK_READ_STATE_VECTOR);

	if (lockless_worker(t->worker) && (t->io_depth < TASK_MAX_IO_DEPTH)) {
		ssize_t result;

		t->io_depth++;
		task_unlock(t, FLG_RD);
		if ((result = task_read_vector(t, true, false)) == 0) {
			return;		// Operation is queued
		}
		t->rd_expires_modified = 0;
		task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
		if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) && (t->rd_cancel == 0) && rdv_cb) {
			rdv_cb(tfd, iov, iovcnt, result, rd_cb_data);
		}
		return;
	}
	if (lockless_worker(task_atomic_load(&t->worker))) {
		t->io_depth = 0;
	}

	queued = task_notify_action(t, FLG_RD, false, true);
	if (!queued) {
		t->rd_expires_modified = 0;
		task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
	}
	task_unlock(t, FLG_RD);
	if (!queued) {
		errno = EOWNERDEAD;
		if (rdv_cb) rdv_cb(tfd, iov, iovcnt, -1, rd_cb_data);
	}
} // TASK_socket_readv



void
TASK_socket_read(int64_t tfd, void *rdbuf, size_t buflen, int64_t expires_in_us, void *rd_cb_data,
		void (*rd_cb)(int64_t tfd, void *rdbuf, ssize_t result, void *rd_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_RD);
	bool queued;

	if (t == NULL) {
		if (rd_cb) rd_cb(tfd, rdbuf, -1, rd_cb_data);
		return;
	}

	if (t->rd_shut) {
		task_unlock(t, FLG_RD);
		errno = EPIPE;
		if (rd_cb) rd_cb(tfd, rdbuf, -1, rd_cb_data);
		return;
	}

	if (t->rd_cancel) {
		task_unlock(t, FLG_RD);
		errno = EOWNERDEAD;
		if (rd_cb) rd_cb(tfd, rdbuf, -1, rd_cb_data);
		return;
	}

	if (t->fd < 0) {
		task_unlock(t, FLG_RD);
		errno = EBADF;
		if (rd_cb) rd_cb(tfd, rdbuf, -1, rd_cb_data);
		return;
	}

	if (buflen == 0) {
		task_unlock(t, FLG_RD);
		errno = EINVAL;
		if (rd_cb) rd_cb(tfd, rdbuf, -1, rd_cb_data);
		return;
	}

	t->rd_bufpos = 0;
	t->rd_cb = rd_cb;
	t->rd_buf = rdbuf;
	t->rd_buflen = buflen;
	t->rd_cb_data = rd_cb_data;
	t->rd_expires_in_us = expires_in_us;
	t->rd_expires_modified = 1;
	task_atomic_store(&t->rd_state, TASK_READ_STATE_BUFFER);

	if (lockless_worker(t->worker) && (t->io_depth < TASK_MAX_IO_DEPTH)) {
		ssize_t result;

		t->io_depth++;
		task_unlock(t, FLG_RD);
		if ((result = task_read_buffer(t, true, false)) == 0) {
			return;		// Operation is queued
		}
		t->rd_expires_modified = 0;
		task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
		if ((task_atomic_load(&t->state) == TASK_STATE_ACTIVE) && (t->rd_cancel == 0) && rd_cb) {
			rd_cb(tfd, rdbuf, result, rd_cb_data);
		}
		return;
	}
	if (lockless_worker(task_atomic_load(&t->worker))) {
		t->io_depth = 0;
	}

	queued = task_notify_action(t, FLG_RD, false, true);
	if (!queued) {
		t->rd_expires_modified = 0;
		task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
	}
	task_unlock(t, FLG_RD);
	if (!queued) {
		errno = EOWNERDEAD;
		if (rd_cb) rd_cb(tfd, rdbuf, -1, rd_cb_data);
	}
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



int
TASK_migrate(int64_t to_tfd, int64_t from_tfd)
{
	struct task *from_t = task_lookup(from_tfd, FLG_MG);
	struct worker *tw;
	int64_t to_tfdi = TFD_TO_INDEX(to_tfd);

	if (from_t == NULL) return -1;	// errno already set

	// Validate the full handle, not just the pool index.  Without this a
	// handle from another instance (or a stale generation) silently selects
	// whichever task happens to occupy that index.
	if (to_tfd < 0) {
		task_unlock(from_t, FLG_MG);
		errno = ERANGE;
		return -1;
	}
	struct instance *i = from_t->worker->instance;
	struct task *to_t;

	if ((uint32_t)TFD_TO_INSTANCE(to_tfd) != (uint32_t)i->ti) {
		task_unlock(from_t, FLG_MG);
		errno = EINVAL;
		return -1;
	}
	if (to_tfdi >= i->tfd_pool_size) {
		task_unlock(from_t, FLG_MG);
		errno = ERANGE;
		return -1;
	}
	to_t = i->tfd_pool + to_tfdi;
	if ((task_atomic_load(&to_t->tfd) != to_tfd) ||
	    (task_atomic_load(&to_t->state) != TASK_STATE_ACTIVE)) {
		task_unlock(from_t, FLG_MG);
		errno = EBADF;
		return -1;
	}
	tw = to_t->worker;
	if (tw == NULL) {
		task_unlock(from_t, FLG_MG);
		errno = EBADF;
		return -1;
	}
	if (from_t->worker == tw) {
		task_unlock(from_t, FLG_MG);
		errno = EALREADY;
		return -1;
	}

	// Listener tasks own a per-worker distribution socket.  Moving only the
	// parent would leave its old worker without a listener and give the target
	// worker two, so listener migration is not supported.
	if (task_atomic_load(&from_t->type) == TASK_TYPE_LISTEN) {
		task_unlock(from_t, FLG_MG);
		errno = EINVAL;
		return -1;
	}

	// Claim the migration until the worker consumes the notification.
	// meta_flags only serialises the API call itself; active_flags survives
	// until the worker is done and stops a second TASK_migrate() overwriting
	// preferred_worker while the first migration is still in flight.
	if (task_atomic_fetch_or(&from_t->active_flags, FLG_MG) & FLG_MG) {
		task_unlock(from_t, FLG_MG);
		errno = EINPROGRESS;
		return -1;
	}

	from_t->stride->preferred_worker = tw;
	bool queued = task_notify_action(from_t, FLG_MG, false, true);
	if (!queued) {
		from_t->stride->preferred_worker = NULL;
		// task_notify_action() may not have reached its own rollback path
		// (for example if the task's worker disappeared), so make sure the
		// in-flight migration claim is always released on failure.
		task_atomic_and(&from_t->active_flags, ~(uint32_t)FLG_MG);
	}
	task_unlock(from_t, FLG_MG);
	if (!queued) {
		errno = EOWNERDEAD;
		return -1;
	}
	return 0;
} // TASK_migrate



// Closes the TFD and makes the callback to the task's close cb if the user registered one
// This close action is always queued, and not immediate as it allows everything that is
// already in the notification queue to drain first
int
TASK_close(int64_t tfd)
{
	register struct task *t = task_lookup(tfd, FLG_CL);

	if (t == NULL) return -1;	// errno already set

	// The task is already marked DESTROY and cancelling by task_lookup().
	// Cancel all timers too; the owner will do the actual teardown.
	task_atomic_store(&t->tm_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
	task_atomic_store(&t->rd_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);
	task_atomic_store(&t->wr_tt.expiry_us, (int64_t)TIMER_TIME_CANCEL);

	// Close is allocation-free and always deferred to the owning worker.
	// The lookup holds the TFD lock, so the transition/enqueue is atomic with
	// respect to task_maybe_free() and other close requests.
	(void)task_defer_close_locked(t, TFD_TO_INDEX(task_atomic_load(&t->tfd)));
	task_unlock(t, FLG_CL);
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

	res = shutdown(t->fd, how);
	if (res == 0) {
		if (how == SHUT_RD) {
			t->rd_shut = true;
		} else if (how == SHUT_WR) {
			t->wr_shut = true;
		} else if (how == SHUT_RDWR) {
			t->rd_shut = true;
			t->wr_shut = true;
		}
	}
	int err = errno;
	task_unlock(t, FLG_SD);
	errno = err;
	return res;
} // TASK_socket_shutdown



// Listen for and accept new connections on a given addr
int
TASK_socket_listen(int64_t tfd, void *accept_cb_data, void (*accept_cb)(int64_t tfd, void *accept_cb_data))
{
	register struct task *t = task_lookup(tfd, FLG_LI);
	register struct worker *w;
	register struct instance *i;

	if (t == NULL) return -1;	// errno already set

	// A migration already claimed or in flight can move t->worker and the
	// listener list entry from under us.  Refuse the listen rather than race
	// worker-side migration.  TASK_migrate() cannot start a new migration
	// while we hold the TFD lock.
	if (task_atomic_load(&t->active_flags) & FLG_MG) {
		task_unlock(t, FLG_LI);
		errno = EINPROGRESS;
		return -1;
	}

	w = t->worker;
	i = w->instance;

	// A listener must be a fresh socket: no IO may already be in flight.
	if (task_atomic_load(&t->in_epoll) || (task_atomic_load(&t->rd_state) != TASK_READ_STATE_IDLE) ||
	    (task_atomic_load(&t->wr_state) != TASK_WRITE_STATE_IDLE) || (task_atomic_load(&t->fd) < 0)) {
		task_unlock(t, FLG_LI);
		errno = EBUSY;
		return -1;
	}

	// Retrieve the local address before mutating any listener state, so a
	// failure here leaves the task exactly as it was.
	struct sockaddr_storage addr;
	socklen_t addrlen = sizeof(addr);
	if (getsockname(task_atomic_load(&t->fd), (struct sockaddr *)&addr, &addrlen) < 0) {
		int err = errno;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	// SO_REUSEADDR/SO_REUSEPORT are normally set by TASK_socket_bind()
	// before bind().  Re-apply them here for externally-bound registered
	// sockets, but do not fail the listen if a family doesn't support them
	// (e.g. AF_UNIX).
	int val = 1;
	(void)setsockopt(task_atomic_load(&t->fd), SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
	(void)setsockopt(task_atomic_load(&t->fd), SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));

	// Start the listen before publishing listener state.  If this fails the
	// task remains a normal IO task and can be retried or closed cleanly.
	if (listen(task_atomic_load(&t->fd), TASK_LISTEN_BACKLOG) < 0) {
		int err = errno;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	// Commit the listener state only after all fallible setup succeeded.
	task_atomic_store(&t->type, TASK_TYPE_LISTEN);
	task_atomic_store(&t->rd_state, TASK_READ_STATE_LISTEN);
	t->listen_child = 0;
	t->stride->accept_cb = accept_cb;
	t->stride->accept_cb_data = accept_cb_data;
	i->is_server = true;
	if (i->flags & TASK_FLAGS_AFFINITY_FORCE) {
		i->all_cpus_seen = true;
	}
	t->addrlen = addrlen;
	memcpy(&t->stride->addr, &addr, addrlen);

	// Listeners do not have IO timeouts.  Make sure of it
	t->rd_expires_modified = 0;

	// Arm the parent listener before publishing it to the other workers or
	// adding it to the poll list.  This keeps a failed listen from accepting
	// connections that would then have to be rolled back.
	if (task_raise_event_flag(t, EPOLLIN) < 0) {
		int err = errno;

		// Roll the parent back to a plain IO task before dropping FLG_LI.
		task_atomic_store(&t->type, TASK_TYPE_IO);
		task_atomic_store(&t->rd_state, TASK_READ_STATE_IDLE);
		t->stride->accept_cb = NULL;
		t->stride->accept_cb_data = NULL;
		task_unlock(t, FLG_LI);
		errno = err;
		return -1;
	}

	// The listener is fully initialised and armed.  Add it to the worker's
	// poll list only now, so a poll-list walk cannot observe a half-built or
	// failed listener.
	pthread_mutex_lock(&i->listeners_lock);
	t->task_next = w->listeners;
	w->listeners = t;
	pthread_mutex_unlock(&i->listeners_lock);

	// Now apply the listener to all IO workers.
	instance_listen_balance(t);

	// Release the FLG_LI flag.  The TASK_READ_STATE_LISTEN flag protects us now
	task_unlock(t, FLG_LI);
	return 0;
} // TASK_socket_listen



// Connect to given destination address. If src_addr is NULL, it will just use the default interface IP and choose any local source port
void
TASK_socket_connect(int64_t tfd, struct sockaddr *addr, socklen_t addrlen, int64_t expires_in_us,
		   void *connect_cb_data, void (*connect_cb)(int64_t tfd, int result, void *connect_cb_data))
{
	register struct task *t;
	register struct instance *i;

	if ((t = task_lookup(tfd, FLG_CO)) == NULL) {
		if (connect_cb) connect_cb(tfd, -1, connect_cb_data);	// errno already set
		return;
	}

	// Validate the address arguments before copying them into the task.
	// task_stride->addr is a sockaddr_storage and is followed by callback
	// pointers, so an oversized addrlen would corrupt task state.
	if ((addr == NULL) || (addrlen < sizeof(sa_family_t)) ||
	    (addrlen > sizeof(t->stride->addr))) {
		task_unlock(t, FLG_CO);
		errno = EINVAL;
		if (connect_cb) connect_cb(tfd, -1, connect_cb_data);
		return;
	}

	if (t->fd < 0) {
		task_unlock(t, FLG_CO);
		errno = EBADF;
		if (connect_cb) connect_cb(tfd, -1, connect_cb_data);
		return;
	}

	// Handle affinity stuff
	i = t->worker->instance;
	i->is_client = true;
	if (i->flags & TASK_FLAGS_AFFINITY_FORCE) {
		i->all_cpus_seen = true;
	}

	// Save the fields we are about to mutate so a queue failure can roll the
	// task back to a plain IO task instead of leaving it stuck in CONNECT
	// with an unqueued operation.
	struct task_stride *stride = t->stride;
	uint32_t old_type = task_atomic_load(&t->type);
	uint32_t old_wr_state = task_atomic_load(&t->wr_state);
	socklen_t old_addrlen = t->addrlen;
	int64_t old_expires = t->wr_expires_in_us;
	bool old_modified = t->wr_expires_modified;
	void *old_cb_data = task_atomic_load(&stride->connect_cb_data);
	void (*old_cb)(int64_t, int, void *) = task_atomic_load(&stride->connect_cb);
	struct sockaddr_storage old_addr = stride->addr;

	// Convert this task to connect type
	stride->connect_cb = connect_cb;
	task_atomic_store(&t->type, TASK_TYPE_CONNECT);
	t->addrlen = addrlen;
	t->wr_expires_in_us = expires_in_us;
	t->wr_expires_modified = true;
	stride->connect_cb_data = connect_cb_data;
	task_atomic_store(&t->wr_state, TASK_WRITE_STATE_CONNECT);
	memcpy(&stride->addr, addr, addrlen);

	// Queue the connect so it starts on the correct worker
	bool queued = task_notify_action(t, FLG_CN, false, true);
	if (!queued) {
		task_atomic_store(&t->type, old_type);
		task_atomic_store(&t->wr_state, old_wr_state);
		t->addrlen = old_addrlen;
		t->wr_expires_in_us = old_expires;
		t->wr_expires_modified = old_modified;
		task_atomic_store(&stride->connect_cb_data, old_cb_data);
		task_atomic_store(&stride->connect_cb, old_cb);
		stride->addr = old_addr;
	}
	task_unlock(t, FLG_CO);
	if (!queued) {
		errno = EOWNERDEAD;
		if (connect_cb) connect_cb(tfd, -1, connect_cb_data);
	}
	return;
} // TASK_socket_connect



int
TASK_socket_bind(int64_t tfd, struct sockaddr *addr, socklen_t addrlen)
{
	register struct task *t;

	if ((t = task_lookup(tfd, FLG_BND)) == NULL) {
		// errno already set
		return -1;
	}

	// Validate the address before handing it to the kernel.  Relying on
	// bind() to reject a bogus addrlen is not portable across families.
	if ((addr == NULL) || (addrlen < sizeof(sa_family_t)) ||
	    (addrlen > sizeof(struct sockaddr_storage))) {
		task_unlock(t, FLG_BND);
		errno = EINVAL;
		return -1;
	}

	// REUSEADDR/REUSEPORT must be set before bind() or the listener
	// balancing children may fail to bind to the same address.  Ignore
	// unsupported socket families (e.g. AF_UNIX).
	int val = 1;
	(void)setsockopt(t->fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
	(void)setsockopt(t->fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));

	// Bind to the given address
	int ret = bind(t->fd, addr, addrlen);
	int err = (ret < 0) ? errno : 0;
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
	task_atomic_store(&t->stride->close_cb, close_cb);
	task_atomic_store(&t->stride->close_cb_data, close_cb_data);
	task_unlock(t, FLG_CCB);
	return 0;
} // TASK_socket_set_close_cb



// Creates a new socket within the Task Library.  Returns an abstract descriptor to the socket
int64_t
TASK_socket_create(int32_t ti, int domain, int type, int protocol, void *close_cb_data,
		  void (*close_cb)(int64_t tfd, void *close_cb_data))
{
	struct instance *i;
	struct task *t;
	int sock;
	int64_t result = -1;

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
		errno = EINVAL;
		goto out;
	}

	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		goto out;
	}

	// Only create sockets for supported domains
	switch (domain) {
	case AF_INET:
	case AF_INET6:
	case AF_UNIX:
		break;
	default:
		errno = EINVAL;
		goto out;
	}

	// Create a socket.  Must be non-blocking so we always turn that on here.
	// SOCK_CLOEXEC prevents library-owned sockets leaking into exec()'d
	// children.
	if ((sock = socket(domain, type | SOCK_NONBLOCK | SOCK_CLOEXEC, protocol)) < 0) {
		goto out;
	}

	if (sock_set_nonblocking(sock) < 0) {
		close(sock);
		goto out;
	}
	if (sock_set_sndbuf(i, sock) < 0) {
		perror("sock_set_sndbuf");
	}
	if (sock_set_rcvbuf(i, sock) < 0) {
		perror("sock_set_rcvbuf");
	}

	if ((t = task_create(i, TASK_TYPE_IO, sock, NULL, close_cb_data, close_cb, true)) == NULL) {
		close(sock);
		goto out;
	}
	result = t->tfd;

out:
	task_instance_unref((uint32_t)ti);
	return result;
} // TASK_socket_create



int64_t
TASK_socket_register(int32_t ti, int sock, void *close_cb_data,
		    void (*close_cb)(int64_t tfd, void *close_cb_data))
{
	struct instance *i;
	struct task *t;
	int64_t result = -1;

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
		errno = EINVAL;
		goto out;
	}

	if ((instance_state_t)__atomic_load_n(&i->state, __ATOMIC_SEQ_CST) ==
	    INSTANCE_STATE_SHUTTING_DOWN) {
		errno = EOWNERDEAD;
		goto out;
	}

	if (sock_set_nonblocking(sock) < 0) {
		goto out;
	}
	if (sock_set_sndbuf(i, sock) < 0) {
		perror("sock_set_sndbuf");
	}
	if (sock_set_rcvbuf(i, sock) < 0) {
		perror("sock_set_rcvbuf");
	}

	// task_create() returns an ACTIVE task ready for use.
	if ((t = task_create(i, TASK_TYPE_IO, sock, NULL, close_cb_data, close_cb, false)) == NULL) {
		goto out;
	}
	t->registered_fd = true;	// Mark it as registered so it isn't closed by task_do_close_cb()
	result = t->tfd;
	errno = 0;

out:
	task_instance_unref((uint32_t)ti);
	return result;
} // TASK_register_fd

