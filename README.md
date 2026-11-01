# TaskLib

A high-performance, multi-threaded, event-driven IO library.

(c) 2019 - Stew Forster <stew675@gmail.com>

## Building

TaskLib has no third-party dependencies. Its lock-free atomic primitives are
implemented in `task_atomics.h` on top of the compiler's `__atomic` builtins,
which are available in GCC 4.7+ and Clang 3.1+. The sources also use C11
_Atomic/_Static_assert/aligned_alloc, so build with a C11-capable compiler. A
64-bit C compiler, pthreads
and a Linux/glibc userspace (epoll, eventfd, futex, `SO_INCOMING_CPU`) are
required. Timer keys are absolute monotonic microsecond timestamps stored in
`void *` keys by the paired heap, so 32-bit pointer builds are rejected at
compile time rather than silently mis-ordering timers:

```sh
make
```

This builds the `echoes` and `load_gen` examples. The sanitizer and profiling
variants are opt-in because they need a working ASan or profiling runtime:

```sh
make asan
make prof
make check       # regression tests for the task-lifetime/concurrency fixes
make check-asan  # the same regression tests built with AddressSanitizer
```

### Source layout

| File | Contents |
| --- | --- |
| `task_lib.h` | Public API |
| `task_internal.h` | Private types, cache-line layout, shared prototypes |
| `task_core.c` | Task pool, TFD lookup/lifetime, notifications, workers, instances |
| `task_worker.c` | Worker loops, epoll dispatch, notify queues, migration |
| `task_io.c` | Socket/IO/connect/listen APIs and their callbacks |
| `task_timer.c` | Timer cool-off lists, paired-heap integration, timeout API |
| `task_atomics.h` | `__atomic` wrappers |
| `stewticket_thr.h` | Adaptive futex ticket lock |
| `ph.c` / `ph.h` | Paired heap |

Benchmarks and recorded comparisons against the last pre-fix revision live in
`benchmarks/`; see `benchmarks/README.md`.

## Features

- An easy to use, high-performance, event-driven asynchronous IO API.
- A fully event-driven, multi-threaded framework.  Public APIs may be called
  from any thread; operations that are not issued by the owning worker are
  queued to it so all task state and epoll manipulation has a single owner.
- Socket connect and IO calls with automated timeout handling.
- Fully distributed connection acceptance across IO workers.
- A lightweight, fast timer queue built on the paired heap library at
  `git@github.com:stew675/Paired-Heap-Library.git`.
- Multiple worker threads, or automatic worker count selection.
- Automatic CPU detection with optional worker CPU affinity.
- Load balancing of tasks across CPUs and workers.
- Optional task migration, so related tasks can share a worker.
- Microsecond resolution timing.
- Multiple independent instances within one process.

## Conventions

### Task descriptors

Every task is identified by an opaque 64-bit `tfd` (task file descriptor). A
`tfd` is produced by `TASK_socket_create()`, `TASK_socket_register()`,
`TASK_timeout_create()`, or by the listener accept callback. Pass it back to the
library unchanged. Do not construct or modify it.

A `tfd` is valid from creation until the task is closed or destroyed. Stale
handles are rejected with `EBADF` or `EOWNERDEAD`. The same value is passed to
callbacks, so it can be compared against the value stored by the application.
The generation field is 27 bits. Generation counters are retained across
instance destruction/recreation, so a handle can eventually alias a new task
after more than 134 million reuses of the same pool slot.

### Limits and process-wide effects

- At most `TASK_MAX_INSTANCES` (16) instances may exist in a process.
- `max_tasks` is validated with checked 64-bit arithmetic. Oversized values are
  rejected with `EINVAL`/`ENOMEM` instead of wrapping to a small pool.
- `num_workers_io` and `max_blocking_workers` must be non-negative. `0` IO
  workers selects an automatic count; `0` blocking workers means
  `TASK_do_blocking_work()` runs synchronously on the caller.
- The library ignores `SIGPIPE` process-wide the first time an instance is
  created. Install application signal handling before creating an instance if a
  different disposition is required.
- Library-created file descriptors (sockets, epoll, eventfd, accepted sockets)
  are `CLOEXEC` by default. Externally registered fds are not modified beyond
  making them non-blocking and applying buffer sizes.

### Return values and errors

Functions that return `int` return `0` on success and `-1` on failure, with
`errno` set. Functions that return `int64_t` return a `tfd` (or a time value) on
success and `-1` on failure, with `errno` set.

The read, write, readv, writev, and connect calls return `void`. Their result is
reported to the supplied callback. If the operation cannot even be started, the
callback is invoked with `-1` and `errno` set, when a callback is supplied.

### Callback results

Socket IO callbacks receive a `result` value:

- `result > 0`: that many bytes were transferred. For reads, the operation may
  have stopped early because the socket would block, reached EOF, or used its
  per-scheduling-slice IO budget. Call the read again for more data. For writes,
  this means the supplied buffer was consumed in full, or that an error occurred
  after a partial write and the remainder must be retried.
- `result < 0`: the operation failed, timed out, or the task was shut down.
  `errno` is set (`ETIMEDOUT`, `EPIPE`, `EOWNERDEAD`, etc.).

IO callbacks are not invoked with `result == 0`.

Callbacks may run before the API call that initiated the operation returns.
This can happen when the call is made from the owning worker and completes
inline, when validation fails, or when another thread's worker processes the
operation immediately. Do not assume that the call has returned before its
callback runs, and do not hold an application lock across an API call if the
callback will take the same lock.

The connect callback receives `1` on success and `-1` on failure, with `errno`
set. The accept callback receives a new `tfd` on success and `-1` if the library
could not allocate a task for the connection. Accepted connections do not
inherit the listener's `close_cb` or `accept_cb`; set a per-connection close
callback from the accept callback if one is required.

### Timeouts

Most IO calls take an `expires_in_us` value:

- Negative values mean "no timeout" (`TASK_TIMEOUT_NEVER` / `TASK_TIMEOUT_NONE`).
- `0` means expire immediately (`TASK_TIMEOUT_ASAP` / `TASK_TIMEOUT_NOW`).
- Positive values are microseconds from now. Values larger than one year are
  capped at one year.

When a timeout fires, the callback receives `result == -1` and `errno` is set to
`ETIMEDOUT`. The operation is cancelled.

Timer and IO cancellation/destruction are requests, not barriers. A callback
may already have been selected when the request is made. The task close
callback (`close_cb`, or the instance `shutdown_cb` during shutdown) is the
synchronisation point after which no further callback for that task may start.
Close waits for in-flight task APIs that already acquired the task and, for a
listener, for every child listener to finish teardown before invoking
`close_cb`. A call that is made after the handle is already closed/stale may
report an error without an ordering relationship to `close_cb`. Do not free
user context from `TASK_timeout_cancel()`/`TASK_timeout_destroy()` alone.

### Threading model

- Task APIs that take a `tfd` can be called from any thread. The library
  resolves the owning instance from the `tfd`. Asynchronous IO actions are
  routed to the owning worker; `bind`, `listen`, `shutdown`, `get_fd` and the
  timer-control calls execute synchronously on the caller while holding the
  task's TFD lock.
- Instance lifecycle calls (`TASK_instance_create()`,
  `TASK_instance_start()`, `TASK_instance_shutdown()`,
  `TASK_instance_wait()`, `TASK_instance_destroy()`) must not race with each
  other for the same `ti`. The application must serialize them.
  Creation-style APIs that take a `ti` are instance-entry references;
  `TASK_instance_shutdown()` and `TASK_instance_destroy()` drain them before
  scanning the task pool and stopping workers. `TASK_instance_wait()` drains
  remaining API references before reaping workers. The global instance-slot
  lock is only used to reserve/publish/retire a slot, never while waiting for
  workers or API references.
- `TASK_instance_wait()` and `TASK_instance_destroy()` called from a callback
  running on that instance's own worker fail with `EDEADLK`. This includes IO
  workers, blocking workers (`work_func`), and callbacks invoked by
  `TASK_instance_wait()`. Other lifecycle calls that find the instance already
  shutting down or being destroyed fail with `EBADF`/`EOWNERDEAD` rather than
  waiting. Do not call lifecycle APIs from callbacks unless the call is
  documented as non-blocking.
- Socket, timer, connect, read, write, and close callbacks run on the task's
  owning worker thread.  If an IO call is made from that worker's thread and
  can complete immediately it runs inline (still on the owning worker).  If an
  operation cannot even be started (invalid arguments, EBADF, EOWNERDEAD), the
  error callback is invoked synchronously on the calling thread.  Do not block
  inside any callback.
- The accept callback runs on the worker that accepted the connection. The new
  task is assigned to its selected worker at creation time, which may be a
  different worker from the one running the callback; it does not subsequently
  migrate from the accepting worker. Accepted tasks do not inherit the
  listener's close or accept callbacks.
- The blocking work callback (`work_cb_func`) runs on the instance notification
  worker, or on a thread blocked in `TASK_instance_wait()`. It does not run on
  the blocking thread.
- `shutdown_cb` runs on the thread that calls `TASK_instance_wait()` or
  `TASK_instance_destroy()`.

### Lifetime and synchronisation model

The lifetime rules are deliberately strict because event dispatch, timers,
listeners, and foreign API callers can all reference the same task:

- A task slot is pinned while any API reference, notification, activity flag
  (including a pending close), IO reservation, timer node, epoll dispatch
  reference, or listener poll-snapshot reference exists.
- `task_maybe_free()` is the only normal recycle point. It runs after close
  teardown has cleared the activity flags and marked the task closed.
- All close requests are deferred to the owning worker's close queue. Close
  does not allocate memory, never runs inline from epoll/poll/foreign callbacks,
  and always leaves the task on a path to eventual teardown (or the shutdown
  nuke pass).
- Epoll events carry a generation-tagged TFD cookie, not a raw task pointer.
  Dispatch pins the slot, revalidates the cookie generation, and drops stale
  events for recycled slots. Listener poll snapshots pin every copied listener
  for the duration of the callback.
- Timer callbacks revalidate task state, armed expiry, and cancellation under
  the TFD lock immediately before invocation. A callback already past that
  check may be in flight; the close callback is the user-visible barrier.

Lock order (do not create a reverse edge):

```text
creation_lock (slot reservation/publish only; never across a wait)
    -> instance->start_lock (created-worker publication/start only)
    -> instance->lock (worker state lists)
    -> tfd_lock (hash-sharded task lock)
    -> worker->lock (notification/close queues)
    -> listeners_lock
    -> cpulock
```

`tfd_lock` may be held while taking `worker->lock`. `worker->lock` is released
before taking `tfd_lock` in drain paths. `listeners_lock` is never held while
taking `tfd_lock` or `worker->lock`. Nested TFD locks are not permitted; code
that must close a child listener after failing to arm it queues that work only
after dropping the parent TFD lock.

---

## Instance API

### `int32_t TASK_instance_create(int num_workers_io, int max_blocking_workers, uint32_t max_tasks, int tcp_sndbuf_size, uint64_t flags)`

Creates a task instance and returns its instance index (`ti`), or `-1` with
`errno` set.

- `num_workers_io`: number of IO workers. `0` selects an automatic count
  (one per three configured CPUs, with a minimum of two).
- `max_blocking_workers`: maximum blocking workers. `0` makes
  `TASK_do_blocking_work()` run synchronously on the caller thread.
- `max_tasks`: used to size the task pool. Must be greater than zero.
- `tcp_sndbuf_size`: per-socket send and receive buffer size request. `0` uses a
  default derived from `max_tasks`. Values are clamped to the range used by the
  library.
- `flags`: a combination of the `TASK_FLAGS_AFFINITY_*` flags described below.

### `int TASK_instance_start(int32_t ti)`

Starts the instance's created workers and assigns the notification worker. This
call returns to the caller. Use `TASK_instance_wait()` to wait for shutdown.

Returns `0`, or `-1` with `errno` set.

### `int TASK_instance_shutdown(int32_t ti, void *shutdown_data, void (*shutdown_cb)(intptr_t ti, void *shutdown_data))`

Requests a graceful shutdown. The instance stops accepting new tasks, waits for
in-flight creation-style APIs to finish publishing their tasks, queues a close
for every live task, and stops its workers. Close callbacks are suppressed
during shutdown.

The `shutdown_cb`, if supplied, is stored and invoked later by
`TASK_instance_wait()` or `TASK_instance_destroy()`.  Close callbacks are
suppressed during shutdown, so per-task application state must be released from
the `shutdown_cb` (or by another application-owned tracker) rather than relying
on `close_cb`.

Returns `0`, or `-1` with `errno` set.

### `int TASK_instance_wait(int32_t ti)`

Waits for the instance to finish shutting down. In-flight API entry references
are drained, workers are reaped, and the stored `shutdown_cb` is invoked. If the
instance was created but never started, this returns immediately. Calling this
from the instance's own worker returns `-1` with `errno == EDEADLK`.

Returns `0`, or `-1` with `errno` set.

### `int TASK_instance_destroy(int32_t ti)`

Destroys an instance. If it is still running or only created, a shutdown is
performed first: live tasks are closed or reaped, workers are stopped and
joined, in-flight task API entry references are drained, and the instance is
freed. This call is safe after `TASK_instance_wait()`. If a `shutdown_cb` is
pending it is invoked after the instance is freed.

Calling this from the instance's own worker returns `-1` with
`errno == EDEADLK`; other lifecycle calls racing the destruction observe
`EBADF`/`EOWNERDEAD`. Do not use `ti` after this call returns success.

Returns `0`, or `-1` with `errno` set.

### Affinity flags

- `TASK_FLAGS_AFFINITY_DISABLE`: disables affinity learning and preferred worker
  selection.
- `TASK_FLAGS_AFFINITY_CPU`: enables learning the CPU of incoming sockets and
  binding IO workers to CPUs.
- `TASK_FLAGS_AFFINITY_NET`: sets `SO_INCOMING_CPU` on sockets as a hint to the
  TCP stack.
- `TASK_FLAGS_AFFINITY_FORCE`: treats all CPUs as already observed, so affinity
  is applied from the first event.
- `TASK_FLAGS_AFFINITY_FORCE_CPU`: `CPU | FORCE`.
- `TASK_FLAGS_AFFINITY_FORCE_ALL`: `CPU | NET | FORCE`.

---

## Timeout API

### `int64_t TASK_get_us_time(int64_t *time_us, bool precise)`

Returns the current monotonic time in microseconds and writes it to `time_us` if
that pointer is non-NULL. A precise value calls `clock_gettime()`. A non-precise
value returns the worker's cached time when called from a worker thread.

### `int64_t TASK_timeout_create(int32_t ti, int64_t expires_in_us, void *timeout_cb_data, void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data))`

Creates a standalone timer task and returns its `tfd`, or `-1` with `errno` set.
The timer is assigned to an IO worker. The callback runs on that worker.

The timer is cancelled after it fires. Call `TASK_timeout_set()` again to re-arm
it. Use `TASK_timeout_destroy()` or `TASK_close()` to free the timer task.

### `int TASK_timeout_set(int64_t tfd, int64_t us_from_now, void *timeout_cb_data, void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *timeout_cb_data))`

Sets or replaces the timeout on a timer task or socket task. Negative durations
become `0` (expire immediately). Durations longer than one year are capped.

Returns `0`, or `-1` with `errno` set.

### `int TASK_timeout_cancel(int64_t tfd)`

Requests cancellation of the timeout callback. For a standalone timer task the
task remains allocated. Returns `0`, or `-1` with `errno` set.

Cancellation is asynchronous: a timeout callback may already be in flight when
this call returns. The close callback is the barrier after which no further
timeout callback for the task may start.

### `int TASK_timeout_destroy(int64_t tfd)`

For a standalone timer task, requests destruction of the task. For a task that
also owns a socket, this behaves like `TASK_timeout_cancel()` and does **not**
close the socket.

Destruction is asynchronous. A timeout callback may still be in flight when
this call returns; the task is freed after its close processing runs. Set a
close callback with `TASK_timeout_set_close_cb()` and treat that callback as the
point after which the task and any user context may be freed.

Returns `0`, or `-1` with `errno` set.

### `int TASK_timeout_set_close_cb(int64_t tfd, void *close_cb_data, void (*close_cb)(int64_t tfd, void *close_cb_data))`

Sets the close callback for a standalone timer task (equivalent to
`TASK_socket_set_close_cb()` for timer tasks). The callback runs exactly once
when the timer task is closed, after all timeout/IO teardown. No timeout
callback may start after it returns. Returns `0`, or `-1` with `errno` set.

### Timeout constants

- `TASK_TIMEOUT_NEVER`, `TASK_TIMEOUT_NONE`: `-1`, no timeout.
- `TASK_TIMEOUT_ASAP`, `TASK_TIMEOUT_NOW`: `0`, expire immediately.
- `TASK_TIMEOUT_ONE_SEC`, `TASK_TIMEOUT_ONE_MINUTE`,
  `TASK_TIMEOUT_ONE_HOUR`, `TASK_TIMEOUT_ONE_DAY`, `TASK_TIMEOUT_ONE_YEAR`.

---

## Socket API

### `int64_t TASK_socket_create(int32_t ti, int domain, int type, int protocol, void *close_cb_data, void (*close_cb)(int64_t tfd, void *close_cb_data))`

Creates a socket owned by the task library and returns its `tfd`, or `-1` with
`errno` set. Supported domains are `AF_INET`, `AF_INET6`, and `AF_UNIX`.

The socket is non-blocking. Send and receive buffer sizes are configured from
the instance settings. `close_cb`, if supplied, is invoked when the task closes
(unless the instance is shutting down or the task is a child listener).
Connections accepted through a listener do not inherit this callback.

### `int64_t TASK_socket_register(int32_t ti, int sock, void *close_cb_data, void (*close_cb)(int64_t tfd, void *close_cb_data))`

Registers an externally created file descriptor with the instance and returns a
`tfd`, or `-1` with `errno` set.

The descriptor is made non-blocking and the instance's configured send and
receive buffer sizes are applied to it. The library does not close it on task
close; it only removes it from the worker's epoll set. The application remains
responsible for the descriptor itself (including closing it after the task's
close callback has run). Use `TASK_socket_create()` if the library should own
the descriptor.

### `int TASK_socket_bind(int64_t tfd, struct sockaddr *addr, socklen_t addrlen)`

Binds the task's socket. `SO_REUSEADDR` and `SO_REUSEPORT` are set before
`bind()` so distributed listeners can share the address. The options are ignored
on socket families that do not support them.

Returns `0`, or `-1` with `errno` set.

### `int TASK_socket_listen(int64_t tfd, void *accept_cb_data, void (*accept_cb)(int64_t tfd, void *accept_cb_data))`

Marks the task as a listener and starts accepting connections. When the instance
has more than one IO worker, additional listener sockets are created so
acceptance is distributed across workers.  Listener arming is done from the
calling thread but the software epoll state is published before the kernel
registration, so an owning worker cannot drop the initial one-shot accept
event.

If the socket is not already bound, `listen()` auto-binds it; the assigned
local address is read afterwards and propagated to all child listeners. If
`accept_cb` is NULL, accepted connections are immediately closed.

Returns `0`, or `-1` with `errno` set. `EINPROGRESS` means a migration for the
task is still in flight; retry after it completes.

Child listener clones bind to the address assigned to the parent. For an
unbound socket this address is read after `listen()` has auto-bound the port,
so all clones share the same port. The listener's `close_cb` is deferred until
all child listeners have closed.

### `void TASK_socket_connect(int64_t tfd, struct sockaddr *addr, socklen_t addrlen, int64_t expires_in_us, void *connect_cb_data, void (*connect_cb)(int64_t tfd, int result, void *connect_cb_data))`

Starts an asynchronous connect. `addr` must be non-NULL and `addrlen` must be
between `sizeof(sa_family_t)` and `sizeof(struct sockaddr_storage)`.

The connect callback receives `1` on success or `-1` on failure, with `errno`
set. A `-1` with `ETIMEDOUT` means the connect timed out. After a successful
connect the task behaves as a regular IO task.

### `int TASK_socket_shutdown(int64_t tfd, int how)`

Calls `shutdown()` on the underlying socket and, only if it succeeds, marks the
matching read and/or write side as shut for the library. Valid values are
`SHUT_RD`, `SHUT_WR`, and `SHUT_RDWR`.

Returns the result of `shutdown()`, or `-1` with `errno` set.

### `int TASK_close(int64_t tfd)`

Requests an asynchronous close for the task. The close callback, if registered,
runs once before the task slot is recycled. Pending IO callbacks are cancelled.
Timers are cancelled and destroyed. `TASK_close()` does not wait for teardown,
and a callback may already be in flight when it returns; `close_cb` is the
barrier after which no further callback for the task may start. Close defers
the callback until APIs that acquired the task have finished and, for a
listener, until all child listeners have closed.

For a task created with `TASK_socket_register()`, the descriptor is removed from
epoll but not closed. For a timeout-only task, the task is freed after close
processing.

Close callbacks are suppressed when the instance is shutting down. During
shutdown, release per-task application state from the instance `shutdown_cb`
instead.

Returns `0`, or `-1` with `errno` set.

### `int TASK_socket_set_close_cb(int64_t tfd, void *close_cb_data, void (*close_cb)(int64_t tfd, void *close_cb_data))`

Sets or replaces the close callback for a task. Returns `0`, or `-1` with
`errno` set.

### `int TASK_socket_get_fd(int64_t tfd)`

Returns the underlying system file descriptor, or `-1` with `errno` set.

### `int TASK_migrate(int64_t to_tfd, int64_t from_tfd)`

Queues a migration so that the `from_tfd` task moves to the same worker as
`to_tfd`. This can reduce cross-thread latency between related tasks.

Returns `0`, or `-1` with `errno` set. `EALREADY` means both tasks already share
a worker. `EINPROGRESS` means another migration for `from_tfd` is still being
completed by its worker. `EINVAL` means `to_tfd` belongs to a different
instance, or that `from_tfd` is a listener task (listener migration is not
supported because per-worker listener distribution would not be preserved);
`EBADF` means the target handle is stale or not active.

---

## IO API

All four IO calls return `void` and report completion through their callback.
The callback is invoked once with a positive byte count or with `-1` and
`errno` set. See "Callback results" above.

The `iov` and buffer arguments must be valid for the duration of the operation.
The caller must keep them alive until the callback runs.

For `readv` and `writev`:

- `iov` must be non-NULL.
- `iovcnt` must be greater than zero and at most `IOV_MAX`.
- Every `iov_base` must be non-NULL.
- The total length must be greater than zero.

For `read` and `write`:

- `buflen` must be greater than zero.
- The buffer must be valid for `buflen` bytes.

Zero-length operations are rejected with `EINVAL` and the callback is invoked
with `-1`.

### `void TASK_socket_writev(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *wr_cb_data, void (*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *wr_cb_data))`

Writes the entire contents of the supplied buffers, or expires trying. The write
continues asynchronously if the socket blocks.

### `void TASK_socket_write(int64_t tfd, const void *buf, size_t buflen, int64_t expires_in_us, void *wr_cb_data, void (*wr_cb)(int64_t tfd, const void *buf, ssize_t result, void *wr_cb_data))`

Writes the entire buffer, or expires trying. The write continues asynchronously
if the socket blocks.

### `void TASK_socket_readv(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *rd_cb_data, void (*rdv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *rd_cb_data))`

Reads into the supplied buffers. The callback may report a partial read if the
socket blocks or reaches EOF. Call again for the remainder.

### `void TASK_socket_read(int64_t tfd, void *buf, size_t buflen, int64_t expires_in_us, void *rd_cb_data, void (*rd_cb)(int64_t tfd, void *buf, ssize_t result, void *rd_cb_data))`

Reads into the supplied buffer. The callback may report a partial read if the
socket blocks or reaches EOF. Call again for the remainder.

At EOF with no data, the callback receives `-1` with `errno == EPIPE`. At EOF
after a partial read, the partial byte count is reported first; the next read
reports `-1` with `EPIPE`.

---

## Blocking Work API

### `int TASK_do_blocking_work(int32_t ti, void *work_data, void (*work_func)(void *work_data), void *work_cb_data, void (*work_cb_func)(int32_t ti, void *work_cb_data))`

Dispatches a blocking operation to a blocking worker, creating a worker if
needed and the limit allows it. When `work_func` completes, `work_cb_func` is
invoked on the instance notification worker.

If `max_blocking_workers` is `0`, `work_func` and `work_cb_func` are called
synchronously on the caller thread.

If the instance has not been started yet, the work is queued on a created worker
and runs when `TASK_instance_start()` is called.

Returns `0`, or `-1` with `errno` set. `EINVAL` means `work_func` is NULL.
`EAGAIN` means the blocking worker limit was reached. `EOWNERDEAD` means the
instance is shutting down.

---

## Debug API

### `int TASK_debug_task(int64_t tfd)`

Dumps task information to `stderr`. Returns `0`, or `-1` with `errno` set.
