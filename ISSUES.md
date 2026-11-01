# TaskLib Correctness and Scalability Remediation Plan

Status: complete
Owner: next implementation session
Scope: `task_core.c`, `task_worker.c`, `task_io.c`, `task_timer.c`, `task_internal.h`,
`task_lib.h`, `Makefile`, `tests/`, `benchmarks/`, `README.md`

This document is the authoritative work plan for the issues found in the most recent
review. Work through it phase by phase. Do not treat individual fixes as isolated
patches: the recurring failures have all been in one area, the lifetime/close/timer
machine, and they recur because that machine has several independent entry points that
each enforce only part of the lifetime rules.

---

## 1. Why previous fix cycles kept finding new "critical" issues

The failures are not independent. They share one root cause:

- There is no single close path. `TASK_close()`, `TASK_timeout_destroy()`,
  `task_queue_close()`, `worker_handle_task_event()` HUP handling,
  `task_handle_listen_event_fail()`, `task_shutdown_listen_children()`,
  `instance_queue_shutdown()`, and `worker_cleanup()` all have their own close logic.
- Some paths queue a close; some perform it inline. The inline paths break the
  assumption that a task slot cannot be recycled while the current epoll/poll batch
  still contains a raw pointer to it.
- Epoll dispatch stores raw task pointers in `epoll_event.data.ptr` and never validates
  them against a generation. If a slot is recycled, a stale event can be delivered to
  the new task.
- Timer callbacks are not gated by the same close barrier as IO callbacks. A timer
  callback can still run after `TASK_timeout_destroy()` has returned.
- Close currently depends on notification-node allocation. When allocation fails, a
  task can be left in `DESTROY` with no close queued and no way to retry.
- Tests have mostly asserted functional success. They have not asserted the temporal
  invariants: "no callback after the close barrier", "no free during an event batch",
  "close always eventually runs", "no stale epoll event is delivered".

The plan below centralises close/recycle, adds generation-tagged epoll dispatch, adds
explicit invariants and assertions, and adds tests that fail if those invariants are
violated.

---

## 2. Non-negotiable invariants

These must hold after every phase. Any patch that weakens one must be rejected or the
invariant must be deliberately redefined and documented.

1. **Single owner for IO state**
   The task's owning IO worker is the only thread allowed to touch its epoll
   registration, timer heap/colt-list membership, socket fd, or invoke its IO callbacks.
   Foreign API threads may mutate atomics / fields under the TFD lock and enqueue work;
   they must not perform epoll/timer/fd teardown.

2. **Pinning**
   A task slot may not be recycled while any of the following references exist:
   - `api_refs > 0` (public API call in flight)
   - `notifyqlen > 0` or `notifyqlen_locked > 0` (queued notification)
   - `active_flags != 0` (including `FLG_CL` for a pending close)
   - `rd_state`/`wr_state` not `IDLE`
   - `closed == 0` while state is `DESTROY` (close not completed)
   - a timer node is linked in a heap/colt list
   - the current epoll event-dispatch loop holds a `task_ref`
   - a listener poll snapshot holds a `task_ref`

3. **Single recycle point**
   Only `task_maybe_free()` may transition `DESTROY -> FREEING` and call `task_free()`.
   `task_nuke()` is the only shutdown exception, and only after every worker is dead
   and every API reference has drained.

4. **No free/recycle during dispatch**
   No task may be freed or recycled synchronously from:
   - inside `worker_do_io_epoll()` or `worker_handle_task_event()`
   - inside `worker_poll_listeners()` or a listener callback
   - inside an API call's callback from a foreign thread
   All closes are deferred to a safe point after the current event/poll/notify batch.

5. **Close is allocation-independent**
   Requesting a close must not depend on notification-node allocation. A task marked
   `DESTROY` must always be on a path that will eventually run close teardown.

6. **Close barrier**
   `close_cb` is the last callback for a task. Once close teardown starts, no IO,
   accept, connect, or timer callback may start. After `close_cb` returns, no further
   callback for that task may run. Asynchronous timer/IO destruction APIs are requests;
   user context may only be freed after `close_cb` (or the instance `shutdown_cb` during
   shutdown, where close callbacks are intentionally suppressed).

7. **Stale event safety**
   Every epoll event and listener poll snapshot must be validated against the current
   task generation before use. A stale event for a recycled slot must be dropped, not
   delivered to the new occupant.

8. **Timer callback revalidation**
   Before invoking a timer callback, the worker must re-check under the TFD lock that
   the task is still `ACTIVE`, not cancelled, and that the timer expiry still matches
   the value that was selected. A callback already past this check may be in flight;
   the close barrier from invariant 6 is the user-visible synchronisation point.

9. **No lifecycle wait under the global lifecycle lock**
   `TASK_instance_destroy()` and `TASK_instance_wait()` must not hold `creation_lock`
   while waiting for workers or API references. Instance-slot reservation must be
   separate from teardown waiting.

10. **Lifecycle calls from an instance's own worker**
    `TASK_instance_wait()`/`TASK_instance_destroy()` called from a callback running on
    that instance's own worker must fail with `EDEADLK` rather than deadlock.

11. **Field access discipline**
    A field touched by more than one thread is either `_Atomic`, protected by the TFD
    lock, or protected by the documented owner/lock for that field. Add comments for
    intentional race-tolerant loads. New plain shared fields require justification.

12. **Lock order**
    Document and preserve the lock order below. Do not introduce a reverse edge.

    ```
    creation_lock (slot reservation only; never across waits)
        -> instance->lock (worker state lists)
        -> tfd_lock (hash-sharded task lock)
        -> worker->lock (notification/close queues)
        -> listeners_lock
        -> cpulock
    ```

    The TFD lock may be held while taking `worker->lock` (existing close path does
    this). `worker->lock` must be released before taking the TFD lock in a drain path.
    `listeners_lock` must never be held while taking the TFD lock or `worker->lock`.

13. **Notification OOM policy**
    Non-close actions may fail with a documented callback error if no notification
    node is available. Close must not fail for that reason. The implementation must
    make that distinction explicit, not implicit.

---

## 3. Issue inventory

Status values: `OPEN`, `IN PROGRESS`, `DONE`, `DEFERRED` (with rationale).

| ID | Severity | Area | Issue | Primary code | Phase | Status |
| --- | --- | --- | --- | --- | --- | --- |
| H1 | High | Read/EOF | `EPOLLRDHUP`/`EPOLLHUP` sets `rd_shut` before reading, losing buffered data | `worker_handle_task_event`, `task_read_buffer`, `task_read_vector` | 2 | DONE |
| H2 | High | Instance create | Failed `TASK_instance_create()` closes fd 0 because `evfd` starts at 0 | `instance_create`, `instance_destroy` | 1 | DONE |
| H3 | High | Sizing | `max_tasks` uint32 arithmetic overflows and silently succeeds | `instance_tfd_pool_init` | 1 | DONE |
| H4 | High | Scalability | Per-instance `tfd_pool_used` atomic caps allocator scaling | `task_get_free_task`, `task_free`, `task_nuke` | 7 | DONE |
| H5 | High | Timers | Timer callback can run after `TASK_timeout_destroy()` returns | `worker_check_timeouts`, `task_do_timeout_cb` | 6 | DONE |
| H6 | High | Close/lifetime | Inline close breaks deferred-destruction; stale epoll events and poll-snapshot UAF | close paths, `worker_do_io_epoll`, `worker_poll_listeners` | 3, 4 | DONE |
| H7 | High | Lifecycle | `creation_lock` held across waits; callback lifecycle call deadlocks | `TASK_instance_destroy`/`wait`, `creation_lock` | 5 | DONE |
| H8 | High | Close/OOM | Close allocation failure leaves task permanently `DESTROY` with no close queued | `task_notify_action`, `TASK_close`, standalone `TASK_timeout_destroy` | 3 | DONE |
| M1 | Medium | Affinity | Odd CPU count scan misses one CPU and can mark affinity complete prematurely | `set_one_workers_affinity` | 1 | DONE |
| M2 | Medium | Affinity | Affinity fast path can select a dead/shutting worker | `get_affined_worker_from_direction` | 1 | DONE |
| M3 | Medium | Affinity | Latent NULL dereference in `tw == NULL` branch | `task_set_initial_preferred_worker` | 1 | DONE |
| M4 | Medium | Eventfd | Eventfd closed from error/wait paths can race notifiers and be reused | `worker_handle_event`, `instance_wait_workers` | 1 | DONE |
| M5 | Medium | Shutdown | `shutdown_cb` invoked before fields clear; reentrant destroy can UAF | `TASK_instance_wait` | 1 | DONE |
| M6 | Medium | API validation | iov total-length sum can overflow | `TASK_socket_readv`/`writev` | 1 | DONE |
| M7 | Medium | Connect | Connect failure does not roll back type/callback/address mutations | `TASK_socket_connect` | 1 | DONE |
| M8 | Medium | Register | `TASK_socket_register()` changes external fd buffer sizes; docs omit it | `TASK_socket_register` | 1 | DONE |
| M9 | Medium | API contract | Callbacks can run before the initiating API returns; undocumented | listen/timeout/IO paths | 8 | DONE |
| M10 | Medium | Performance | Owner-queued actions self-wake via eventfd | `task_notify_action` owner branch | 1 | DONE |
| M11 | Medium | Timer/lifetime | `task_free()` mutates timer state from foreign threads; relies on close having run | `task_free` | 3 | DONE |
| M12 | Medium | Start | `TASK_instance_start()` ignores worker-start failures | `TASK_instance_start` | 1 | DONE |
| L1 | Low | Portability/security | Global SIGPIPE change; missing CLOEXEC on instance evfd, epoll, sockets, accepted sockets | `TASK_instance_create`, `worker_create`, socket/accept paths | 1/8 | DONE |
| L2 | Low | Hygiene | Mutex attributes not destroyed; dead fields; stale "locked" comments | multiple | 1 | DONE |
| L3 | Low | Bind | `TASK_socket_bind()` relies on kernel addrlen validation | `TASK_socket_bind` | 1 | DONE |
| L4 | Low | Docs | Threading-model text conflicts with `max_blocking_workers == 0` section | README | 8 | DONE |
| L5 | Low | Docs/API | Timer tasks have no documented close-callback barrier API | `task_lib.h`, README | 6 | DONE |
| L6 | Low | Tests | No TSan target; no teardown/timer/RDHUP/OOM regression tests | `Makefile`, `tests/` | 0 | DONE |

---

## 4. Core design decisions

These decisions are the intended implementation. If a phase reveals a concrete reason
they cannot work, stop, record the reason in this file, and get agreement before
changing the design.

### 4.1 Central deferred close queue

Replace all inline close paths and all close notifications with one per-worker pending
close queue.

- Add to `struct worker` a close-list head, e.g.:

  ```c
  _Atomic uint32_t close_head;   /* tfdi index; TASK_CLOSE_EMPTY = 0xffffffff */
  ```

  Place it on a cold line or document its cache-line placement in the existing
  `_Static_assert` layout.

- Use `task->free_next` as the close-list link only while the task is in state
  `DESTROY` with `FLG_CL` set. It is not used by the free list until the task is
  recycled, and is not used by listeners (those use `task_next`). Document this
  dual use next to both fields.

- Add `task_defer_close(struct task *t)` and
  `task_defer_close_locked(struct task *t, uint32_t tfdi)`:

  ```text
  under tfd_lock:
      if state is UNUSED or FREEING: fail
      if state is ACTIVE: state=DESTROY; set rd/wr shut+cancel; closed=0
      set tm_tt.expiry_us = TIMER_TIME_CANCEL; tm_cancelled = 1
      if state is FREEING or closed != 0: already closed; fail/skip
      if active_flags already has FLG_CL: already queued; return true
      set FLG_CL
      w = t->worker
      if w == NULL: leave state DESTROY; return false (shutdown nuke will reap)
      under w->lock:
          t->free_next = w->close_head
          w->close_head = tfdi
      unlock
      worker_notify(w)  /* may be skipped when caller is w; but harmless */
      return true
  ```

  The TFD lock serialises close requests against `task_do_close_cb()` and prevents the
  "closed just became 1" race.

- Add `worker_process_closes(struct worker *w)`:

  ```text
  for a bounded batch:
      under w->lock: swap w->close_head to EMPTY
      for each tfdi in the local list:
          t = &i->tfd_pool[tfdi]
          next = t->free_next
          if t->tfd changed or state not DESTROY: drop
          if t->worker != w: forward tfdi to t->worker's close queue; continue
          if !(active_flags & FLG_CL): continue
          task_do_close_cb(t)   /* removes listener, closes fd, destroys timers, cb, drops FLG_CL */
          restore iteration to next saved before close
  ```

  The link must be read before `task_do_close_cb()` because close may eventually
  recycle the slot and overwrite `free_next`.

- Call `worker_process_closes(w)` in `worker_loop_io()` after
  `worker_process_notifyq()` and after `worker_poll_listeners()`. Call it during
  `worker_cleanup()` too, so a worker that dies with pending closes drains them.
- Make `get_next_epoll_timeout_ms()` return 0 if `w->close_head != EMPTY`, so the worker
  does not sleep with pending closes.
- Route every close request through this queue:
  - `TASK_close()`
  - `TASK_timeout_destroy()` for standalone `TASK_TYPE_TIMER` tasks. For
    socket-backed tasks this API must keep its existing "cancel timer only"
    behaviour and use `FLG_TM`; it must **not** close the socket.
  - `task_queue_close()`
  - `instance_queue_shutdown()`
  - `task_shutdown_listen_children()`
  - `task_handle_listen_event_fail()`
  - `worker_handle_task_event()` error/HUP with no pending IO
  - `task_do_accept_cb()` when it rejects an accepted task
  - `worker_cleanup()` residual actions
  - `worker_process_one_action()` FLG_CL forwarding (compatibility only)
- Remove the inline fallback from `task_queue_close()`, `TASK_close()`, and
  `TASK_timeout_destroy()`. Close no longer allocates a `struct ntfyq`, so there is no
  OOM fallback and no inline teardown.
- Keep `FLG_CL` as the pending/in-progress close pin. `task_action_done(t, FLG_CL)` at
  the end of `task_do_close_cb()` clears it and allows recycling.
- Flush any existing `FLG_CL` notification entries in `worker_process_notifyq()` into
  `task_defer_close()`; after the transition, new code should never enqueue FLG_CL.
- Assert in debug builds:
  - `close_head == EMPTY` before `worker_destroy()` frees the worker (or explicit
    logging that pending closes are being left to `instance_nuke_tasks()`).
  - A task is not simultaneously on the free list and the close list.
  - `task_free()` is never called with `FLG_CL` set.

### 4.2 TFD-tagged epoll events

Raw pointers in epoll events are not generation-safe. Use the TFD as the event cookie.

- Define a sentinel for the worker eventfd, e.g. `TASK_EPOLL_EVENTFD = TFD_NONE`
  (`UINT64_MAX`). A valid TFD never has bit 63 set, so this is unambiguous.
- For task registrations, set `ev.data.u64 = (uint64_t)t->tfd` in:
  - `task_create_event_flag()`
  - `task_raise_event_flag()`
  - `task_lower_event_flag()`
  - `worker_handle_task_migration()`
- In `worker_create()`, register the worker eventfd with
  `ev.data.u64 = TASK_EPOLL_EVENTFD`.
- In `worker_do_io_epoll()`:

  ```text
  uint64_t cookie = w->events[n].data.u64;
  if (cookie == TASK_EPOLL_EVENTFD) { worker_handle_event(...); continue; }
  idx = TFD_TO_INDEX(cookie);
  if (idx >= i->tfd_pool_size) continue;
  t = &i->tfd_pool[idx];
  task_ref(t);                                  /* pin before validating */
  if (task_atomic_load(&t->tfd) != (int64_t)cookie) {
      task_unref(t); continue;                  /* stale event for recycled slot */
  }
  if (state == ACTIVE && in_epoll) worker_handle_task_event(t, revents);
  task_unref(t);
  ```

  `task_ref()` before the TFD check preserves the same stale-reference safety as
  `task_lookup()`; `task_init()` preserves `api_refs`.
- Keep the existing `task_ref` around event dispatch.
- Add a test hook or white-box test that feeds a stale cookie and asserts the event is
  dropped.

### 4.3 Listener poll snapshots

Even with deferred close, harden the snapshot:

- In `worker_poll_listeners()`, while holding `listeners_lock`, take `task_ref()` for
  each task copied into `w->poll_tasks`.
- Release each `task_unref()` after its `task_handle_listen_event()` call.
- If the snapshot array is full, release refs for entries not processed.
- Do not walk `w->listeners` outside the lock or after callbacks.

### 4.4 Timer callback barrier

The recommended semantics are:

- `TASK_timeout_cancel()` and `TASK_timeout_destroy()` are asynchronous requests.
- The timeout callback may be in flight when those calls return.
- The close callback is the barrier that guarantees no further callback for the task.
- `TASK_timeout_destroy()` on a standalone timer must therefore be documented as
  "request destroy; the task is freed after close processing". On a socket-backed task
  it remains a timer cancel and does not close the socket. Provide/document a way
  to set a close callback on a timer task. Add
  `TASK_timeout_set_close_cb(int64_t tfd, void *data, void (*cb)(int64_t, void *))` as a
  thin wrapper over `TASK_socket_set_close_cb()` (or document the latter explicitly for
  timer tasks; a named wrapper is clearer).
- Revalidate under the TFD lock in `task_do_timeout_cb()` before invoking `FLG_TM`:

  ```text
  tfd_lock
  if state != ACTIVE: unlock; return
  if tt->expiry_us != timeout_us: unlock; return
  if tm_cancelled: unlock; return
  tm_cancelled = 1
  capture cb/data
  tfd_unlock
  invoke cb
  ```

  This removes the large TOCTOU window. A callback already past this point remains
  covered by the close barrier.
- Apply the same expiry/state revalidation to `FLG_RT`/`FLG_WT` where practical; the IO
  callback wrappers already check cancel/state, but matching expiry avoids firing an
  old timeout against a newer operation.
- Add a stress test that creates zero-delay timers, sets a close_cb, destroys them, and
  asserts that after close_cb runs no timeout callback can run. Also assert that
  `TASK_timeout_destroy()` no longer causes a callback after the close barrier.
- Update README: timer/IO destruction is asynchronous; close_cb is the safe point.

If a synchronous `TASK_timeout_destroy()` is later required, implement it as a separate
opt-in API (or an explicit `_sync` variant), not by silently blocking inside the
existing call.

### 4.5 Instance lifecycle slot reservation

Stop holding `creation_lock` across waits.

- Introduce a reserved sentinel, e.g.:

  ```c
  #define INSTANCE_SLOT_BUSY ((struct instance *)(uintptr_t)1)
  ```

- `task_lookup()` and every instance-index API must check for the sentinel before
  dereferencing. Treat it as `EBADF`/`EOWNERDEAD` as appropriate.
- `TASK_instance_create()`:
  1. Lock `creation_lock`; find `instances[ti] == NULL`; set it to
     `INSTANCE_SLOT_BUSY`; unlock.
  2. Allocate/initialise the instance and workers without the global lock.
  3. On failure, lock, set slot `NULL`, unlock, free.
  4. On success, lock, publish `instances[ti] = i`, unlock.
- `TASK_instance_destroy()`:
  1. Lock `creation_lock`; validate the slot and instance state; change
     `instances[ti]` to `INSTANCE_SLOT_BUSY` so no new API can find it; unlock.
  2. Drain API refs, queue closes, stop/join/reap workers, nuke tasks, free the
     instance without the global lock.
  3. Lock, set the slot `NULL`, unlock.
- `TASK_instance_wait()`, `TASK_instance_shutdown()`, and `TASK_instance_start()` must
  treat the busy sentinel as "instance is being destroyed" and fail cleanly.
- Detect lifecycle calls from the instance's own worker:
  - if `__thr_current_worker != NULL` and
    `__thr_current_worker->instance == i`, return `-1` with `errno = EDEADLK` for
    `TASK_instance_wait()` and `TASK_instance_destroy()`.
  - `TASK_instance_shutdown()` from the own worker is allowed (it does not wait).
- Remove the scan-for-instance-and-clear logic from `instance_destroy()`; the caller
  owns slot release.
- Add a regression test where a worker/timer callback calls `TASK_instance_create()`
  while another thread is in `TASK_instance_destroy()`; it must complete, not hang.

### 4.6 Scalability counters

- `tfd_pool_used`: first remove the advisory 80% check and the counter updates entirely
  (the free-list inventory already bounds the pool). If a hard cap is still required,
  use a sharded counter aligned to cache lines and only sum it on the slow check path.
  Do not leave a single seq_cst RMW on every allocation/free.
- `w->num_tasks`: change create/free/migration updates to relaxed atomics. If
  contention remains visible, use per-worker/sharded counts or sample two workers
  (power-of-two choices) instead of scanning every worker on every task creation.
- Consider raising `TASK_FREE_SHARDS` to 64 (power of two) or sizing it to the CPU count;
  16 heads will contend on large machines.
- Keep `TASK_API_REF_SHARDS` sharded; record that it is 64-way and degrades above that
  many distinct API threads.
- Re-run `benchmarks/run_micro.sh` and `benchmarks/run_http.sh`; record new result files
  and update `benchmarks/results/README.md`. Do not overwrite historical results.
- Only after correctness is stable, consider cache-line separation of producer-written
  fields (`api_refs`, `notifyqlen`, queue tails) from owner-read state.

### 4.7 OOM policy

- Close: never depends on allocation; deferred close queue uses no nodes.
- Non-close actions: may fail; APIs must leave the task in a retryable state and invoke
  their error callback. Specifically, `TASK_timeout_set()` must not leave fields
  modified with no `FLG_TM` queued; either reserve before mutating or restore on
  failure.
- `TASK_close()`/`TASK_timeout_destroy()` must not return failure after transitioning a
  task to an unrecoverable `DESTROY` with no close route.

---

## 5. Implementation phases

Each phase must end with:

```sh
make
make check
make check-asan
```

and with a `git commit` containing the phase changes and tests. Do not combine phases
in one commit unless a later phase is impossible to build otherwise.

### Phase 0: Baseline, tests, and harness

Goal: make the current failures visible before changing behaviour.

Tasks:

- Read `README.md`, `task_internal.h`, `task_lib.h`, and the four implementation files
  completely. Read `stewticket_thr.h` and `ph.c/ph.h` at least for lifetime rules.
- Run `make`, `make check`, `make check-asan`; record the baseline.
- Run `benchmarks/run_micro.sh` and one short HTTP run; save baseline numbers outside
  the repo or in a `baseline/` ignored file.
- Add `make check-tsan` to the `Makefile`:
  `EXTRA_CFLAGS="-fsanitize=thread -fno-omit-frame-pointer" ./tests/run.sh`.
  Expect noise; make it a diagnostic target, not a hard gate until triaged.
- Add failing regression tests (names are suggestions):
  - `tests/test_rdhup.c` (H1)
  - `tests/test_timer_destroy_barrier.c` (H5/L5)
  - `tests/test_instance_create_failure.c` (H2)
  - `tests/test_max_tasks.c` (H3)
  - `tests/test_iov_overflow.c` (M6; may use an internal helper)
  - `tests/test_lifecycle_callback.c` (H7, with a timeout wrapper)
  - `tests/test_close_from_callback.c` (H6/H8)
- Add tests to `tests/run.sh` only once they pass; until then run them manually and
  record the expected failure.
- Add a debug-only invariant checker (e.g. `task_assert_lifetime(t)` behind
  `#ifdef TASK_DEBUG_LIFETIME`) that verifies pin counts/states at `task_maybe_free`
  and `task_do_close_cb`.

Acceptance:

- New tests fail for the documented reason, not because of test bugs.
- Baseline test and benchmark results recorded.
- `ISSUES.md` status for Phase 0 marked done.

### Phase 1: Isolated correctness and hygiene fixes

Goal: fix low-coupling issues without touching the central close design yet.

Tasks:

- H2: initialise `i->evfd = -1` immediately after `memset()` in `instance_create()`.
  Audit every other fd/lifecycle field initialised only at the end of a constructor.
- H3: use checked `size_t`/`uint64_t` arithmetic for pool sizing; reject oversized
  `max_tasks` with `EINVAL`/`ENOMEM` rather than wrapping. Validate worker counts.
- M6: checked iov total sum; reject overflow with `EINVAL`/`EOVERFLOW` before mutating
  task state.
- M7: save/restore connect type, callbacks, addr, addrlen on queue failure; or reserve
  queue capacity before mutating.
- M5: clear `shutdown_cb`/`shutdown_data` before invoking in `TASK_instance_wait()`;
  document no recursive lifecycle calls from the callback.
- M4: stop closing `evfd`s in `worker_handle_event()` and
  `instance_wait_workers()`/`TASK_instance_wait()` error paths. Set state to shutting
  down and let `worker_destroy()`/`instance_destroy()` close after joins/reaping.
- M10: skip `worker_notify(w)` when queuing to the owner's own queue; the owner will
  check `notifyq_head` before blocking. (For close queue, revisit in Phase 3.)
- M1/M2/M3: fix affinity:
  - scan all CPUs on odd `num_cpus` (or use `(num_cpus + 1) / 2`);
  - filter worker states in the affinity selection path;
  - guard the `tw == NULL` branch in `task_set_initial_preferred_worker()`.
- M12: handle `worker_start()` failures in `TASK_instance_start()`; do not leave a
  partly running instance silently. At minimum return the error and ensure destroy can
  clean up.
- M11: add an assertion/comment in `task_free()` that `closed != 0` implies timer
  nodes are already detached; or move timer destruction exclusively into close and
  assert it.
- L1: add `EFD_CLOEXEC` to the instance eventfd, `EPOLL_CLOEXEC` to
  `epoll_create1()`, `SOCK_CLOEXEC` to created sockets, and use `accept4()` with
  `SOCK_NONBLOCK|SOCK_CLOEXEC`. Document the process-wide SIGPIPE ignore. Handle
  `accept4` failure as a normal accept failure (no fd leak).
- L3: validate `addr`/`addrlen` in `TASK_socket_bind()` similarly to connect.
- L2: destroy `pthread_mutexattr_t`, remove/annotate dead fields
  (`max_tcp_mem`, `worker_offset`, `num_io_workers_affined`, `cpuinfo.worker_pos`),
  fix stale comments ("task is already locked", "unlocks the task").
- M8: document that `TASK_socket_register()` makes the fd nonblocking and applies
  instance send/receive buffer sizes. If that is not desired, add an opt-out flag; do
  not silently change existing behaviour without docs.

Tests:

- `test_instance_create_failure.c`: use `RLIMIT_AS` and a large max_tasks; after failure
  assert fds 0/1/2 are still open.
- `test_max_tasks.c`: `UINT32_MAX` returns `-1`/`EINVAL`, not success.
- `test_iov_overflow.c`: overflow rejected; no callback with a bogus positive result.
- Extend `test_locked_actions.c` with connect failure rollback if practical.
- Run all existing tests + ASan.

Acceptance:

- All Phase 1 tests pass.
- No new warnings.
- `git commit`.

### Phase 2: RDHUP/HUP read data loss

Goal: preserve buffered data on peer half-close/close.

Tasks:

- In `worker_handle_task_event()`:
  - Do not set `rd_shut` before calling the read handler.
  - For `EPOLLRDHUP`, lower the RDHUP/IN event flags, then call the read handler if a
    read is pending; let `read()` return data, then `0` for EOF.
  - For `EPOLLERR|EPOLLHUP`, still force the write side to observe the error, but if a
    read is pending, attempt the read first (or at least do not pre-set `rd_shut`).
  - If no read is pending, record the EOF hint without poisoning a future read. A
    future read must be allowed to drain data that was already queued.
- Ensure `task_read_buffer()`/`task_read_vector()` only set `rd_shut` when the
  underlying `read()`/`readv()` returns 0, not on an event hint.
- Keep the `EPOLLRDHUP`/`EPOLLHUP` event lowering correct for ONESHOT/ET.
- Add `tests/test_rdhup.c`:
  - pending read, peer writes 5 bytes then `shutdown(SHUT_WR)`;
    callback result == 5; next read result == -1/EPIPE.
  - pending read, peer writes 5 bytes then `close()`;
    callback result == 5; next read -1/EPIPE.
  - no pending read, data+FIN already queued; read returns 5 then -1/EPIPE.
- Add a TCP variant if the test harness can bind loopback; socketpair is acceptable
  for the kernel event semantics but TCP is closer to the real use case.

Acceptance:

- All three cases pass.
- Existing tests + ASan pass.
- `git commit`.

### Phase 3: Central deferred close queue

Goal: eliminate inline close, notification allocation for close, and the OOM-stuck
`DESTROY` state.

Tasks:

- Implement the design in section 4.1.
- Update `struct worker` layout and `_Static_assert`s.
- Add `task_defer_close()` / `task_defer_close_locked()` and
  `worker_process_closes()`.
- Route every close path listed in 4.1. Remove the inline fallbacks from
  `task_queue_close()`, `TASK_close()`, and `TASK_timeout_destroy()`.
- Flush legacy `FLG_CL` notification entries into the deferred queue.
- Ensure `task_do_close_cb()` is only called from `worker_process_closes()` and
  `worker_cleanup()` (and `task_nuke()` at shutdown).
- Ensure `worker_cleanup()` drains both the close queue and the notification queues;
  no worker exits with a live close pinned by `FLG_CL`.
- Add assertions:
  - `FLG_CL` is not set after successful close completion.
  - A task on the close list has `state == DESTROY`, `closed == 0`, and is not on the
    free list.
  - `task_maybe_free()` never sees a pending close.
- Add comments near `free_next` and `task_next` explaining the dual uses.

Tests:

- Extend `test_listener_close.c`, `test_migrate_timers.c`,
  `test_create_shutdown_race.c`, and add `test_close_from_callback.c`:
  - close from read/write/accept/timer callbacks;
  - close during migration;
  - close a listener while it has children and pending events;
  - close many tasks concurrently from foreign threads.
- Add a debug build test that forces the old notification allocation to fail (via a
  test hook) and asserts close still eventually runs.
- Run existing tests + ASan.
- Run a stress loop (e.g. `for i in 1..20; do ./tests/run.sh; done`).

Acceptance:

- No inline `task_do_close_cb` calls remain in event/API paths.
- Close never returns `-1` for allocation reasons.
- A task marked `DESTROY` is always eventually closed or nuked at shutdown.
- `git commit`.

### Phase 4: TFD-tagged epoll events and poll-snapshot refs

Goal: make stale event delivery impossible (except after the documented generation
wrap).

Tasks:

- Implement section 4.2:
  - Replace `.data.ptr = t` with `.data.u64 = (uint64_t)t->tfd` everywhere a task is
    registered.
  - Register the worker eventfd with `TASK_EPOLL_EVENTFD`.
  - Update `worker_do_io_epoll()` to decode the cookie, pin the slot with `task_ref`,
    validate `t->tfd`, then dispatch.
  - Keep the existing state/in_epoll checks.
- Implement section 4.3 for `worker_poll_listeners()`.
- Add a white-box test (including `task_internal.h`) that:
  - registers a task, closes it, waits for recycle, and feeds the old cookie through
    the dispatch validation helper; assert it is dropped;
  - optionally exercises migration re-registration to ensure the cookie changes with
    the generation.
- Run benchmarks: the decode + atomic validation should be negligible.

Acceptance:

- No raw task pointer is used from `epoll_event.data`.
- Stale-cookie test drops the event.
- All tests, ASan, and a TSan triage pass.
- `git commit`.

### Phase 5: Instance lifecycle slot reservation

Goal: remove the global-lock-across-waits deadlock class.

Tasks:

- Implement section 4.5.
- Update every `instances[ti]` load in `task_lookup()`, `TASK_instance_*`,
  `TASK_socket_create()`, `TASK_timeout_create()`, `TASK_socket_register()`, and
  `TASK_do_blocking_work()`.
- Add `EDEADLK` detection for `TASK_instance_wait()`/`TASK_instance_destroy()` from the
  instance's own worker.
- Ensure `TASK_instance_create()` failure paths release the slot.
- Ensure `instance_destroy()` no longer scans/clears `instances[]`.
- Update README threading model: lifecycle calls are serialised by the application;
  calls from the instance's own worker return `EDEADLK`; other lifecycle calls during
  destruction return `EBADF`/`EOWNERDEAD`.

Tests:

- `test_lifecycle_callback.c`: worker/timer callback calls `TASK_instance_create()`
  while another thread destroys; it must complete (no hang).
- `test_lifecycle_from_worker.c`: `TASK_instance_destroy(ti)` from a callback on `ti`
  returns `-1`/`EDEADLK`, not deadlock.
- Extend `test_instance_destroy_api_race.c` and `test_create_shutdown_race.c`.
- Add a timeout wrapper in `run.sh` so a hang fails the suite rather than blocking CI.

Acceptance:

- No `pthread_mutex_lock(&creation_lock)` remains across worker/API waits.
- Callback-create-during-destroy test completes.
- All tests + ASan pass.
- `git commit`.

### Phase 6: Timer callback revalidation and close barrier

Goal: make timer cancellation/destruction semantics explicit and eliminate the large
TOCTOU window.

Tasks:

- Implement section 4.4.
- Revalidate state/expiry/cancel under the TFD lock in `task_do_timeout_cb()`.
- Keep `TASK_timeout_destroy()` on a socket-backed task as a timer cancel; only a
  standalone `TASK_TYPE_TIMER` task is destroyed via the deferred close queue.
- Add `TASK_timeout_set_close_cb()` (or document `TASK_socket_set_close_cb()` for timer
  tasks) and use it in tests.
- Update README:
  - `TASK_timeout_cancel()`/`TASK_timeout_destroy()` are asynchronous requests.
  - A timeout callback may be in flight when they return.
  - `close_cb` is the barrier; free user context only after it runs.
  - For standalone timers, show how to set the close callback.
- Add `tests/test_timer_destroy_barrier.c`:
  - create zero-delay timer with close_cb;
  - destroy immediately;
  - wait for close_cb;
  - after close_cb, sleep/yield and assert timeout callback count did not increase;
  - stress 100k+ iterations and assert no callback after the barrier.
- Run the existing timer/migration tests.

Acceptance:

- The stress test no longer observes callbacks after the close barrier.
- No synchronous destroy API is promised unless explicitly implemented.
- All tests + ASan pass.
- `git commit`.

### Phase 7: Scalability

Goal: restore near-linear allocator/API scaling and reduce needless contention.

Tasks:

- Remove the `tfd_pool_used` advisory cap and atomic updates, or replace with a sharded
  counter. Measure with `benchmarks/run_micro.sh`.
- Relax/relax+shard `num_tasks` updates; measure.
- Consider increasing `TASK_FREE_SHARDS` to 64 or CPU-scaled.
- Remove owner self-notification if not already done.
- Evaluate `worker_select_io_worker()` O(workers) scan; if it shows up, use
  power-of-two-choices sampling rather than a full scan. Keep the algorithm
  deterministic enough for tests.
- Re-run micro and HTTP benchmarks; add new result files and update
  `benchmarks/results/README.md` without overwriting the historical baseline.
- Do not trade correctness for speed: all tests and stress runs must pass after each
  change.

Acceptance:

- Alloc microbenchmark per-thread throughput no longer collapses with thread count.
- No HTTP throughput regression.
- All tests + ASan pass.
- `git commit`.

### Phase 8: Documentation, TSan, and final validation

Goal: leave the repo in a state where the same class of bugs cannot silently return.

Tasks:

- Update README:
  - callback-before-return contract;
  - asynchronous destroy/cancel and close barrier;
  - timer close callback usage;
  - `TASK_socket_register()` behaviour;
  - SIGPIPE and CLOEXEC behaviour;
  - max_tasks/worker limits;
  - lifecycle calls from callbacks (`EDEADLK`/unsupported);
  - correct `max_blocking_workers == 0` wording;
  - updated threading/lock model and invariants.
- Add `make check-tsan` and triage results. Fix real races; annotate or document
  intentional ones. Do not claim TSan-clean if it is not.
- Run:
  - `make`
  - `make check`
  - `make check-asan`
  - `make check-tsan` (at least one run; record known limitations)
  - the full test suite repeatedly (e.g. 20 iterations) to catch flakiness
  - `benchmarks/run_micro.sh`
  - `benchmarks/run_http.sh`
- Update `ISSUES.md` status table. Every issue must be `DONE` or `DEFERRED` with a
  rationale and a follow-up note.
- Update `benchmarks/results/README.md` with the new results and commit.

Acceptance:

- All issues in the inventory are resolved or explicitly deferred.
- All standard tests plus ASan pass.
- TSan results are documented.
- Benchmarks recorded.
- Final commit.

---

## 6. Test matrix

| Test / command | Purpose | Required after |
| --- | --- | --- |
| `tests/test_rdhup.c` | Half-close/HUP data preservation | Phase 2 |
| `tests/test_timer_destroy_barrier.c` | No timer callback after close barrier | Phase 6 |
| `tests/test_instance_create_failure.c` | Failed create does not close fds | Phase 1 |
| `tests/test_max_tasks.c` | No sizing overflow | Phase 1 |
| `tests/test_iov_overflow.c` | No iov length wrap | Phase 1 |
| `tests/test_lifecycle_callback.c` | No lifecycle deadlock from callbacks | Phase 5 |
| `tests/test_lifecycle_from_worker.c` | Explicit `EDEADLK` instead of self-wait | Phase 5 |
| `tests/test_close_from_callback.c` | Deferred close from all callback types | Phase 3 |
| `tests/test_epoll_cookie.c` | Stale epoll cookie dropped after reuse | Phase 4 |
| `tests/test_vector_io.c` | Existing vector IO behavior preserved | every phase |
| `tests/test_migrate_timers.c` | Migration/timer lifetime preserved | every phase |
| `tests/test_create_shutdown_race.c` | Create/destroy races | Phase 5 |
| `tests/test_instance_destroy_api_race.c` | API refs during destroy | Phase 5 |
| `tests/test_shutdown_blocking.c` | Blocking worker shutdown | every phase |
| `make check-asan` | Memory errors/leaks | every phase |
| `make check-tsan` | Data races (diagnostic) | Phase 4/8 |
| stress loops of `tests/run.sh` | Flakiness/races | Phase 3+ |
| `benchmarks/run_micro.sh` | Allocator/API scaling | Phase 7/8 |
| `benchmarks/run_http.sh` | End-to-end regression | Phase 7/8 |

Add a timeout wrapper around every test invocation in `tests/run.sh` (e.g.
`timeout 60 ...`) so future hangs fail fast.

---

## 7. Documentation changes

Update `README.md` in the same phase as the behavior change:

- Callbacks may be invoked before the initiating API returns.
- `TASK_timeout_cancel()`/`TASK_timeout_destroy()` are asynchronous requests; close_cb
  is the barrier.
- How to set a close callback on a timer task.
- `TASK_socket_register()` modifies blocking mode and buffer sizes.
- Close is asynchronous, close callbacks are suppressed during shutdown, and
  `shutdown_cb` is the per-instance cleanup point.
- Lifecycle calls from the instance's own worker return `EDEADLK`; lifecycle calls must
  not be made from callbacks except where documented.
- SIGPIPE ignore is process-wide; fds are CLOEXEC by default for library-created fds.
- Limits/validation for `max_tasks` and worker counts.
- Update the threading-model bullet for `max_blocking_workers == 0`.

---

## 8. Definition of done

- [x] All high-priority issues are fixed, with regression tests.
- [x] All medium/low issues are fixed or explicitly deferred with rationale.
- [x] No inline task teardown remains in event/API/poll paths.
- [x] Close does not allocate and cannot leave a permanent `DESTROY` task.
- [x] Epoll events are TFD-tagged and validated; poll snapshots are referenced.
- [x] `TASK_instance_destroy()`/`wait()` do not hold `creation_lock` while waiting.
- [x] Timer callbacks revalidate state/expiry; close barrier is tested.
- [x] `make`, `make check`, `make check-asan` pass; TSan results documented.
- [x] Micro and HTTP benchmarks recorded with no correctness regressions.
- [x] README and ISSUES.md are accurate and consistent.
- [x] Final commit exists.

---

## 9. Progress log

Use this section as the implementation session works.

```text
Phase 0: [x] baseline recorded; regression tests and check-tsan target added
Phase 1: [x] isolated fixes
Phase 2: [x] RDHUP/HUP
Phase 3: [x] deferred close queue
Phase 4: [x] epoll cookies + poll refs
Phase 5: [x] lifecycle slot reservation
Phase 6: [x] timer barrier/revalidation
Phase 7: [x] scalability
Phase 8: [x] docs/TSan/final validation
```

Baseline (pre-change): `make`, `make check`, and `make check-asan` all passed
the historical 15-test suite.  The Phase 0 regression tests were then added;
before their fixes, `test_max_tasks`, `test_iov_overflow`,
`test_instance_create_failure`, and `test_rdhup` failed for the documented
reasons, and `test_create_shutdown_race` was observed hanging/flaking.

Final validation (Phase 8):

- `make` passes.
- `make check` passes all 24 tests.
- `make check-asan` passes all 24 tests.
- `make check-tsan` passes all 24 tests.  TSan initially found real
  cross-thread cancellation-field writes and a bulk-reset race on `free_next`;
  both were fixed (atomic cancellation fields and atomic `free_next` reset).
  The remaining TSan findings were test-synchronisation bugs in
  `test_blocking_work` and `test_rdhup` (plain reads / closing a registered fd
  before the close callback).
- Stress: `test_create_shutdown_race` repeated 15x, with and without ASan;
  `test_close_from_callback` repeated 10x.  All passed.
- Benchmarks: `benchmarks/results/micro_phase7.txt` and
  `benchmarks/results/http_phase7.txt`; HTTP throughput is within variance of
  the previous final result, and allocator throughput no longer collapses with
  thread count.

### Deviations / decisions recorded during implementation

1. **HUP with no pending read** (Phase 2): rather than queueing an immediate
   close when `EPOLLERR|EPOLLHUP` arrives with no read pending, the task is left
   ACTIVE with the read side usable.  This preserves the requirement that a
   future read can drain queued data.  The write side is still shut immediately.
   Proven by `tests/test_rdhup.c` (prefilled data + FIN/close cases).
2. **Unstarted-worker close list** (Phase 3): a worker created but never
   started has no thread to drain its deferred-close list.  `worker_destroy()`
   asserts the list is empty only for workers that actually ran; for unstarted
   workers the stale index list is discarded and `instance_nuke_tasks()` reaps
   the slots.  This is the shutdown exception to the single-recycle invariant.
3. **API admission gate** (Phases 3/5): `task_api_slot_open[]` blocks new
   instance-entry references before the reference drain.  Without it a stream
   of racing create/close calls can starve `instance_wait_api_refs()`.  The
   gate is process-lifetime state and is never dereferenced as an instance.
4. **IO timeout revalidation** (Phase 6): the same state/expiry/cancel
   revalidation is applied to `FLG_RT`/`FLG_WT`, not only `FLG_TM`.
5. **`tfd_pool_used`** (Phase 7): the field is retained for source
   compatibility but is no longer updated; the free-list inventory is the
   authoritative bound.
6. **`TASK_timeout_set_close_cb()`** was added as a thin wrapper over
   `TASK_socket_set_close_cb()` so standalone timer cleanup has a named API.
