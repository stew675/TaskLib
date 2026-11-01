#!/bin/sh
set -e
cd "$(dirname "$0")"
CC=${CC:-gcc}
SRC="../task_core.c ../task_worker.c ../task_io.c ../task_timer.c ../ph.c"
INC="-I.."
FLAGS="-O1 -g -Wall -Wextra -Wno-unused-parameter $INC ${EXTRA_CFLAGS:-}"
TIMEOUT=${TEST_TIMEOUT:-60}
for t in test_locked_actions test_cross_instance test_migrate test_migrate_live test_timer_base test_blocking_work test_listener_close test_listen_failure test_task_recycle test_migrate_timers test_instance_destroy_api_race test_vector_io test_stale_tfd test_create_shutdown_race test_shutdown_blocking test_rdhup test_instance_create_failure test_max_tasks test_iov_overflow test_lifecycle_callback test_lifecycle_from_worker test_close_from_callback test_timer_destroy_barrier test_epoll_cookie test_close_api_ref_barrier test_listener_close_barrier test_accept_close_cb_not_inherited test_unbound_listener test_blocking_lifecycle test_blocking_start_race test_listen_arm_race test_accept_shutdown_race; do
	if [ ! -f "$t.c" ]; then
		continue
	fi
	printf '%-24s ' "$t"
	$CC $FLAGS "$t.c" $SRC -o "/tmp/tasklib_$t" -lpthread
	timeout "$TIMEOUT" "/tmp/tasklib_$t"
	echo ok
done

# Exercise the optional pthread-mutex TFD lock build.  This used to crash in
# instance_destroy() when instance creation failed before allocating tfd_locks.
printf '%-24s ' "test_create_failure_mutex"
$CC $FLAGS -DTASK_NO_TICKET_LOCKS test_instance_create_failure.c $SRC -o /tmp/tasklib_test_create_failure_mutex -lpthread
timeout "$TIMEOUT" /tmp/tasklib_test_create_failure_mutex
echo ok
