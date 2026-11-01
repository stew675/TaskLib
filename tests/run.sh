#!/bin/sh
set -e
cd "$(dirname "$0")"
CC=${CC:-gcc}
SRC="../task_core.c ../task_worker.c ../task_io.c ../task_timer.c ../ph.c"
INC="-I.."
FLAGS="-O1 -g -Wall -Wextra -Wno-unused-parameter $INC ${EXTRA_CFLAGS:-}"
for t in test_locked_actions test_cross_instance test_migrate test_migrate_live test_timer_base test_blocking_work test_listener_close test_listen_failure test_task_recycle test_migrate_timers test_instance_destroy_api_race test_vector_io test_stale_tfd test_create_shutdown_race test_shutdown_blocking; do
	printf '%-24s ' "$t"
	$CC $FLAGS "$t.c" $SRC -o "/tmp/tasklib_$t" -lpthread
	"/tmp/tasklib_$t"
	echo ok
done
