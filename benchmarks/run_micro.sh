#!/bin/bash
# TaskLib microbenchmarks.
#
# Usage: ./run_micro.sh [source-dir] [output-file]
#
#   source-dir    TaskLib source tree to build/link against.  Defaults to this
#                 repository.  Pass another checkout/worktree to compare
#                 revisions against the same microbenchmark source.
#   output-file   File to append results to.  Defaults to
#                 benchmarks/results/micro-<timestamp>.txt.
#
# ITERS and REPEAT can be overridden in the environment.
set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
SRC_DIR=${1:-"$ROOT"}
OUT=${2:-"$ROOT/benchmarks/results/micro-$(date +%Y%m%d-%H%M%S).txt"}
CC=${CC:-gcc}
ITERS=${ITERS:-2000000}
REPEAT=${REPEAT:-3}

if [ ! -f "$SRC_DIR/task_internal.h" ]; then
	echo "error: $SRC_DIR does not look like a TaskLib source tree" >&2
	exit 1
fi

BIN=$(mktemp /tmp/tasklib-micro.XXXXXX)
trap 'rm -f "$BIN"' EXIT

echo "building microbenchmark against $SRC_DIR" | tee -a "$OUT"
$CC -O3 -march=native -I"$SRC_DIR" \
	"$SCRIPT_DIR/micro_bench.c" \
	"$SRC_DIR/ph.c" "$SRC_DIR/task_core.c" "$SRC_DIR/task_worker.c" \
	"$SRC_DIR/task_io.c" "$SRC_DIR/task_timer.c" \
	-o "$BIN" -lpthread || exit 1

for mode in alloc getfd read; do
	for threads in 1 2 4 8; do
		for rep in $(seq 1 "$REPEAT"); do
			"$BIN" "$mode" "$threads" "$ITERS" | tee -a "$OUT"
		done
	done
done
