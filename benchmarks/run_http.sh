#!/bin/bash
# End-to-end loopback echo benchmark.
#
# Usage: ./run_http.sh [bin-dir] [output-file] [base-port]
#
#   bin-dir       Directory containing the built "echoes" and "load_gen"
#                 binaries.  Defaults to the repository root.
#   output-file   File to append results to.  Defaults to
#                 benchmarks/results/http-<timestamp>.txt.
#   base-port     First TCP port to use.  Each scenario increments it.
#
# The server and clients are deliberately separate processes.  Both directions
# are reported: IN is the load generator's received echo traffic, OUT is what
# it sent to the server.
set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
BIN=${1:-"$ROOT"}
OUT=${2:-"$ROOT/benchmarks/results/http-$(date +%Y%m%d-%H%M%S).txt"}
PORT=${3:-19500}

cd "$BIN" || exit 1
if [ ! -x ./echoes ] || [ ! -x ./load_gen ]; then
	echo "error: build echoes and load_gen in $BIN first (make -C $ROOT)" >&2
	exit 1
fi
if ! command -v /usr/bin/time >/dev/null 2>&1; then
	echo "error: /usr/bin/time is required" >&2
	exit 1
fi

LOG_DIR=$(mktemp -d /tmp/tasklib-bench.XXXXXX)
trap 'rm -rf "$LOG_DIR"' EXIT
: > "$OUT"

run_one()
{
	local label="$1"
	local nclients="$2"
	local nprocs="$3"
	local seconds="$4"
	local start_avg="$5"
	local ep pids=()

	PORT=$((PORT + 1))
	echo "=== $label : loadgens=$nprocs clients/loadgen=$nclients dur=${seconds}s port=$PORT" | tee -a "$OUT"

	/usr/bin/time -v -o "$LOG_DIR/echoes.time" \
		./echoes -t $((seconds + 3)) -n 200000 -l 127.0.0.1:$PORT \
		>"$LOG_DIR/echoes.log" 2>&1 &
	ep=$!
	sleep 1

	for i in $(seq 1 $nprocs); do
		/usr/bin/time -v -o "$LOG_DIR/lg$i.time" \
			./load_gen -n "$nclients" -s 100 -t "$seconds" -c 127.0.0.1:$PORT \
			>"$LOG_DIR/${label}_lg$i.log" 2>&1 &
		pids+=($!)
	done
	for p in "${pids[@]}"; do wait "$p"; done
	wait "$ep"

	local tin=0 tout=0 tpeak=0
	for i in $(seq 1 $nprocs); do
		local inavg outavg peak cpu rss
		read inavg outavg peak < <(
			awk -v s="$start_avg" -F'[= ]+' '
				/OUT MB\/sec=/ {
					t=$1; sub("T\\+","",t);
					if (t >= s) {
						si += $5; so += $8; n++;
						if ($8 > max) max = $8
					}
				}
				END {
					if (n > 0) printf "%.0f %.0f %.0f", si/n, so/n, max;
					else print "0 0 0"
				}' "$LOG_DIR/${label}_lg$i.log"
		)
		cpu=$(awk -F': ' '/Percent of CPU/{print $2}' "$LOG_DIR/lg$i.time")
		rss=$(awk -F': ' '/Maximum resident set size/{print $2}' "$LOG_DIR/lg$i.time")
		echo "  lg$i: IN=${inavg} MB/s OUT=${outavg} MB/s peak=${peak} MB/s cpu=$cpu rss=${rss}KB" | tee -a "$OUT"
		tin=$((tin + inavg)); tout=$((tout + outavg)); tpeak=$((tpeak + peak))
	done

	local ecpu erss evcs eivcs
	ecpu=$(awk -F': ' '/Percent of CPU/{print $2}' "$LOG_DIR/echoes.time")
	erss=$(awk -F': ' '/Maximum resident set size/{print $2}' "$LOG_DIR/echoes.time")
	evcs=$(awk -F': ' '/Voluntary context switches/{print $2}' "$LOG_DIR/echoes.time")
	eivcs=$(awk -F': ' '/Involuntary context switches/{print $2}' "$LOG_DIR/echoes.time")
	echo "  TOTAL IN=${tin} MB/s OUT=${tout} MB/s peak_out_sum=${tpeak} MB/s" | tee -a "$OUT"
	echo "  echoes: cpu=$ecpu rss=${erss}KB voluntary_cs=$evcs involuntary_cs=$eivcs" | tee -a "$OUT"
}

run_one "1x500"   500  1 15 8
run_one "1x1000"  1000 1 15 8
run_one "1x2000"  2000 1 15 8
run_one "2x1000"  1000 2 15 8
run_one "4x1000"  1000 4 15 8
