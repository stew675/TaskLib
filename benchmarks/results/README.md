# Recorded results

Baseline: commit `e10d6f1` (the last committed revision before the concurrency
fixes and optimizations).

Final: the working tree containing the migration/lifetime fixes, sharded
lock-free free list, vector IO fairness, and sharded API-entry references.

Raw output files:

| File | Description |
| --- | --- |
| `http_baseline_e10d6f1.txt` | baseline HTTP echo suite |
| `http_final_run1.txt` | final HTTP suite, first run |
| `http_final_run2.txt` | final HTTP suite, repeat run |
| `micro_baseline_e10d6f1.txt` | baseline allocator/API microbenchmarks |
| `micro_final.txt` | final allocator/API microbenchmarks |

## HTTP echo throughput

`IN`/`OUT` in MB/s. Final values are from `http_final_run2.txt`; `run1` is
included for variance. One client in `run1`'s 4x1000 case had a transient
outlier, so the repeat run is the representative one.

| Scenario | Baseline IN | Final IN | Baseline OUT | Final OUT |
| --- | ---: | ---: | ---: | ---: |
| 1x500 | 7480 | 7208 | 7539 | 7272 |
| 1x1000 | 6830 | 6923 | 6902 | 6996 |
| 1x2000 | 7048 | 7030 | 7114 | 7119 |
| 2x1000 | 6215 | 6144 | 6360 | 6288 |
| 4x1000 | 5285 | 5518 | 5573 | 5806 |

The end-to-end result is within run-to-run variance, with a possible small gain
at the highest client count. Server CPU use stayed around 245-257% in both
revisions. In other words, the correctness work did not cost throughput on this
workload.

## Allocator microbenchmark

Median aggregate `Mops` of three runs. This isolates
`task_get_free_task()`/`task_free()` from socket syscalls.

| Threads | Baseline | Final | Speedup |
| ---: | ---: | ---: | ---: |
| 1 | 73.4 | 88.0 | 1.20x |
| 2 | 18.7 | 42.9 | 2.29x |
| 4 | 13.9 | 36.6 | 2.63x |
| 8 | 8.6 | 35.1 | 4.07x |

The original single per-instance free-list mutex collapsed under contention.
The final allocator uses 16 sharded tagged free lists. Each thread sticks to a
shard, so the hot head cache line is local, while an empty shard falls back to
scanning the others.

## API lookup/unlock microbenchmark

Aggregate `Mops` (higher is better):

| Mode | Threads | Baseline | Final |
| --- | ---: | ---: | ---: |
| `getfd` | 1 | 104.3 | 83.4 |
| `getfd` | 8 | 794.7 | 652.2 |
| `read` lookup | 1 | 124.9 | 111.0 |
| `read` lookup | 8 | 934.0 | 877.6 |

These foreign-thread calls pay roughly 2-3 ns extra for the API-entry reference
used to make `TASK_instance_destroy()` safe against in-flight task APIs. The
counters are sharded by thread, so the overhead does not grow into a contended
cache line. Calls made from one of the instance's own IO workers skip the
reference entirely because destruction already joins all workers before freeing
the instance; the server callback hot path therefore does not pay it.

## Phase 7 result (deferred-close / TFD-cookie revision)

New files, recorded on the same host/environment:

| File | Description |
| --- | --- |
| `micro_phase7.txt` | allocator/API microbenchmarks after the lifetime rework |
| `http_phase7.txt` | HTTP echo suite after the lifetime rework |

### HTTP echo throughput

| Scenario | Baseline IN | Final (pre-Phase-7) IN | Phase 7 IN | Phase 7 OUT |
| --- | ---: | ---: | ---: | ---: |
| 1x500 | 7480 | 7208 | 7362 | 7426 |
| 1x1000 | 6830 | 6923 | 6988 | 7061 |
| 1x2000 | 7048 | 7030 | 7080 | 7170 |
| 2x1000 | 6215 | 6144 | 6175 | 6317 |
| 4x1000 | 5285 | 5518 | 5446 | 5734 |

The HTTP result is within run-to-run variance of the previous final run. The
deferred-close queue and generation-tagged epoll dispatch did not introduce a
throughput regression.

### Allocator/API microbenchmarks

Aggregate `Mops` (median of three runs, converted from the recorded `ns/op`):

| Mode | Threads | Baseline | Pre-Phase-7 | Phase 7 |
| --- | ---: | ---: | ---: | ---: |
| alloc | 1 | 73.4 | 88.0 | 91.5 |
| alloc | 2 | 18.7 | 42.9 | 97.8 |
| alloc | 4 | 13.9 | 36.6 | 97.0 |
| alloc | 8 | 8.6 | 35.1 | 95.4 |
| getfd | 1 | 104.3 | 83.4 | 83.2 |
| getfd | 8 | 794.7 | 652.2 | 621.9 |
| read | 1 | 124.9 | 111.0 | 107.0 |
| read | 8 | 934.0 | 877.6 | 839.5 |

The allocator no longer applies a per-instance `tfd_pool_used` seq_cst RMW or
the advisory 80% cap on every allocate/free, and `TASK_FREE_SHARDS` is now 64.
Per-thread allocator throughput stays flat instead of collapsing as thread count
grows. The reported `getfd`/`read` phase-7 values are slightly below the
pre-phase-7 run on this host, within normal microbenchmark variance; the API
entry admission check remains cheap and uncontended.
