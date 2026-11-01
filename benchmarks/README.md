# Benchmarks

Two benchmark suites are kept here:

- `run_http.sh` — end-to-end loopback echo throughput using the `echoes` server
  and `load_gen` client from this repository.
- `run_micro.sh` — task allocator and API lookup/unlock microbenchmarks.

The recorded results are in `results/`. They are for comparison between
revisions on the same machine, not absolute performance claims. Run-to-run
variance was roughly +/-5% for the HTTP suite even on an idle loopback host.

## Environment used for the recorded results

- AMD Ryzen 9 7950X, 16 CPUs, single NUMA node
- Linux 7.2.13
- GCC 16.2.1
- `-O3 -march=native` builds
- HTTP traffic over loopback

## HTTP benchmark

```sh
make
./benchmarks/run_http.sh . benchmarks/results/http-<label>.txt 19500
```

Each scenario runs `echoes` for `duration + 3` seconds and one or more
`load_gen` processes for `duration` seconds. `IN` is echo traffic received by
the load generator, `OUT` is traffic sent to the server. The script averages
the per-second `OUT MB/sec` samples from second 8 onward, which skips warm-up
and the final partial second.

The recorded suite is:

```text
1x500    1 load generator,  500 clients
1x1000   1 load generator, 1000 clients
1x2000   1 load generator, 2000 clients
2x1000   2 load generators, 1000 clients each
4x1000   4 load generators, 1000 clients each
```

## Microbenchmarks

```sh
./benchmarks/run_micro.sh . benchmarks/results/micro-<label>.txt
```

Modes:

- `alloc` — `task_get_free_task()` + `task_free()` pair. This deliberately uses
  TaskLib internals to isolate the free-list from socket syscalls.
- `getfd` — `TASK_socket_get_fd()`, covering the locked meta lookup/unlock path.
- `read` — `TASK_socket_read()` with a zero-length buffer. The call fails
  `EINVAL` after the lock-free read-side lookup/reservation path, so it measures
  lookup/unlock overhead without doing IO.

Each mode is run with 1, 2, 4, and 8 threads. Threads are pinned to distinct
CPUs. `Mops` is the aggregate operation rate across all threads.

`ITERS` and `REPEAT` can be overridden:

```sh
ITERS=1000000 REPEAT=5 ./benchmarks/run_micro.sh .
```

## Comparing revisions

Use a Git worktree for the baseline:

```sh
git worktree add --detach /tmp/tasklib-baseline e10d6f1
make -C /tmp/tasklib-baseline
./benchmarks/run_http.sh /tmp/tasklib-baseline benchmarks/results/http-baseline.txt 19500
./benchmarks/run_micro.sh /tmp/tasklib-baseline benchmarks/results/micro-baseline.txt
```
