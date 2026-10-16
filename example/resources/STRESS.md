# Isolate stress test

From the SDK root, run:

```sh
ISOLATE_STRESS=1 tgo test ./example/resources/check \
  -run '^TestIsolateStress$' -count=1 -v -timeout=23m
```

The test builds a checker using the native isolate toolchain, warms it up, and
then runs continuous load for **10 minutes**, followed by collection and cleanup
checks. The outer timeout allows up to 10 minutes for compilation plus 12 minutes
for execution. Ordinary test runs skip the soak test. No Temporal server is needed.

Each batch runs four host drivers concurrently. Each driver completes one
workflow and evicts another while suspended. Each workflow starts four workers
that churn 4–7 MiB of allocations, hash buffers, grow stacks, use pull iterators,
iterate maps, consume random values, and select between ready channels. A
checksum includes the order of observations and must match the warmed run for
the same input and workflow identity. Workflows suspend with live state, a
blocked child, and timers. Cleanup must drain goroutines and stacks, and invoking
retained callbacks after close must not revive execution.

Every batch also replays the saved determinism history. The replay computes a
fresh trace, compares it to recorded activity observations, and checks the final
result, exercising timers, random distributions, maps, reflection, cancellation,
and deterministic goroutine scheduling across workflow tasks.

The checker records a baseline after three warmup batches. Every 30 seconds and
after the load ends, it collects garbage, releases unused memory to the OS, and
checks these fixed growth allowances:

| Resource | Allowed growth from warmed baseline |
| --- | ---: |
| Live heap | 16 MiB |
| Live heap objects | 20,000 |
| Goroutine stack memory | 4 MiB |
| Resident memory | 64 MiB |
| Goroutines | 8 |
| OS threads | 8 |
| File descriptors | 4 |

These allowances accommodate runtime caches and scheduler pools and never scale
with the number of executions. After cleanup, the checker also allows at most
100 ms of process CPU during a 250 ms idle interval to detect stranded busy
work. Samples report cumulative process CPU so workload intensity is visible.
A pass means no resource growth beyond these bounds in this run; smaller or
slower leaks can require longer runs. File descriptor accounting requires OS
support from gopsutil (the source development environment uses Linux).

For a shorter smoke test or a longer investigation, build and run the same
checker directly:

```sh
tgo build -o /tmp/isolate-resource-stress ./example/resources/check
/tmp/isolate-resource-stress -stress -duration=30s \
  -history=example/determinism/testdata/history-shared-random.json
```

## Recorded validation

On 2026-10-16, using the Linux arm64 isolate toolchain
`go1.28-devel_dba1c0fd35`, the full test passed after 10m0.335s of measured load:
50,440 workload executions and 6,305 saved-history replays, excluding warmup.

| Resource | Warmed baseline | After load and collection |
| --- | ---: | ---: |
| Live heap | 3.11 MiB | 3.26 MiB |
| Live heap objects | 13,686 | 13,986 |
| Stack memory | 0.91 MiB | 0.94 MiB |
| Resident memory | 34.80 MiB | 37.53 MiB |
| Goroutines | 1 | 1 |
| OS threads | 12 | 13 |
| File descriptors | 7 | 7 |

The process consumed about 1,914 CPU seconds during load and 10 ms during the
final 250 ms idle check. No growth exceeded the configured leak thresholds.
The existing compiled resource controls, saved-history corruption check, and
new leak-threshold regression tests also passed.
