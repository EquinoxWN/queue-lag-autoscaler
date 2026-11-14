# RFC 0001: queue-lag-autoscaler design

- **Status:** Accepted for M1 (M2 and M3 open)
- **Author:** EquinoxWN
- **Created:** 2026-11-14

## Problem (Define)

Job-processing workers that read from a queue are usually scaled on CPU. CPU is a late and indirect
signal: a backlog builds while the existing workers look busy but not saturated, workers that wait
on I/O never reach the CPU target, and once the scale-up comes it is sized for CPU, not for the
backlog. The result is jobs that finish long after they were submitted, or a fleet sized for the
peak around the clock.

The signals that matter are in the queue itself: how many messages are waiting (lag), how fast they
arrive and how fast the workers commit them. From those, the number of workers needed to clear the
backlog within a target time can be computed directly.

Users: platform and backend engineers who run consumer groups (Kafka in M1) behind a worker
Deployment and choose its replica count or autoscaler settings.

Critical-to-quality measures:

| CTQ | Target | How it is measured (M1) |
|---|---|---|
| Decision is sufficient | For every decision not cut by a limit, `workers x per-worker rate >= arrival + lag / target` | Randomized test over 20,000 cases; the demo counts violations (`short_decisions`, must be 0) |
| Decision is minimal | One worker fewer would fall below the required rate | Same randomized test |
| Message wait under bursts | Longest wait <= target + 2 controller ticks + worker startup | Demo on the in-process broker (`max_wait_within_bound`) |
| Cost vs a peak-sized fleet | Fewer worker-minutes than a static fleet at the maximum size | Demo reports worker-minutes for both |
| Input safety | Malformed snapshots and configs are refused with a clear error, inputs are size-bounded | Parser and config error tables in the tests |

## Goals

- M1: a controller reads queue lag and processing rate for each consumer group.
- M1: it computes the workers needed to clear the backlog within a target time (Little's law), not a
  CPU percentage.
- M2: stabilization windows and cooldowns prevent flapping when traffic is spiky.
- M2: idle queues scale to zero, and hard limits cap cost.
- M3: a bursty load generator in kind compares this scaler with the CPU-based Horizontal Pod
  Autoscaler, reporting time to drain, worker-minutes and scaling oscillations.

## Non-goals

- Running as a hosted production service.
- Changing Kubernetes objects in M1: M1 prints recommendations; acting on a Deployment comes with
  the kind comparison.
- Brokers other than Kafka in M1 (RabbitMQ queue depth fits the same model and is a later source).
- Predicting future traffic. Decisions use measured rates only.

## Design (M1)

```
source (kafka_exporter /metrics over HTTP, or kafka-consumer-groups.sh output from a file)
  -> lag.Offset per (group, topic, partition): committed offset and log-end offset
  -> rate.Estimator per group: arrival = growth of log-end offsets, processing = growth of commits
  -> scale.Decide: workers = ceil((arrival + lag / target) / per-worker rate), then limits
  -> controller.Recommendation as one JSON line per group per tick
```

**Signals.** Lag per partition is `log_end - committed` (a partition with no commit counts its whole
log). The arrival rate is the growth of log-end offsets between two snapshots divided by the
elapsed time (monotonic clock); the processing rate is the growth of committed offsets. Partitions
whose committed offset moved backwards (a reset) or that appeared since the last snapshot add
nothing to that interval. Arrivals are smoothed with an exponentially weighted average
(`smoothing`, default 0.5); the processing rate is not, because it depends on the worker count,
which changes between intervals.

**Decision.** Little's law (L = lambda x W) links the backlog L, the throughput lambda and the time W
a message spends waiting. To clear L messages within the target T while A messages per second keep
arriving, the group must process `A + L / T` messages per second. Dividing by the per-worker rate and
rounding up gives the worker count. The plan is re-evaluated every tick with the new lag, so the
backlog decays geometrically rather than linearly; what the formula keeps is the wait of the newest
message, `L / capacity <= T`, at the measured rates.

**Per-worker capacity** is the hard part, because a measured processing rate equals capacity only
while workers are busy. The decision uses, in order:

1. `measured`: processing rate / workers, when the lag is above `busy_lag` (workers have work queued).
   The divisor is the larger of the worker counts at the two ends of the interval, so a change of
   the count mid-interval never overstates one worker's rate.
2. `remembered`: the last `measured` value from an interval in which the worker count did not change.
3. `configured`: the policy's `worker_rate`, if set.
4. `idle-bound`: the measured rate while not busy. It is only a lower bound on capacity, so it may
   scale down but never up (otherwise small arrival noise ratchets the count up).
5. `none`: nothing known; hold the current count (start one worker if there are none and a backlog).

**Limits.** `min_workers` (>= 1 in M1), `max_workers`, and by default the partition count, because a
Kafka partition is consumed by one member of the group. Every decision carries the projected time
to drain at the recommended count (or -1 if capacity does not exceed arrivals).

**Reliability.** HTTP fetches have a per-attempt timeout (5 s), three attempts with doubling
backoff for network errors, 429 and 5xx, no retry for other 4xx or parse errors, and an 8 MiB body
cap. The `run` loop stops after `-max-failures` consecutive failed ticks instead of looping on a
dead source. Inputs are validated: group and topic names, offsets, label syntax, integral values,
duplicate partitions, and the `LAG` column of `kafka-consumer-groups.sh` must equal
`log_end - committed`.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Horizontal Pod Autoscaler on CPU | The signal this project replaces: it scales after CPU rises and sizes for CPU, not for the backlog. It is the baseline for the M3 comparison. |
| HPA on an external lag metric (`AverageValue` of lag per pod) | Needs a hand-picked "lag per pod" target that encodes per-worker speed and the time target implicitly, and does not use the arrival or processing rate; a change of worker speed silently changes the drain time. Kept as a second M3 baseline. |
| KEDA Kafka scaler | Uses the same lag-per-replica idea as above (`lagThreshold`), with scale to zero. A good production choice; this project computes the count from rates and a time target instead and explains each decision, which is what it wants to measure. |
| Proportional-integral controller on lag | Can track a lag set point well but needs tuning per workload and gives no closed-form explanation of a decision; Little's law gives one from measured quantities. |
| Queueing model (M/M/c) for the wait distribution | More precise for random arrivals, but needs service-time distributions this tool does not observe; the backlog term dominates during bursts, which is the case M1 targets. |

## Measurement plan (Measure, Analyze, Control)

- M1 (done, see [docs/results/m1.md](../results/m1.md)): a 50-minute simulated bursty load on the
  in-process broker (32 partitions, 50 msg/s per worker, three bursts) compares the lag-based
  decisions with two static fleets on longest wait, wait percentiles, worker-minutes and the number
  of changes. A CI job (not yet run) feeds real `kafka-consumer-groups.sh` snapshots from a Kafka
  4.3.1 broker to `plan` and checks the lag equals the tool's own `LAG` column.
- M3: the same bursty load in kind, against this scaler, CPU-based HPA and HPA on lag: time to
  drain, worker-minutes and scaling oscillations.
- Control: CI runs gofmt, vet, `go test -race`, govulncheck and the demo with its verify line on
  every push.

## Milestones

- **M1:** A controller reads queue lag and processing rate for each consumer group. It computes the workers needed to clear the backlog within a target time (Little's law), not a CPU percentage.
- **M2:** Stabilization windows and cooldowns prevent flapping when traffic is spiky. Idle queues scale to zero, and hard limits cap cost.
- **M3:** A bursty load generator in kind compares this scaler with the CPU-based Horizontal Pod Autoscaler. Results report time to drain, worker-minutes used and scaling oscillations.

## Risks and open questions

- **Flapping.** M1 has no stabilization: around a boundary (2 workers at exactly the arrival rate)
  it alternates between two counts. The demo shows 76 changes in 50 minutes, most of them 2 <-> 3.
  M2 adds windows and cooldowns.
- **Commit interval.** With Kafka auto-commit (5 s by default) the lag is rarely 0 even when workers
  are idle. `busy_lag` should be at least arrival rate x commit interval, or the idle rate is read
  as capacity and the fleet is over-sized (never under-sized).
- **Uneven partitions.** The model treats workers as equal and the lag as shared; a hot partition
  can only be served by one worker, which more workers do not fix.
- **Startup time** of new workers delays the effect of a scale-up; the demo models 10 s.
