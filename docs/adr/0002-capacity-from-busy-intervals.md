# ADR 0002: Per-worker capacity only from busy, stable intervals

- **Status:** Accepted

## Context

The worker count is `(arrival + lag / target) / per-worker rate`. The per-worker rate is the
processing rate divided by the worker count, but that equals a worker's capacity only when the
workers are busy. When they keep up, the processing rate equals the arrival rate, and dividing it
by the workers understates capacity.

In the first version of the demo this produced two defects:

- With an idle measurement, `ceil(arrival x workers / processing)` exceeds the current count
  whenever arrivals are a little above the last interval's processing, so the count ratcheted up
  from 2 to 8 workers at a steady 100 msg/s.
- A measurement taken in an interval during which the worker count changed was divided by the new
  count, so after a scale-down the per-worker rate was overstated and the fleet shrank while 9,000
  messages were still waiting.

A configured `worker_rate` avoids both, but goes stale when the workload changes, which is the
reason to measure at all.

## Decision

- A measured rate counts as capacity only when the lag is above `busy_lag`.
- It is divided by the larger worker count at the two ends of the interval.
- It is remembered for later idle periods only if the count did not change in that interval.
- Idle measurements are a lower bound: they may lower the count, never raise it.
- The configured `worker_rate` is used only when nothing has been measured while busy.

## Consequences

- Never overstating capacity means erring towards more workers: in the demo the lag-based run used
  227 worker-minutes where 2 workers would have done 100 at the baseline rate, but 1,600 for a fleet
  sized for the peak.
- The remembered value can be stale if the workload gets faster or slower while the queue stays
  empty; the next busy interval corrects it.
- `busy_lag` must be set above the lag that commit intervals leave behind on an idle group (see the
  RFC risks); a value that is too low makes the scaler hold more workers than needed, not fewer.
