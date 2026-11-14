# ADR 0003: Standard library only, and a read-only M1

- **Status:** Accepted

## Context

The README's stack names controller-runtime and Kafka lag metrics. A Kafka client library (for
example franz-go) or controller-runtime would pull in tens of modules and a Kubernetes API
server for tests (envtest), while M1 only has to read lag and rates and compute a worker count.

## Decision

- M1 uses the Go standard library only. Lag is read from two text formats that Kafka users already
  have: the Prometheus metrics of kafka_exporter (`kafka_consumergroup_current_offset`,
  `kafka_topic_partition_current_offset`) over HTTP, and the output of
  `kafka-consumer-groups.sh --describe`.
- M1 prints recommendations as JSON lines and changes nothing. Writing the replica count of a
  Deployment (through the scale subresource, with controller-runtime if it earns its place) comes
  with the kind comparison in M3.
- Local tests use an in-process broker (`internal/fakebroker`); a real Kafka broker runs only in CI.

## Consequences

- No third-party modules to audit; `govulncheck` covers the standard library only.
- The kafka_exporter parser is tested against a fixture written from the exporter's metric names,
  not yet against a running exporter; the `kafka-consumer-groups.sh` path has a CI job with a real
  broker (not yet run).
- Users who want the decision applied today have to apply it themselves (for example
  `kubectl scale`), which keeps a human in the loop while the M2 stabilization is missing.
