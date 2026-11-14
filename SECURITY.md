# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/queue-lag-autoscaler/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** crashes, panics or unbounded memory use on malformed metrics, `kafka-consumer-groups.sh` output or config files; a recommendation that does not follow the documented formula; data races; a source URL that leaks credentials.
- **Out of scope:** Performance problems and missing features listed on the roadmap.

## How this repository protects itself

- Every GitHub Action is pinned to a full commit SHA and every container image to a digest;
  workflows run with read-only permissions and without persisted credentials.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a single `main` branch; dependencies are updated by hand and checked by the CI audit.
- CI runs lint, tests and a known-vulnerability audit (`make audit`) on every push and pull request.
- The tool only reads: it fetches a metrics URL or reads files and prints recommendations. In M1 it
  changes nothing in Kubernetes or Kafka.
