.PHONY: setup lint test bench demo plan audit ci

setup:
	go mod download

lint:
	@files="$$(gofmt -l cmd internal)"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w"; exit 1)
	go vet ./...

test:
	go test -race -count=1 ./...

# The numbers in the README: 50 simulated minutes, three bursts, in-process broker.
bench:
	go run ./cmd/queue-lag-autoscaler demo

demo: bench

# One recommendation per group from two saved kafka-consumer-groups.sh snapshots taken 60 s apart.
plan:
	go run ./cmd/queue-lag-autoscaler plan -config examples/config.json \
		-before examples/describe-before.txt -after examples/describe-after.txt -elapsed 60s

# Known vulnerabilities in called code (Go 1.26.11 as in CI); any finding fails.
audit:
	bash scripts/vulncheck.sh

ci: setup lint test
