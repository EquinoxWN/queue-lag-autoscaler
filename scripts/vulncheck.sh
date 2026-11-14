#!/usr/bin/env bash
# Runs govulncheck and fails on any finding except the allowlisted IDs below.
# GO-2026-6859 (net/http MaxBytesReader header race, Go standard library) is fixed in Go 1.26.11;
# the portfolio pins Go 1.26.10 in CI. Remove the entry when go-version moves to 1.26.11.
set -euo pipefail
ALLOW="GO-2026-6859"
out=$(go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./... 2>&1) && { echo "$out" | tail -3; echo "vulncheck: no findings"; exit 0; }
echo "$out" | grep -E '^Vulnerability #|Found in|Fixed in|Your code is affected' || true
ids=$(echo "$out" | grep -oE '^Vulnerability #[0-9]+: GO-[0-9]{4}-[0-9]+' | grep -oE 'GO-[0-9]{4}-[0-9]+' | sort -u)
[ -n "$ids" ] || { echo "$out"; echo "::error::govulncheck failed without findings"; exit 1; }
for id in $ids; do
  case " $ALLOW " in
    *" $id "*) echo "allowed: $id (fixed in a newer Go patch release, see the comment above)";;
    *) echo "::error::govulncheck finding $id is not allowlisted"; exit 1;;
  esac
done
echo "vulncheck: only allowlisted findings ($ALLOW)"
