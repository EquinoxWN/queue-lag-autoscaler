#!/usr/bin/env bash
# Runs govulncheck v1.8.0 over all packages; any finding fails.
set -euo pipefail
out=$(go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./... 2>&1) && { echo "$out" | tail -3; echo "vulncheck: no findings"; exit 0; }
echo "$out" | grep -E '^Vulnerability #|Found in|Fixed in|Your code is affected' || echo "$out"
ids=$(echo "$out" | grep -oE '^Vulnerability #[0-9]+: GO-[0-9]{4}-[0-9]+' | grep -oE 'GO-[0-9]{4}-[0-9]+' | sort -u | tr '\n' ' ')
echo "::error::govulncheck failed: ${ids:-no vulnerability ids parsed}"
exit 1
