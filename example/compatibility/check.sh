#!/usr/bin/env bash
set -euo pipefail

sdk_root=$(cd "$(dirname "$0")/../.." && pwd)
isolate_go=${GO_ISOLATES:-"$sdk_root/../golang-go/bin/go"}
baseline=4c2862a43c9a2f653c345addd761776b5ba5ac1a
artifacts=${COMPATIBILITY_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/isolate-compatibility.XXXXXX")}
mkdir -p "$artifacts/baseline"
artifacts=$(cd "$artifacts" && pwd)
trap 'status=$?; if (( status != 0 )); then tail -n 80 "$artifacts"/*.log; fi; printf "Compatibility logs: %s\n" "$artifacts"; exit "$status"' EXIT

# Use an immutable released source snapshot. Do not rewrite the caller's checkout.
if ! git -C "$sdk_root" cat-file -e "$baseline^{commit}" 2>/dev/null; then
  git -C "$sdk_root" fetch origin "$baseline" > "$artifacts/fetch.log" 2>&1
fi
git -C "$sdk_root" archive "$baseline" | tar -x -C "$artifacts/baseline"
(
  cd "$artifacts/baseline"
  "$isolate_go" build -o "$artifacts/baseline-replay" ./example/determinism/check
) > "$artifacts/baseline-build.log" 2>&1
(
  cd "$sdk_root"
  "$isolate_go" build -o "$artifacts/current-replay" ./example/determinism/check
) > "$artifacts/current-build.log" 2>&1

# Rebuild each SDK with the current compiler. This checks SDK upgrade/rollback
# within determinism v1; it does not claim arbitrary old toolchain compatibility.
for revision in baseline current; do
  for fixture in "$sdk_root/example/determinism/testdata/history-shared-random.json" "$sdk_root/example/compatibility/testdata/history-v1.json"; do
    for procs in 1 8; do
      GOMAXPROCS="$procs" "$artifacts/$revision-replay" -history "$fixture" >> "$artifacts/$revision-replay.log" 2>&1
    done
  done
done
printf 'SDK upgrade and rollback replay passed (old and new histories, GOMAXPROCS 1/8).\n'
