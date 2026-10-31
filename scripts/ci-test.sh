#!/usr/bin/env bash
# Run the Rust test gate with a diagnostic native backtrace hook. The hook
# preserves SIGABRT on a pure-virtual call; it only makes the failure useful.
#
# Usage: scripts/ci-test.sh [cargo test args...]
# Cargo's per-host runner preloads the hook into test binaries (and their
# children), not into cargo, rustc, build scripts, or DuckDB compilation.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
umask 077
hook_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/lighthouse-pure-virtual.XXXXXXXX")"
shim="$hook_dir/pure-virtual-backtrace.so"
log="$hook_dir/test.log"
cleanup() {
  rm -f -- "$shim" "$log"
  rmdir -- "$hook_dir"
}
trap cleanup EXIT

unavailable() {
  if [ -n "${CI:-}" ]; then
    echo "error: CI cannot install the pure-virtual backtrace hook: $1" >&2
    exit 1
  fi
  echo "warning: $1; running tests without the backtrace hook" >&2
}

hook_ready=false
if ! cxx="$(command -v c++ || command -v g++)"; then
  unavailable "no C++ compiler found"
elif [ "${shim}" != "${shim//[[:space:]:]/}" ]; then
  # Cargo splits its runner on whitespace; LD_PRELOAD splits on whitespace
  # and colons. In either case it would silently load the wrong library.
  unavailable "hook path contains whitespace or a colon: $shim"
else
  host_triple="$(rustc -vV | sed -n 's/^host: //p')"
  if [ -z "$host_triple" ]; then
    unavailable "could not determine the Rust host target"
  elif [ -n "${CI:-}" ] && [ -n "${CARGO_BUILD_TARGET:-}" ] && [ "$CARGO_BUILD_TARGET" != "$host_triple" ]; then
    unavailable "CARGO_BUILD_TARGET=$CARGO_BUILD_TARGET differs from native $host_triple"
  elif "$cxx" -shared -fPIC -O2 -g -o "$shim" scripts/ci/pure-virtual-backtrace.cc; then
    runner_var="CARGO_TARGET_$(printf '%s' "$host_triple" | tr 'a-z-' 'A-Z_')_RUNNER"
    export "${runner_var}=env LD_PRELOAD=${shim}"
    hook_ready=true
    echo "native pure-virtual backtrace hook configured for $host_triple: $shim" >&2
  else
    unavailable "C++ compiler failed to build the hook"
  fi
fi

# The exact previous gate unset this credential. Keep the same assertion
# environment, test selection, and Cargo lockfile discipline.
set +e
env -u LIGHTHOUSE_AGENT_TOKEN cargo test --locked "$@" 2>&1 | tee "$log"
results=("${PIPESTATUS[@]}")
set -e
status="${results[0]}"
if [ "${results[1]}" -ne 0 ]; then
  echo "error: could not capture test output for backtrace diagnostics" >&2
  status=1
fi

# A Cargo runner can be bypassed by a configured build.target. The marker is
# printed by the hook INSIDE a test binary, not by this wrapper. We verify
# admin_incidents for the full CI gate; this is not proof of every binary.
# Filtered local runs need not include that test.
if [ -n "${CI:-}" ] && [ "$#" -eq 0 ] && [ "$hook_ready" = true ]; then
  if ! grep -Eq '^\[native-hook-loaded\] pid=[0-9]+ exe=.*/admin_incidents-[^/ ]+$' "$log"; then
    echo "error: admin_incidents ran without the native backtrace hook" >&2
    status=1
  fi
fi

# Symbolize *while this exact test binary is still on the runner*. GitHub's
# ephemeral runner will discard it; raw PIE offsets alone aren't enough to
# diagnose a native crash later. Keep raw frames too, even when a dependency
# has no line-level debug info. Only run addr2line on existing files named in
# the hook's backtrace, never executing an object path as a command. The log
# can contain arbitrary test output, so source paths here are diagnostic data.
if grep -q '^\[pure-virtual-debug\] pid=' "$log"; then
  echo '--- native pure-virtual stack (symbolized frames) ---' >&2
  if command -v addr2line >/dev/null 2>&1; then
    while read -r object offset; do
      if [ -f "$object" ]; then
        printf '%s %s: ' "$object" "$offset" >&2
        addr2line -C -f -i -p -e "$object" "$offset" >&2 || true
      fi
    # backtrace_symbols_fd prints `object(+0xOFFSET)[0xADDR]`, with no space
    # before the bracket. An earlier version of this pattern required one,
    # matched nothing, and left this section empty on a real failure.
    done < <(sed -nE 's|^([^ ()]+)\(\+(0x[0-9a-fA-F]+)\) ?\[.*|\1 \2|p' "$log")
  else
    echo 'warning: addr2line unavailable; raw frames above remain in test output' >&2
  fi
  # A child can abort without its parent test failing. A native abort still
  # makes CI red, even if a caller accidentally ignored that child exit.
  status=1
fi
exit "$status"
