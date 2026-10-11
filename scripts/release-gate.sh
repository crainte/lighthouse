#!/usr/bin/env bash
# Release gate: the checks TODO.md's "Establish release gates" item asks for,
# run in one place so `hk`'s pre-commit/pre-push hooks and CI can both call
# into it instead of re-implementing the same commands three ways.
#
# Gates, in order, fail-fast:
#   1. `cargo check --all-targets --locked`         — clean compile, every target
#   2. `cargo clippy --all-targets --locked -- -D warnings` — clean lint
#   3. `cargo test --locked`                        — Rust test suite
#   4. `bun install --frozen-lockfile` (frontend)   — reproducible install
#   5. `npx svelte-check --threshold error`         — frontend types
#   6. `npx vitest run`                             — frontend unit tests
#   7. `bun audit`                                  — frontend advisories
#   8. `cargo audit`                                — Rust advisories (respects .cargo/audit.toml)
#   9. `bun run build`                              — production frontend bundle
#  10. `docker build`                                — container image (skippable)
#  11. scripts/backup-restore-smoke.sh              — real-binary backup/restore round trip
#  12. `git status --porcelain` is empty            — clean or intentionally documented tree
#
# Usage: scripts/release-gate.sh [--skip-docker]
#
# `cargo test` runs with `LIGHTHOUSE_AGENT_TOKEN` unset: several tests assert
# behavior for an agent with no ambient credential, and a machine that
# happens to export that variable for its own agent config would otherwise
# make those tests flaky depending on where they run (see tests/ws_api.rs).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

skip_docker=0
for arg in "$@"; do
  case "$arg" in
    --skip-docker) skip_docker=1 ;;
    *)
      echo "unknown flag: $arg (expected --skip-docker)" >&2
      exit 1
      ;;
  esac
done

gate_start_time=$(date +%s)
step_start_time=0

step_start() {
  step_start_time=$(date +%s)
  echo
  echo "==> $1"
}

step_pass() {
  local dur=$(( $(date +%s) - step_start_time ))
  printf 'PASS  %-55s (%ss)\n' "$1" "$dur"
}

step_fail() {
  local dur=$(( $(date +%s) - step_start_time ))
  printf 'FAIL  %-55s (%ss)\n' "$1" "$dur"
  echo
  echo "release gate failed at: $1"
  exit 1
}

# Runs "$@" as a step named "$1"; the command itself is "$2..".
run_step() {
  local name="$1"
  shift
  step_start "$name"
  if "$@"; then
    step_pass "$name"
  else
    step_fail "$name"
  fi
}

cargo_check() { cargo check --all-targets --locked; }
cargo_clippy() { cargo clippy --all-targets --locked -- -D warnings; }
cargo_test() { env -u LIGHTHOUSE_AGENT_TOKEN cargo test --locked; }
cargo_audit_step() { cargo audit; }

frontend_install() { (cd frontend && bun install --frozen-lockfile); }
frontend_svelte_check() { (cd frontend && npx svelte-check --threshold error); }
frontend_vitest() { (cd frontend && npx vitest run); }
frontend_audit() { (cd frontend && bun audit); }
frontend_build() { (cd frontend && bun run build); }

docker_build_step() {
  local git_sha
  git_sha="$(git rev-parse --short=12 HEAD)"
  # Tagged `lighthouse:gate`, distinct from `lighthouse:latest`, so a local gate run
  # never clobbers whatever image an operator already deployed from.
  docker build --build-arg "GIT_SHA=$git_sha" -t lighthouse:gate .
}

backup_restore_smoke() { "$repo_root/scripts/backup-restore-smoke.sh"; }

working_tree_clean() {
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "working tree is not clean:" >&2
    git status --porcelain >&2
    return 1
  fi
  return 0
}

run_step "cargo check --all-targets" cargo_check
run_step "cargo clippy --all-targets -- -D warnings" cargo_clippy
run_step "cargo test --locked" cargo_test
run_step "frontend: bun install --frozen-lockfile" frontend_install
run_step "frontend: svelte-check --threshold error" frontend_svelte_check
run_step "frontend: vitest run" frontend_vitest
run_step "frontend: bun audit" frontend_audit
run_step "cargo audit" cargo_audit_step
run_step "frontend: production build" frontend_build

if [[ "$skip_docker" -eq 1 ]]; then
  echo
  echo "SKIP  container build (--skip-docker)"
else
  run_step "container build (docker build -t lighthouse:gate)" docker_build_step
fi

run_step "backup/restore smoke test" backup_restore_smoke
run_step "working tree clean (git status --porcelain)" working_tree_clean

total_dur=$(( $(date +%s) - gate_start_time ))
echo
echo "release gate passed in ${total_dur}s"