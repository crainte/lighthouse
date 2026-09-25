#!/usr/bin/env bash
# Backup/restore smoke test against the real `warden` binary.
#
# Starts a standalone node with a fresh file-backed DuckDB, creates one check
# through the authenticated admin API (checks are never file-seeded — see
# `Config`'s `deny_unknown_fields` on `[[checks]]`), mints an agent credential
# and restarts so the embedded agent can authenticate (standalone still
# polls its own coordinator over HTTP for the check list; only result
# ingest is in-process), lets it probe itself over TCP a couple of times,
# stops the node, runs `warden backup`, then `warden restore` into a fresh
# path, and asserts the restore report says data actually came back.
# Exercises the same `run_backup`/`run_restore` code paths in src/main.rs
# that an operator runs by hand.
#
# Usage: scripts/backup-restore-smoke.sh
# Exit 0 on success, non-zero (with a message on stderr) on failure.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

WARDEN_BIN="${WARDEN_BIN:-}"
if [[ -z "$WARDEN_BIN" ]]; then
  echo "==> building warden (debug; the smoke test only needs a runnable binary)"
  cargo build --locked --bin warden >/dev/null
  WARDEN_BIN="$repo_root/target/debug/warden"
fi

work_dir="$(mktemp -d)"
pid=""
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
admin_password="gate-smoke-$$"
db_path="$work_dir/coordinator.duckdb"
config_path="$work_dir/warden.toml"
cookie_jar="$work_dir/cookies.txt"
base_url="http://127.0.0.1:$port"

write_config() {
  cat >"$config_path" <<EOF
[coordinator]
listen = "127.0.0.1:$port"
db_path = "$db_path"
admin_password = "$admin_password"
EOF
  if [[ -n "${1:-}" ]]; then
    cat >>"$config_path" <<EOF

[agent]
token = "$1"
EOF
  fi
}

start_node() {
  "$WARDEN_BIN" standalone --config "$config_path" >"$work_dir/warden.log" 2>&1 &
  pid=$!
  local ready=0
  for _ in $(seq 1 50); do
    if curl -fsS -o /dev/null "$base_url/api/v1/session"; then
      ready=1
      break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "warden exited before becoming ready; log:" >&2
      cat "$work_dir/warden.log" >&2
      exit 1
    fi
    sleep 0.2
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "warden never became ready on $base_url" >&2
    cat "$work_dir/warden.log" >&2
    exit 1
  fi
}

stop_node() {
  kill "$pid"
  wait "$pid" 2>/dev/null || true
  pid=""
}

echo "==> starting standalone node on 127.0.0.1:$port (db: $db_path)"
write_config
start_node

echo "==> logging in"
login_status="$(curl -fsS -o /dev/null -w '%{http_code}' -c "$cookie_jar" \
  -H 'Content-Type: application/json' \
  -d "{\"password\":\"$admin_password\"}" \
  "$base_url/api/v1/login")"
if [[ "$login_status" != "200" ]]; then
  echo "login failed (HTTP $login_status)" >&2
  exit 1
fi

echo "==> creating a self-probing TCP check (every 5s, the schedule minimum)"
create_status="$(curl -fsS -o "$work_dir/create.json" -w '%{http_code}' -b "$cookie_jar" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"gate-smoke\",\"type\":\"tcp\",\"target\":\"127.0.0.1:$port\",\"cron\":\"*/5 * * * * *\",\"timeout_secs\":3}" \
  "$base_url/api/v1/admin/checks")"
if [[ "$create_status" != "200" && "$create_status" != "201" ]]; then
  echo "check creation failed (HTTP $create_status):" >&2
  cat "$work_dir/create.json" >&2
  exit 1
fi

echo "==> minting an agent credential so the embedded agent can authenticate"
mint_status="$(curl -fsS -o "$work_dir/mint.json" -w '%{http_code}' -b "$cookie_jar" \
  -H 'Content-Type: application/json' \
  -d '{"zone":"unknown","region":"unknown"}' \
  "$base_url/api/v1/admin/agent-credentials")"
if [[ "$mint_status" != "200" && "$mint_status" != "201" ]]; then
  echo "minting agent credential failed (HTTP $mint_status):" >&2
  cat "$work_dir/mint.json" >&2
  exit 1
fi
agent_token="$(jq -r '.token' "$work_dir/mint.json")"
if [[ -z "$agent_token" || "$agent_token" == "null" ]]; then
  echo "could not parse minted agent token" >&2
  cat "$work_dir/mint.json" >&2
  exit 1
fi

echo "==> restarting with the agent credential so probing can start"
stop_node
write_config "$agent_token"
start_node

echo "==> waiting for the embedded agent to pick up the check and probe it"
have_results=0
for _ in $(seq 1 40); do
  count_json="$(curl -fsS -b "$cookie_jar" "$base_url/api/v1/admin/results/count" || true)"
  if [[ "$count_json" =~ [1-9] ]]; then
    have_results=1
    break
  fi
  sleep 1
done
if [[ "$have_results" -ne 1 ]]; then
  echo "no results recorded after waiting; warden log:" >&2
  cat "$work_dir/warden.log" >&2
  exit 1
fi

echo "==> stopping the node so the backup gets an exclusive view of the database"
stop_node

backup_path="$work_dir/backup.duckdb"
restore_path="$work_dir/restored.duckdb"

echo "==> warden backup --output $backup_path"
"$WARDEN_BIN" backup --config "$config_path" --output "$backup_path"

echo "==> warden restore --from $backup_path --into $restore_path"
restore_output="$("$WARDEN_BIN" restore --from "$backup_path" --into "$restore_path")"
echo "$restore_output"

if [[ ! -f "$restore_path" ]]; then
  echo "restore did not produce $restore_path" >&2
  exit 1
fi
if [[ "$restore_output" != *"checksum_verified=true"* ]]; then
  echo "restore did not verify the backup checksum" >&2
  exit 1
fi
if [[ "$restore_output" =~ raw_results=([0-9]+) ]]; then
  raw_results="${BASH_REMATCH[1]}"
  if [[ "$raw_results" -lt 1 ]]; then
    echo "restored database holds no raw results" >&2
    exit 1
  fi
else
  echo "could not parse raw_results from restore output" >&2
  exit 1
fi

echo "==> backup/restore smoke test passed (raw_results=$raw_results)"
