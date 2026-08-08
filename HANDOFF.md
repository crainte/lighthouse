# Warden — session handoff

Repo: `~/code/uptime`. HEAD `28e9f45`, plus **7 uncommitted files** (below). Everything verified; nothing outstanding.

## Original request

Optional latency graphs styled like <https://meta.hyperping.app/>, copying date switching, collapsing sections, and the hover latency readout. **All three verified live in a browser** — see evidence below.

## Verified (2026-08-07, live)

| Gate | Result |
|---|---|
| `cargo test` (with `WARDEN_AGENT_TOKEN` **exported**) | 75 + 23 + 1 + 1 passed, 0 failed |
| `cargo fmt --check` | clean |
| `cargo clippy --all-targets` | 0 warnings |
| `npx svelte-check --threshold error` | 212 files, **0 errors** |
| `npx vitest run` | 22 passed |
| `npm run build` | `index-sF5RM8Ej.js` |
| `docker compose up -d --build` | coordinator, agent-a, agent-b all `Up` |
| served bundle vs `dist` | `index-sF5RM8Ej.js` — **match**, no stale rust-embed |

### Feature evidence (headless Chromium on `:8082`)

- **Range switching** — 4 buttons; each range draws a genuinely different axis:
  - `24h` → 600 bars, ticks `12 PM … 12 PM`
  - `7d` → 269 bars, `Jul 31 … Aug 7`
  - `30d` → 65 bars, `Jul 8 … Aug 7`
  - `90d` → 23 bars, `May 9 … Aug 6`
  This is the regression that previously made all four windows identical (`24h=17405000 7d=17405000 30d=17405000`) — a clamp narrowed every window to the data extent. It is gone.
- **Hover readout** — cursor at 40 % → `no data · Jun 13, 06:58 PM`, `.lat-cursor` at `left: 39.98%`; at 78 % → `Jul 18, 06:58 AM` / `77.95%`. On `24h`: `25ms · 09:10 AM` with the hovered bar highlighted (screenshot `/tmp/warden-24h.png`).
- **Collapsing sections** — `.group-head` for "Warden Internals" round-trips `5 → 7 → 5` visible `.component`s.
- **No** `"Collecting latency data…"` placeholder anywhere (the old `enoughData` guard bug).
- Graphs appear only for checks with `latency_graph = true` (5 of 7 seeded checks); `coordinator-port` / `coordinator-self` correctly have none.

## Uncommitted changes

| File | Why |
|---|---|
| `tests/ws_api.rs` | env-isolation fix (below) + fmt |
| `frontend/src/lib/ws.ts` | contract-drift fix (below) |
| `src/agent.rs`, `src/coordinator/{mod,state,storage}.rs`, `tests/admin_api.rs` | `cargo fmt` only — HEAD was fmt-dirty |

### Fix 1 — `tests/ws_api.rs:62`

Added `std::env::remove_var("WARDEN_AGENT_TOKEN")` before `Coordinator::new`.

`Coordinator::new` → `cfg.resolved_agent_token()` falls back to the `WARDEN_AGENT_TOKEN` env var, so `agent_token: None` did **not** mean "no auth". On any machine exporting it (the docker-compose `.env` workflow encourages it) the test's unauthenticated `POST /api/v1/results` got `401` instead of `200`. The test now passes with the var still exported — that is the proof the fix is real and not an `env -u` workaround.

Gotchas for whoever edits this:
- Edition is **2021** — `remove_var` is safe; do **not** wrap it in `unsafe` (`unused_unsafe` fails the lint gate).
- That binary holds exactly **one** `#[tokio::test]`, so the process-global mutation has no cross-test race. Adding a second test there means switching to a non-global approach.
- `tests/e2e.rs:107` has the same `agent_token: None` but authenticates its requests, so it is unaffected. Checked, not assumed.

### Fix 2 — `frontend/src/lib/ws.ts:40-42, 51-52`

`parseIncident` did not carry `acknowledged_at` / `acknowledged_by`, so its return type no longer satisfied `Incident` after the incident-acknowledge work landed — svelte-check error. Now validated and mapped, using `!= null` so an older coordinator that omits the fields still parses instead of dropping the event.

## If you pick this up

Nothing is blocked. Sensible next moves, in the design doc but out of scope for this ask: Lua eval hooks, Slack/Email/PagerDuty notifiers, gossip clustering.

## Ground rules worth keeping

- Never weaken a test to get green. The `ws_api` change removes an ambient-env dependency; it relaxes no assertion.
- Verify against **live JSON on :8082**, not the TS types — `api.ts` and the Rust DTOs drift (that is exactly what Fix 2 was).
- Always re-check the served bundle hash against `frontend/dist` after a rebuild; `rust-embed` does not reliably re-embed. If stale: `cargo clean -p warden`.
- Wait **≥55 s** after `docker compose up`; `seed_mock` does 18–26 s of DuckDB inserts and 15 s readiness probes report false failures.
- Svelte `{#each}` keys must be unique — duplicates silently hang the page. Zone/region names repeat, so keys must be composite.
- Do not hand subagents images; pass context as `local://*.md`.
