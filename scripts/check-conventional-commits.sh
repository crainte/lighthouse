#!/usr/bin/env bash
# Require conventional commit messages on everything a pull request can land.
#
# git-cliff derives the version and the changelog from conventional commits
# and silently drops any that are not, so one stray "Fix thing" on main breaks
# the release script. The local commit-msg hook catches it at commit time; this
# is the gate for commits made without the hook, and for the PR title, which
# GitHub uses as the commit message on a squash merge.
#
# Each message goes through `hk util check-conventional-commit`, the same
# checker as the commit-msg hook in hk.pkl, whose default types are exactly the
# ones cliff.toml groups. One definition, so the three cannot drift.
#
# Environment (the commits checked are the non-merge ones the head adds):
#   HEAD_SHA   the head to check. Default: HEAD.
#   BASE_REF   the branch it will merge into, compared as origin/<BASE_REF> as
#              it is now. CI sets this: the base SHA recorded in the event goes
#              stale when the base branch moves, and would pull the base
#              branch's own commits into the range.
#   BASE_SHA   an explicit lower bound instead, for local runs. Wins over BASE_REF.
#   (neither)  origin/main, else main.
#   PR_TITLE   also check this (the squash-merge message). Optional.
#
# Usage: scripts/check-conventional-commits.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if ! command -v hk >/dev/null 2>&1; then
  echo "error: hk is not installed (mise install)" >&2
  exit 1
fi

head_sha="${HEAD_SHA:-HEAD}"
base_sha="${BASE_SHA:-}"
if [ -z "$base_sha" ]; then
  if [ -n "${BASE_REF:-}" ]; then
    candidates=("origin/$BASE_REF")
  else
    candidates=(origin/main main)
  fi
  for ref in "${candidates[@]}"; do
    if git rev-parse -q --verify "$ref" >/dev/null; then
      base_sha="$(git merge-base "$ref" "$head_sha")"
      break
    fi
  done
fi
if [ -z "$base_sha" ]; then
  if [ -n "${CI:-}" ]; then
    echo "error: no base commit to compare against (set BASE_REF or BASE_SHA)" >&2
    exit 1
  fi
  echo "warning: no origin/main or main to compare against; set BASE_REF or BASE_SHA" >&2
  exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT
msg="$work/message"

bad=0
checked=0
report() { # <label> <first line of the message>
  printf '  %s\n      %s\n' "$1" "$2" >&2
  bad=$((bad + 1))
}

while read -r sha; do
  [ -n "$sha" ] || continue
  checked=$((checked + 1))
  git log -1 --format=%B "$sha" >"$msg"
  if ! hk util check-conventional-commit "$msg" >/dev/null 2>&1; then
    report "commit $(git rev-parse --short "$sha")" "$(head -n 1 "$msg")"
  fi
done < <(git rev-list --no-merges --reverse "$base_sha..$head_sha")

if [ -n "${PR_TITLE:-}" ]; then
  checked=$((checked + 1))
  printf '%s\n' "$PR_TITLE" >"$msg"
  if ! hk util check-conventional-commit "$msg" >/dev/null 2>&1; then
    report "pull request title" "$PR_TITLE"
  fi
fi

if [ "$bad" -gt 0 ]; then
  cat >&2 <<'EOF'

error: the messages above are not conventional commits.

Use `type(scope): description`, where type is one of
build, chore, ci, docs, feat, fix, perf, refactor, revert, style, test
(scope optional; `!` after it marks a breaking change), e.g.
  fix(storage): close the database worker before exit

Release notes and the version bump are generated from these messages, and
anything else is dropped from them. To fix:
  - pull request title: edit it on GitHub (this check re-runs on edit). A
    squash merge uses it as the commit message.
  - a commit: `git rebase -i` and reword it, then force-push the branch.
GitHub's own "Revert ..." messages are not conventional either; use
`revert: <what was reverted>`.
EOF
  exit 1
fi

echo "ok: $checked message(s) are conventional commits" >&2
