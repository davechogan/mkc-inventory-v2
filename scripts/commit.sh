#!/usr/bin/env bash
# commit.sh — commit to both the main repo and the artifacts submodule
# with the same message. Use this instead of bare `git commit`.
#
# Usage:
#   scripts/commit.sh "your commit message"
#
# Both the main repo and artifacts submodule will receive the same message.
# If artifacts has no staged changes, only the main repo is committed.

set -e

ROOT="$(git rev-parse --show-toplevel)"
ARTIFACTS="$ROOT/artifacts"

if [ -z "$1" ]; then
  echo "Usage: scripts/commit.sh 'commit message'"
  exit 1
fi

MESSAGE="$1"

# --- Artifacts submodule ---
if [ -d "$ARTIFACTS" ]; then
  ARTIFACTS_STATUS="$(git -C "$ARTIFACTS" status --porcelain 2>/dev/null)"
  if [ -n "$ARTIFACTS_STATUS" ]; then
    echo "→ Committing artifacts submodule..."
    git -C "$ARTIFACTS" add -A
    git -C "$ARTIFACTS" commit -m "$MESSAGE"
    git -C "$ROOT" add artifacts
    echo "✓ Artifacts committed and pointer staged."
  fi
fi

# --- Main repo ---
echo "→ Committing main repo..."
git -C "$ROOT" commit -m "$MESSAGE"
echo "✓ Done."
