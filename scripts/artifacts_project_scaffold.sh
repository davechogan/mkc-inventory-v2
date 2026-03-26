#!/usr/bin/env bash
set -euo pipefail

# Create a standardized project folder in the shared Artifacts repository.
#
# Usage:
#   scripts/artifacts_project_scaffold.sh /path/to/Artifacts mkc-inventory-v2
#
# Optional env:
#   SOURCE_REPO_URL="https://github.com/org/repo.git"
#   PROJECT_OWNER="owner-name"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <artifacts_repo_path> <project_slug>" >&2
  exit 1
fi

ARTIFACTS_ROOT="$1"
PROJECT_SLUG="$2"
PROJECT_ROOT="$ARTIFACTS_ROOT/projects/$PROJECT_SLUG"

if [[ ! -d "$ARTIFACTS_ROOT/.git" ]]; then
  echo "ERROR: $ARTIFACTS_ROOT does not look like a git repository root." >&2
  exit 1
fi

mkdir -p \
  "$PROJECT_ROOT/plans" \
  "$PROJECT_ROOT/scripts" \
  "$PROJECT_ROOT/db_snapshots" \
  "$PROJECT_ROOT/exports" \
  "$PROJECT_ROOT/logs" \
  "$PROJECT_ROOT/metadata"

README_PATH="$PROJECT_ROOT/README.md"
PROVENANCE_PATH="$PROJECT_ROOT/metadata/provenance.template.json"

if [[ ! -f "$README_PATH" ]]; then
  cat > "$README_PATH" <<EOF
# $PROJECT_SLUG artifacts

## Source repository
- URL: ${SOURCE_REPO_URL:-"<set-source-repo-url>"}
- Owner: ${PROJECT_OWNER:-"<set-owner>"}

## Structure
- plans/: architecture plans and implementation specs
- scripts/: utility scripts not intended for release app bundle
- db_snapshots/: compressed DB snapshots + checksums
- exports/: CSV/JSON exports for analysis
- logs/: diagnostic logs retained for incident analysis
- metadata/: provenance and restore metadata

## Snapshot conventions
- Use dated folders under db_snapshots: YYYY-MM-DD/
- Store compressed snapshots (for example, .zst or .gz)
- Include SHA256SUMS.txt with checksums
- Include provenance JSON for each snapshot batch

## Restore notes
Document project-specific restore commands and prerequisites here.
EOF
fi

if [[ ! -f "$PROVENANCE_PATH" ]]; then
  cat > "$PROVENANCE_PATH" <<'EOF'
{
  "project_slug": "<project-slug>",
  "source_repo_url": "<source-repo-url>",
  "source_commit": "<git-sha>",
  "created_at_utc": "<YYYY-MM-DDTHH:MM:SSZ>",
  "created_by": "<name-or-id>",
  "artifact_set": "<human-readable-description>",
  "artifacts": [
    {
      "path": "<relative/path/to/artifact>",
      "sha256": "<sha256-hex>"
    }
  ]
}
EOF
fi

echo "Scaffold created at: $PROJECT_ROOT"
echo "Next steps:"
echo "  1) Review README.md and provenance template."
echo "  2) Add initial artifacts."
echo "  3) Commit in Artifacts repo:"
echo "     git -C \"$ARTIFACTS_ROOT\" add \"projects/$PROJECT_SLUG\""
echo "     git -C \"$ARTIFACTS_ROOT\" commit -m \"[artifacts] initialize $PROJECT_SLUG structure\""
