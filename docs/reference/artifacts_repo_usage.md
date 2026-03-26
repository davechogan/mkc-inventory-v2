# Shared Artifacts Repo Usage

This project uses a separate repository for non-release artifacts:

- Artifacts repo: `https://github.com/davechogan/Artifacts.git`

Use this for:
- plans and deep implementation notes you do not want in release clones
- project-local utility scripts not required by runtime release
- DB snapshots, exports, logs, and investigation artifacts

Do not use this for:
- production application source intended for runtime deployment
- secrets or credentials

## Standard layout for each project

```text
projects/<project-slug>/
  README.md
  plans/
  scripts/
  db_snapshots/
  exports/
  logs/
  metadata/
```

## Create a new project in Artifacts repo

1. Clone Artifacts repo locally (one-time):

```bash
git clone https://github.com/davechogan/Artifacts.git
```

2. Run scaffold script from this project:

```bash
scripts/artifacts_project_scaffold.sh /path/to/Artifacts <project-slug>
```

3. Add initial artifacts and commit in Artifacts repo:

```bash
git -C /path/to/Artifacts add projects/<project-slug>
git -C /path/to/Artifacts commit -m "[artifacts] initialize <project-slug> structure"
git -C /path/to/Artifacts push origin main
```

## DB snapshot guidance

- Compress snapshots before committing (for example, `zstd` or `gzip`).
- Keep `SHA256SUMS.txt` per snapshot batch.
- Add provenance metadata under `metadata/`.
- Never store live secrets with snapshots.
