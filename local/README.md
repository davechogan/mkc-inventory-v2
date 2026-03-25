# Local workspace (not tracked by Git)

Put **machine-specific or disposable** artifacts here so they never land in the repository:

- Large exports (CSVs, ZIPs, DB copies), one-off downloads, email extracts
- Personal notes, scratch SQL, experiment scripts, backup copies of files (e.g. `*.old`)
- Anything you would not want on another machine or in CI

Create subfolders as needed, for example `exports/`, `scratch/`, `notes/`. **Everything under `local/` except this file is ignored by Git.**

## Do **not** put here (keep in normal project paths)

- Application and library code (`app.py`, `*.py` modules at repo root, shared `tools/` scripts you want teammates to run)
- `static/`, `scripts/` used by the app or CI
- Documentation you intend to version (`README.md` at repo root, everything else under `docs/`)

If you are unsure whether something belongs in `local/`, prefer keeping it in the main tree and we can adjust `.gitignore` or layout in a follow-up.
