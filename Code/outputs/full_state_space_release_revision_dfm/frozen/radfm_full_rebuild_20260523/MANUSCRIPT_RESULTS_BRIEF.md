# Q1 Manuscript Freeze Brief

Generated UTC: `2026-05-23T22:50:11+00:00`

## Interpretation

- Treat this freeze as the source of manuscript tables only after checking `FREEZE_FAILURE_AUDIT.csv`.
- The Q1 narrative should compare S/T results against no-revision first, then use density, revision diagnostics, mature robustness, and mechanism evidence for state-space value.
- SPF is included only if `MANIFEST.json` points to a built or supplied public SPF benchmark file.

## Key Files

- `MANIFEST.json`: run configuration, git metadata, copied artifact map.
- `HEADLINE_POINT_WINNERS_FROM_FREEZE.csv` and `HEADLINE_REVISION_WINNERS_FROM_FREEZE.csv`: summary only, not the sole evidence.
- `runs/*/evidence_dir/`: DM/CW/MCS/bootstrap/density/revision/mechanism tables.
- `FREEZE_FAILURE_AUDIT.csv`: run-level failure counts.
