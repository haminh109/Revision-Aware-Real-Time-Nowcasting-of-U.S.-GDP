# Stage 1 checklist

Use this checklist before treating the bronze layer as complete.

## A. Scope discipline

- [ ] Stage 0 has been revalidated with semantic checks, not only path checks
- [ ] RTDSM release-stage and complete-vintage files are preserved with their raw source semantics
- [ ] ALFRED wide-vintage matrices are normalized without inventing unsupported fields
- [ ] Census proxy timing remains explicitly labeled as proxy and never as official timestamp truth
- [ ] No downstream harmonization assumptions are silently pushed into bronze outputs

## B. Bronze build order

- [ ] `python scripts/01_parse_rtdsm_release_specific.py`
- [ ] `python scripts/02_parse_rtdsm_complete_vintages.py`
- [ ] `python scripts/03_parse_alfred_monthly.py`
- [ ] `python scripts/04_build_release_calendar_master.py`

## C. Expected bronze artifacts

- [ ] `data/bronze/targets/gdp_release_targets.csv`
- [ ] `data/bronze/targets/gdp_complete_vintages_long.csv`
- [ ] `data/bronze/indicators/alfred_monthly_long.csv`
- [ ] `data/bronze/calendars/release_calendar_master.csv`

## D. Artifact-specific semantic checks

- [ ] `gdp_release_targets.csv` keeps the RTDSM release-stage structure (`first`, `second`, `third`, `most_recent`) and leaves `release_date` blank because the raw workbook does not provide row-level dates
- [ ] `gdp_complete_vintages_long.csv` preserves RTDSM quarter-coded vintage labels as `vintage_period` and does not fabricate exact vintage dates
- [ ] `alfred_monthly_long.csv` preserves `realtime_start`, derives `realtime_end` from ordered vintage dates when the raw schema is wide, keeps `value_raw`, and marks missing-value sentinels explicitly
- [ ] `release_calendar_master.csv` carries `source_type`, `coverage_scope`, and `release_time_status` so official, missing-time, and proxy cases remain distinguishable
- [ ] Census proxy rows have blank `release_time_et` and `release_time_status = proxy_not_official`

## E. Known bronze-layer limitations to keep explicit

- [ ] RTDSM release-stage data are q/q annualized growth rates, while the RTDSM complete-vintage workbook is a separate raw numeric history that looks level-like; harmonization belongs to Stage 2, not Stage 1
- [ ] BEA and BLS official calendar artifacts currently reflect current-year or scheduled raw pages in this repo, not yet a completed historical calendar archive
- [ ] The bronze calendar master is therefore a faithful normalization of current raw artifacts, not a finished historical release-calendar panel for exact-vintage evaluation

## F. Validation gate

- [ ] `python scripts/validate_stage1.py` returns PASS
- [ ] `data/metadata/stage1_validation_report.json` is written successfully

## G. Exit condition

Only move to Stage 2 after the bronze artifacts pass validation and the known limitations above are documented rather than silently ignored.
