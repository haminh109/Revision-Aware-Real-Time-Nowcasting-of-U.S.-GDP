# Stage 2 checklist

Use this checklist before treating the silver layer as complete.

## A. Scope discipline

- [ ] Stage 2 remains a semantic/governance layer, not a modeling layer
- [ ] Release-stage GDP targets and complete-vintage GDP history remain explicitly separate target objects
- [ ] Indicator metadata records release-block mapping status rather than hiding uncertainty
- [ ] Calendar coverage limitations are encoded as machine-readable metadata
- [ ] Census proxy timing remains proxy-only and blank in `release_time_et`
- [ ] No exact daily or intraday timing is invented for RTDSM targets or quarter-coded vintage history

## B. Build order

- [ ] `python scripts/05_build_target_definition_tables.py`
- [ ] `python scripts/06_build_indicator_metadata.py`
- [ ] `python scripts/07_build_release_taxonomy_and_mappings.py`
- [ ] `python scripts/08_build_calendar_coverage_metadata.py`
- [ ] `python scripts/09_build_silver_curated_layer.py`

## C. Expected silver artifacts

- [ ] `data/silver/targets/target_definition_table.csv`
- [ ] `data/silver/targets/gdp_release_stage_silver.csv`
- [ ] `data/silver/targets/gdp_complete_vintages_silver.csv`
- [ ] `data/silver/indicators/indicator_metadata.csv`
- [ ] `data/silver/indicators/indicator_release_map.csv`
- [ ] `data/silver/calendars/release_block_taxonomy.csv`
- [ ] `data/silver/calendars/release_calendar_silver.csv`
- [ ] `data/silver/calendars/calendar_coverage_metadata.csv`
- [ ] `data/silver/governance/source_limitations_registry.csv`

## D. Semantic checks

- [ ] `target_definition_table.csv` separates RTDSM release-stage GDP growth targets from RTDSM complete-vintage GDP history
- [ ] `gdp_release_stage_silver.csv` keeps blank `release_date` because the raw source does not provide row-level dates
- [ ] `gdp_complete_vintages_silver.csv` keeps quarter-coded vintage semantics and does not fabricate daily vintage dates
- [ ] `indicator_metadata.csv` includes every required Stage 1 ALFRED series
- [ ] `indicator_release_map.csv` distinguishes official support, proxy support, and current unmapped status
- [ ] `release_calendar_silver.csv` preserves `source_type`, `coverage_status`, and `release_time_status`
- [ ] `calendar_coverage_metadata.csv` explicitly records snapshot-only, date-only, and proxy constraints
- [ ] `source_limitations_registry.csv` records the core Stage 1 limitations as active downstream constraints

## E. Known limitations that must remain visible

- [ ] RTDSM release-stage growth targets and RTDSM complete-vintage history are not directly interchangeable
- [ ] BEA and BLS calendar artifacts remain coverage-limited in the current repo
- [ ] Federal Reserve G.17 dates are official but time-of-day remains unavailable
- [ ] Census timing remains ALFRED-based proxy logic, not official Census timestamp truth
- [ ] Some indicators remain currently unmapped for release-calendar support and must not be forced into Stage 3 joins

## F. Validation gate

- [ ] `python scripts/validate_stage2.py` returns PASS
- [ ] `data/metadata/stage2_validation_report.json` is written successfully

## G. Exit condition

Only move to Stage 3 after the silver layer passes validation and the machine-readable coverage/limitation artifacts are accepted as the governing semantic contract for exact-vintage information-set construction.
