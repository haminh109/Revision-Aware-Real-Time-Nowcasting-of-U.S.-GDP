# Q2 Manuscript Roadmap

This document describes the current RADFM manuscript workflow. The repository
keeps the full-state Kalman/EM release-revision implementation and adds the Q2
benchmark layer needed for a defensible manuscript.

## Current Claim Discipline

Allowed baseline claim:

```text
The paper estimates a joint quarterly release-revision state-space system by
Kalman filtering, smoothing, and EM, using vintage-correct monthly information
aggregated into release-origin-specific quarterly inputs.
```

Allowed only as an experimental robustness/candidate model until full-sample
diagnostics are frozen:

```text
The paper estimates a monthly mixed-frequency Kalman/EM benchmark with monthly
indicator observations and sparse quarter-end GDP release observations.
```

Reason: `monthly_mixed_frequency_kalman_em` now keeps the state equation
monthly and enters GDP releases as sparse quarter-end observations. It is still
not ready for the headline claim until the full-sample run verifies convergence,
forecast stability, and the timing audit. The older release-revision Kalman
models remain quarterly release-ladder models fed by vintage-correct quarterly
factor inputs.

## Q2 Benchmark Layer

The exact/pseudo runner now supports:

- `no_revision`: revision benchmark with `S=A` before the second release and
  `T=S` before the third release.
- `midas_umidas`: unrestricted MIDAS benchmark using separate monthly lag
  coefficients from the origin-specific monthly indicator panel.
- `spf`: optional external professional benchmark loaded from CSV.
- `monthly_mixed_frequency_kalman_em`: experimental mixed-frequency Kalman/EM
  benchmark with monthly indicator observations and sparse GDP release
  observations.

SPF CSV schema:

```text
forecast_origin_date,target_quarter,target_id,forecast_value
2024-02-15,2024:Q1,A,2.1
```

The SPF benchmark is intentionally optional so the repo remains reproducible
without proprietary or manually curated professional-forecast data.

## Recommended Journal Candidate Run

First run a small smoke check:

```bash
python -m full_state_space_release_revision_dfm.run_smoke_tests
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 2 \
  --max-iter 5 \
  --output-dir outputs/full_state_space_release_revision_dfm/smoke_q2
```

Then run the full candidate build:

```bash
python scripts/10_build_gdp_release_calendar_from_alfred.py

python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 50 \
  --midas-lags 6 \
  --output-dir outputs/full_state_space_release_revision_dfm/q2_candidate_max_iter50

python -m full_state_space_release_revision_dfm.build_report_package \
  --source-dir outputs/full_state_space_release_revision_dfm/q2_candidate_max_iter50 \
  --output-dir outputs/full_state_space_release_revision_dfm/q2_candidate_report_package

python -m full_state_space_release_revision_dfm.build_journal_evidence_package \
  --source-dir outputs/full_state_space_release_revision_dfm/q2_candidate_max_iter50 \
  --output-dir outputs/full_state_space_release_revision_dfm/q2_candidate_journal_evidence
```

If SPF data are available, add:

```bash
--spf-forecasts data/external/spf_forecasts.csv
```

to the `exact_pseudo_backtest` command.

## Remaining Q2 Hardening

- Rerun full-sample `max_iter=50` and `max_iter=100`; use the higher-quality run
  as the frozen manuscript source.
- Keep convergence diagnostics in the main appendix. If strict tolerance is not
  reached, describe estimates as fixed-iteration EM/Kalman estimates.
- Add official BEA archive cross-checks for ALFRED-derived GDP release dates.
- Add thresholded revision sign analysis for `|revision| > 0.1` and `> 0.2`.
- Do not mix historical branch/folder outputs with the current canonical
  `RADFM/main` outputs.
