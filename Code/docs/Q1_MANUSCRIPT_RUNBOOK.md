# Q1 Forecasting Manuscript Runbook

> Historical note: this Q1 runbook is retained for provenance. For the current
> cleaned repository layout, use the root `README.md` and
> `docs/Q2_MANUSCRIPT_ROADMAP.md`.

This runbook is the canonical path for the Q1 forecasting/econometrics upgrade.

## Validated Short Run

The short validation freeze has passed with SPF and the Q1 evidence tables:

```bash
/opt/anaconda3/bin/python -m pytest -q tests
/opt/anaconda3/bin/python -m full_state_space_release_revision_dfm.run_smoke_tests
/opt/anaconda3/bin/python scripts/12_run_journal_candidate_pipeline.py \
  --max-origins 1 \
  --max-iters 1 \
  --skip-mature-robustness \
  --midas-lags 6 \
  --mcs-bootstrap-reps 10 \
  --spf-forecasts data/external/spf/spf_rgdp_growth_benchmark.csv \
  --freeze-name q1_validation_smoke
```

Validation freeze:

```text
outputs/full_state_space_release_revision_dfm/frozen/q1_validation_smoke/
```

## Build Public SPF Benchmark

```bash
/opt/anaconda3/bin/python scripts/13_build_public_spf_benchmark.py
```

Output:

```text
data/external/spf/spf_rgdp_growth_benchmark.csv
```

The SPF availability proxy is conservative: the survey is treated as available at the end of the second month of its survey quarter.

## Official Q1 Freeze

This is the full manuscript-grade run. It is computationally heavy because every full-sample forecast origin refits several Kalman/EM models.

```bash
/opt/anaconda3/bin/python scripts/12_run_journal_candidate_pipeline.py \
  --max-origins 0 \
  --max-iters 50 100 \
  --mature-max-iter 100 \
  --mature-variants mature_1y mature_3y mature_latest \
  --midas-lags 6 \
  --mcs-bootstrap-reps 5000 \
  --spf-forecasts data/external/spf/spf_rgdp_growth_benchmark.csv \
  --run-initialization-audit \
  --initialization-seeds 1 2 3 4 5 \
  --initialization-max-iter 100 \
  --initialization-max-origins 24 \
  --run-q1-sensitivity \
  --sensitivity-max-iter 100 \
  --sensitivity-mcs-bootstrap-reps 1000 \
  --factor-grid 1 2 3 \
  --midas-lag-grid 4 6 9 \
  --window-modes expanding rolling \
  --exclude-covid-sensitivity \
  --freeze-name q1_manuscript_v1
```

Official freeze:

```text
outputs/full_state_space_release_revision_dfm/frozen/q1_manuscript_v1/
```

## Pragmatic Overnight Alternative

If the full grid is too slow, run this first and treat it as `q1_manuscript_v1_core`. It keeps full sample, SPF, mature robustness, convergence, and initialization, while deferring sensitivity runs.

```bash
/opt/anaconda3/bin/python scripts/12_run_journal_candidate_pipeline.py \
  --max-origins 0 \
  --max-iters 50 100 \
  --mature-max-iter 100 \
  --mature-variants mature_1y mature_3y mature_latest \
  --midas-lags 6 \
  --mcs-bootstrap-reps 5000 \
  --spf-forecasts data/external/spf/spf_rgdp_growth_benchmark.csv \
  --run-initialization-audit \
  --initialization-seeds 1 2 3 4 5 \
  --initialization-max-iter 100 \
  --initialization-max-origins 24 \
  --freeze-name q1_manuscript_v1_core
```

## Manuscript Evidence Files

Use these files inside the accepted freeze:

- `HEADLINE_POINT_WINNERS_FROM_FREEZE.csv`
- `HEADLINE_REVISION_WINNERS_FROM_FREEZE.csv`
- `FREEZE_FAILURE_AUDIT.csv`
- `STATE_SPACE_CONVERGENCE_MAIN_MAX_ITER100.csv`
- `STATE_SPACE_VARIANCE_POINT_MAIN_MAX_ITER100.csv`
- `STATE_SPACE_VARIANCE_REVISION_MAIN_MAX_ITER100.csv`
- `STATE_SPACE_COVARIANCE_SUMMARY_MAIN_MAX_ITER100.csv`
- `runs/*/evidence_dir/bootstrap_loss_difference_point.csv`
- `runs/*/evidence_dir/bootstrap_loss_difference_revision.csv`
- `runs/*/evidence_dir/common_sample_spf_point.csv`
- `runs/*/evidence_dir/external_benchmark_coverage.csv`
- `runs/*/evidence_dir/density_point_metrics.csv`
- `runs/*/evidence_dir/density_revision_metrics.csv`
- `runs/*/evidence_dir/revision_threshold_diagnostics.csv`
- `runs/*/evidence_dir/revision_magnitude_bins.csv`
- `runs/*/evidence_dir/release_mechanism_analysis.csv`

## Q1 Narrative Rule

Do not frame the paper as "Kalman always wins." The defensible Q1 claim is:

> Real-time GDP nowcasting is timing- and target-dependent. Before the advance release, monthly information dominates. Before second and third releases, no-revision is a hard point-forecast benchmark. Release-ladder state-space models add value through uncertainty calibration, revision-risk diagnostics, mature-target robustness, and mechanism evidence.
