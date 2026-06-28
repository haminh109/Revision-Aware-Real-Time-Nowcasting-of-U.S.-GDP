# Journal Candidate Runbook

This runbook is the operational checklist for producing a frozen
journal-candidate evidence package from the current RADFM repository.

## 1. Build Timing And Mature-Target Inputs

```bash
python scripts/10_build_gdp_release_calendar_from_alfred.py
python scripts/11_build_mature_target_robustness_panels.py
```

Expected outputs:

- `data/silver/calendars/gdp_release_calendar_alfred.csv`
- `data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv`
- `data/bronze/targets/robustness/gdp_release_targets_mature_3y.csv`
- `data/bronze/targets/robustness/gdp_release_targets_mature_latest.csv`

## 2. Run The Full High-Iteration Grid

Minimum journal grid:

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 50 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter50

python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 100 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100
```

If the joint model still reaches the iteration cap and RMSE/log-score stability is not clear, add:

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 200 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter200
```

## 3. Build Report, Evidence, And Variance Packages

For each high-iteration run:

```bash
python -m full_state_space_release_revision_dfm.build_report_package \
  --source-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100_report_package

python -m full_state_space_release_revision_dfm.build_journal_evidence_package \
  --source-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100_journal_evidence \
  --mcs-bootstrap-reps 5000

python -m full_state_space_release_revision_dfm.build_variance_audit \
  --source-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter100_variance_audit
```

## 4. Run Mature-Target Robustness

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 50 \
  --gdp-release-targets data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_mature_1y_max_iter50

python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 50 \
  --gdp-release-targets data/bronze/targets/robustness/gdp_release_targets_mature_3y.csv \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_mature_3y_max_iter50

python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 50 \
  --gdp-release-targets data/bronze/targets/robustness/gdp_release_targets_mature_latest.csv \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_mature_latest_max_iter50
```

Build report/evidence/variance packages for each mature-target run before using the results in the paper.

## 5. Run Multi-Initialization Audit

```bash
python -m full_state_space_release_revision_dfm.run_initialization_audit \
  --seeds 1 2 3 4 5 \
  --max-origins 0 \
  --max-iter 50 \
  --initialization-jitter 0.02 \
  --output-root outputs/full_state_space_release_revision_dfm/initialization_audit_max_iter50
```

Use a smaller `--max-origins` only for smoke testing. For paper claims, use `--max-origins 0`.

## 6. Build Stability Tables

```bash
python -m full_state_space_release_revision_dfm.build_convergence_stability_table \
  --output-root outputs/full_state_space_release_revision_dfm \
  --output-dir outputs/full_state_space_release_revision_dfm/convergence_stability_journal_candidate
```

The paper can claim fully converged EM only if the convergence table supports that wording. Otherwise, use the safer wording: high-iteration fixed-cap EM estimates with convergence diagnostics.

## 7. One-Command Journal Candidate Pipeline

The orchestration script runs the main steps above and writes a frozen manifest:

```bash
python scripts/12_run_journal_candidate_pipeline.py \
  --max-iters 50 100 \
  --max-origins 0 \
  --mcs-bootstrap-reps 5000 \
  --mature-max-iter 50 \
  --mature-variants mature_1y mature_3y mature_latest \
  --run-initialization-audit \
  --initialization-seeds 1 2 3 4 5 \
  --initialization-max-iter 50 \
  --initialization-max-origins 0 \
  --freeze-name full_state_space_journal_candidate_20260425
```

This command is compute-intensive. Use the smoke version first:

```bash
python scripts/12_run_journal_candidate_pipeline.py \
  --max-iters 3 \
  --max-origins 1 \
  --mcs-bootstrap-reps 99 \
  --mature-max-iter 2 \
  --mature-variants mature_1y \
  --run-initialization-audit \
  --initialization-seeds 1 2 \
  --initialization-max-iter 3 \
  --initialization-max-origins 1 \
  --freeze-name smoke_journal_candidate_freeze
```

## 8. Acceptance Criteria

- `failures.csv` is empty for all headline runs.
- State-space rows have finite positive `forecast_variance` and `forecast_sd`.
- `covariance_matrix_audit.csv` reports `share_psd = 1.0` for state-space models.
- High-iteration RMSE/log-score rankings are stable from `max_iter=50` to `max_iter=100`.
- Mature-target robustness does not overturn the main third-release conclusion.
- Multi-initialization RMSE ranges are small enough to rule out seed-driven conclusions.
- The final paper does not claim model dominance at the advance release.
