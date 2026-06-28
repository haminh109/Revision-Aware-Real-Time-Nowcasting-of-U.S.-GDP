# Full State-Space Release-Revision DFM

This folder is the modeling core for the paper-project working copy: a Kalman/EM release-ladder state-space model plus manuscript-grade benchmark and evidence builders.

It is intentionally separate from the existing staged data pipeline. The current course-project report should still describe the reported empirical results as a transparent factor-regression approximation unless this module is later connected to the real-time backtest and the results are regenerated.

## What This Implements

The module implements:

- Kalman filtering with arbitrary missing observations represented by `np.nan`.
- Rauch--Tung--Striebel smoothing.
- EM estimation for linear-Gaussian state-space models.
- A constrained release-revision DFM wrapper with state vector:
- A full joint indicator-revision extension with a separate monthly-indicator revision state.
- A GDP-revision-disabled ablation, `indicator_revision_only_dfm_kalman_em`, to isolate the monthly indicator-revision state.
- Gaussian density forecast utilities: forecast intervals, log score, CRPS, and interval coverage.
- Bronze-data adapters for RTDSM release targets and ALFRED first-vs-mature monthly indicator panels.

```text
[monthly factors..., latent GDP activity, latent GDP revision state]
```

The GDP release block is:

```text
A_t = g_t + 1.00 * s_t + e_A,t
S_t = g_t + psi_S * s_t + e_S,t
T_t = g_t + psi_T * s_t + e_T,t
M_t = g_t + 0.00 * s_t + e_M,t
```

with `0 <= psi_T <= psi_S <= 1` enforced by the constrained M-step.

Monthly indicators load only on the latent monthly factor block in the baseline release-revision DFM. GDP releases load only on the latent activity and revision states. The transition matrix is estimated jointly by EM.

The joint indicator-revision model expands the state vector to:

```text
[monthly factors..., latent GDP activity, latent GDP revision state, latent indicator revision state]
```

and uses:

```text
first_indicator_i,t  = lambda_i f_t + gamma_i u_t + e_i,t
mature_indicator_i,t = lambda_i f_t + e_i,t
```

where `u_t` is the common indicator-revision state. This addresses the old limitation that indicator revisions were not represented in the model.

## Files

- `kalman_em.py`: generic Kalman filter, RTS smoother, and EM loop.
- `release_revision_dfm.py`: constrained release-structured revision-aware DFM wrapper.
- `joint_indicator_revision_dfm.py`: joint GDP-release and monthly-indicator revision model.
- `density.py`: Gaussian forecast density utilities.
- `data_adapter.py`: loaders that convert bronze RTDSM/ALFRED files into model panels.
- `example_synthetic.py`: synthetic smoke example.
- `example_bronze_smoke.py`: small smoke run using the repository's bronze data.
- `prototype_quarterly_backtest.py`: expanding-window quarterly prototype that writes forecast and metric CSVs.
- `q2_benchmarks.py`: Q2 benchmark layer with no-revision/latest-release, unrestricted MIDAS/UMIDAS, and optional SPF forecast-origin alignment.
- `mixed_frequency_release_kalman.py`: experimental monthly mixed-frequency Kalman/EM benchmark with monthly indicator observations and sparse quarter-end GDP release observations.
- `exact_pseudo_backtest.py`: release-checkpoint backtest that compares AR, no-revision, bridge, MIDAS/UMIDAS, optional SPF, monthly mixed-frequency Kalman/EM, standard DFM, release DFM, revision DFM, and full joint Kalman/EM variants on the same exact/pseudo origins.
- `build_report_package.py`: converts exact/pseudo backtest CSVs into report-ready tables, robustness summaries, figures, LaTeX snippets, and a narrative results draft.
- `build_journal_evidence_package.py`: adds journal-facing diagnostics: HAC DM tests, Clark-West-style tests, MCS proxy, block-bootstrap MCS-style tables, density scores, cumulative loss, and data audit tables.
- `build_convergence_stability_table.py`: scans multiple backtest/report outputs and builds convergence and RMSE stability summaries.
- `build_variance_audit.py`: audits predictive variances, interval coverage, and serialized GDP-release covariance matrices.
- `run_initialization_audit.py`: reruns exact/pseudo backtests under multiple EM initializations and summarizes RMSE/convergence stability.
- `run_smoke_tests.py`: assertion-based smoke tests for the main APIs.
- `__init__.py`: public exports.

## Run A Smoke Test

From the repository root:

```bash
python -m full_state_space_release_revision_dfm.example_synthetic
python -m full_state_space_release_revision_dfm.run_smoke_tests
```

Expected behavior:

- EM iterations print log-likelihood values.
- The script exits without errors.
- Latest fitted values for `A/S/T/M` are printed.
- Joint indicator-revision forecasts are finite.

To verify that the module can read the current bronze artifacts:

```bash
python -m full_state_space_release_revision_dfm.example_bronze_smoke
```

This is deliberately a smoke run, not a final backtest. It uses a small indicator subset and a short EM loop to validate data compatibility.

## Run The Prototype Quarterly Backtest

From the repository root:

```bash
python -m full_state_space_release_revision_dfm.prototype_quarterly_backtest
```

The default run uses the most recent 24 eligible quarterly origins, 13 ALFRED indicators, one common factor, and 8 EM iterations. To run a faster diagnostic:

```bash
python -m full_state_space_release_revision_dfm.prototype_quarterly_backtest \
  --max-origins 6 \
  --max-iter 6
```

Outputs are written to:

```text
outputs/full_state_space_release_revision_dfm/
```

The generated files are:

- `prototype_quarterly_forecasts.csv`
- `prototype_quarterly_revision_forecasts.csv`
- `prototype_quarterly_point_metrics.csv`
- `prototype_quarterly_revision_metrics.csv`
- `prototype_quarterly_failures.csv`
- `prototype_quarterly_run_summary.md`

Important: this runner is intentionally labeled `prototype_quarterly`. It masks current-quarter GDP releases by checkpoint and masks mature GDP/indicator values over the maturity lag, but it uses quarterly averaged first-vintage indicator panels rather than the exact event-time snapshot builder. It is suitable for validating that the full Kalman/EM model is operational; it is not yet a replacement for the exact/pseudo journal backtest.

## Run The Exact/Pseudo Model-Family Backtest

From the repository root:

```bash
python scripts/10_build_gdp_release_calendar_from_alfred.py

python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 6 \
  --max-iter 3
```

Outputs are written to:

```text
outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest/
```

Generated files:

- `forecast_results.csv`
- `revision_forecast_results.csv`
- `metrics_summary.csv`
- `revision_metrics_summary.csv`
- `exact_pseudo_point_gaps.csv`
- `exact_pseudo_revision_gaps.csv`
- `failures.csv`
- `state_space_covariance_records.csv`
- `run_summary.md`

For the full 2005Q1--2024Q4 evaluation, run:

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 10
```

For a journal convergence grid, rerun the same command with higher iteration caps and separate output folders, for example:

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

Interpretation: `exact` snapshots use ALFRED vintages with `realtime_start <= forecast_origin_date`; `pseudo` snapshots use a coarser month-end cutoff for the same checkpoint month. After running `scripts/10_build_gdp_release_calendar_from_alfred.py`, exact GDP checkpoint dates are one business day before release dates inferred from the first GDPC1 ALFRED `realtime_start` vintages after the target quarter end. The headline 2005Q1--2024Q4 A/S/T sample has vintage-derived GDP release dates; the time convention remains BEA's standard 08:30 ET release time, not a separately scraped intraday archive.

## Build Mature-Target Robustness Panels

The default mature target `M` is a 12-quarter-later anchor. For journal robustness, build alternative `M` definitions:

```bash
python scripts/11_build_mature_target_robustness_panels.py
```

This writes:

- `data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv`
- `data/bronze/targets/robustness/gdp_release_targets_mature_3y.csv`
- `data/bronze/targets/robustness/gdp_release_targets_mature_latest.csv`

Then pass any alternative target file into the same backtest runner:

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 50 \
  --gdp-release-targets data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_mature_1y_max_iter50
```

## Build The Report Package

After an exact/pseudo run, convert the raw forecast CSVs into report artifacts:

```bash
python -m full_state_space_release_revision_dfm.build_report_package
```

Outputs are written to:

```text
outputs/full_state_space_release_revision_dfm/report_package/
```

Generated files include:

- `headline_point_results.csv`
- `headline_revision_results.csv`
- `headline_exact_vs_pseudo.csv`
- `headline_revision_exact_vs_pseudo.csv`
- `point_metrics_full.csv`
- `revision_metrics_full.csv`
- `subsample_robustness_point.csv`
- `subsample_robustness_revision.csv`
- `subsample_robustness_point_winners.csv`
- `subsample_robustness_revision_winners.csv`
- `winner_stability.csv`
- `data_coverage_summary.csv`
- `convergence_diagnostics.csv`
- `latex_tables.tex`
- `journal_results_draft.md`
- `manifest.json`
- `figures/`

The report package uses the convention `forecast_error = forecast_value - realized_value`; reported bias is the mean of that error. It is designed as the source of truth for writing a report from a given exact/pseudo build.

## Build The Journal Evidence Package

After the exact/pseudo backtest and report package are available, build the reviewer-facing evidence layer:

```bash
python -m full_state_space_release_revision_dfm.build_journal_evidence_package
```

Outputs are written to:

```text
outputs/full_state_space_release_revision_dfm/journal_evidence_package/
```

Generated files include:

- `forecast_comparison_point_dm.csv`
- `forecast_comparison_revision_dm.csv`
- `forecast_comparison_point_clark_west.csv`
- `forecast_comparison_revision_clark_west.csv`
- `model_confidence_set_proxy_point.csv`
- `model_confidence_set_proxy_revision.csv`
- `model_confidence_set_block_bootstrap_point.csv`
- `model_confidence_set_block_bootstrap_revision.csv`
- `density_point_metrics.csv`
- `density_revision_metrics.csv`
- `density_point_records.csv`
- `density_revision_records.csv`
- `cumulative_loss_point.csv`
- `cumulative_loss_revision.csv`
- `calendar_audit.csv`
- `series_coverage_audit.csv`
- `target_coverage_audit.csv`
- `journal_evidence_summary.md`
- `manifest.json`
- `figures/`

Interpretation caveats:

- DM tests use HAC/Newey-West long-run variance with lag `floor(n^(1/3))`.
- Clark-West rows are diagnostics for nested or near-nested comparisons, not a complete replacement for a dedicated nested forecast-test section.
- The proxy MCS files are kept for continuity; the block-bootstrap MCS-style files are the stronger multiple-model comparison diagnostic.
- Density forecasts use model-implied state-space predictive variances when the backtest output contains them; non-state-space benchmarks fall back to expanding residual-calibrated Gaussian variances from prior forecast errors.

## Build The Variance Audit

After an exact/pseudo run, audit predictive variance and covariance behavior:

```bash
python -m full_state_space_release_revision_dfm.build_variance_audit \
  --source-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter50 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter50_variance_audit
```

Generated files include:

- `variance_point_audit.csv`
- `variance_revision_audit.csv`
- `covariance_matrix_audit.csv`
- `variance_audit_summary.md`
- `manifest.json`

The audit checks whether state-space predictive variances are positive and finite, whether covariance matrices are symmetric/positive semidefinite, and whether predictive standard deviations are plausible relative to empirical forecast errors.

## Run The Initialization Audit

To test EM sensitivity to local initialization, run:

```bash
python -m full_state_space_release_revision_dfm.run_initialization_audit \
  --seeds 1 2 3 4 5 \
  --max-origins 0 \
  --max-iter 50 \
  --initialization-jitter 0.02 \
  --output-root outputs/full_state_space_release_revision_dfm/initialization_audit_max_iter50
```

For fast diagnostics, use `--max-origins 1` or `--max-origins 12`. For a journal claim, use `--max-origins 0`.

## Build Convergence Stability Tables

After running multiple `max_iter` builds, summarize convergence and RMSE stability:

```bash
python -m full_state_space_release_revision_dfm.build_convergence_stability_table \
  --output-dir outputs/full_state_space_release_revision_dfm/convergence_stability
```

Generated files include:

- `convergence_point_runs.csv`
- `convergence_revision_runs.csv`
- `convergence_point_stability.csv`
- `convergence_revision_stability.csv`

## Run A Journal-Candidate Pipeline

The orchestration script runs calendar construction, mature-target panel construction, exact/pseudo backtests, report packages, journal evidence packages, variance audits, convergence stability, optional initialization audit, and a frozen manifest:

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

This is the command to run before writing final Q1/Q2 claims. It can be compute-intensive because it repeats full-sample Kalman/EM estimation many times.

## How To Connect To The Project Pipeline Later

The wrapper expects two aligned panels:

```python
monthly_panel: DataFrame with shape (time, monthly_indicators)
release_panel: DataFrame with columns ["A", "S", "T", "M"]
```

Both panels should use the same time index. Missing observations must be `np.nan`.

For a real-time forecast origin:

1. Build the exact or pseudo snapshot panel up to the forecast origin.
2. Convert monthly indicators into the aligned measurement grid.
3. Insert known GDP releases for the current quarter and `np.nan` for unreleased targets.
4. Fit or recursively update the model using only rows available at that origin.
5. Extract the filtered state at the forecast origin and map it through the GDP release rows of the design matrix.

For the joint indicator-revision model, pass:

```python
monthly_first_panel
monthly_mature_panel
release_panel[["A", "S", "T", "M"]]
```

The `data_adapter.py` helpers can construct first-vintage and mature-vintage panels from `data/bronze/indicators/alfred_monthly_long.csv`, then quarterly-average and align them with `data/bronze/targets/gdp_release_targets.csv`.

## What This Completes Relative To The Old Limitations

This folder addresses the main modeling limitations in the old report:

- Full Kalman filtering and RTS smoothing are implemented directly.
- EM estimation is implemented for generic linear-Gaussian models and constrained release/revision models.
- GDP release ladder `A/S/T/M` is represented as a joint measurement block.
- GDP revision state is represented structurally.
- Monthly indicator revisions can be represented through first-vintage and mature-vintage indicator measurements.
- Forecast densities can be scored with log score, CRPS, and interval coverage.

What remains before making journal claims:

- run a final high-iteration exact/pseudo build, e.g. `--max-iter 10` or higher;
- inspect convergence diagnostics for the Kalman/EM rows;
- freeze the exact/pseudo output directory and generated report package;
- rewrite the paper claims only from the frozen package.

## Important Interpretation Rule

This folder is implementation groundwork. It is not evidence by itself. The paper/report may claim full Kalman/EM estimation only after:

- this module is wired into the rolling backtest;
- exact/pseudo snapshots are passed into the model without leakage;
- forecast result CSVs are regenerated;
- metrics are compared against the existing benchmark family.

The exact/pseudo runner and report-package builder now complete those wiring steps. The remaining guardrail is to use a final frozen run with adequate EM iterations and to report convergence diagnostics alongside RMSE rankings.
