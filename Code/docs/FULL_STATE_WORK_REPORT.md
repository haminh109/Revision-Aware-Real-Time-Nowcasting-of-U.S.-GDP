# Full State Folder Work Report

> Historical note: this report was written before the branch folders were
> consolidated into the current `RADFM/main` repository. Paths that mention a
> sibling `full_state` folder or old frozen output directories are historical
> context, not the current repo layout.

## 1. Folder Purpose

This folder contains the upgraded full-state-space version of the U.S. GDP release-revision nowcasting project.

Folder path:

```text
<PROJECT_ROOT>/full_state
```

The purpose of this folder is to separate the journal-oriented full-state-space implementation from the older `main` and `submission-mode` versions inside `RADFM`.

The central research idea is:

```text
U.S. GDP nowcasting should be treated as a release-specific forecasting problem,
not as a single final-vintage GDP forecast.
```

The model explicitly treats GDP as a release ladder:

```text
Advance -> Second -> Third -> Mature
A       -> S      -> T     -> M
```

This means the model separately evaluates:

```text
pre_advance -> y_A
pre_second  -> y_S
pre_third   -> y_T
```

and also studies revision targets:

```text
DELTA_SA = S - A
DELTA_TS = T - S
DELTA_MT = M - T
```

## 2. Main Additions

The main new code is stored in:

```text
full_state_space_release_revision_dfm/
```

This module adds a full state-space modeling layer with:

- Kalman filtering with missing observations.
- Rauch-Tung-Striebel smoothing.
- EM estimation for linear-Gaussian state-space systems.
- A GDP release-revision dynamic factor model.
- A full joint indicator-revision model.
- Exact/pseudo real-time backtesting.
- Report-ready output generation.
- Journal evidence diagnostics.

Key files:

```text
full_state_space_release_revision_dfm/kalman_em.py
full_state_space_release_revision_dfm/release_revision_dfm.py
full_state_space_release_revision_dfm/joint_indicator_revision_dfm.py
full_state_space_release_revision_dfm/exact_pseudo_backtest.py
full_state_space_release_revision_dfm/build_report_package.py
full_state_space_release_revision_dfm/build_journal_evidence_package.py
full_state_space_release_revision_dfm/density.py
```

## 3. State-Space Models Implemented

### 3.1 Release-Revision DFM

The first full state-space model is:

```text
revision_dfm_kalman_em
```

Its state vector contains:

```text
[monthly factors, latent GDP activity, GDP revision state]
```

The GDP release block is conceptually:

```text
A_t = g_t + psi_A s_t + e_A,t
S_t = g_t + psi_S s_t + e_S,t
T_t = g_t + psi_T s_t + e_T,t
M_t = g_t + 0     s_t + e_M,t
```

where:

- `g_t` is latent GDP activity.
- `s_t` is the latent GDP revision state.
- `M` is used as the mature anchor.

### 3.2 Full Joint Indicator-Revision DFM

The second model is:

```text
joint_indicator_revision_dfm_full_kalman_em
```

Its state vector extends the release-revision model:

```text
[monthly factors, latent GDP activity, GDP revision state, indicator revision state]
```

This model distinguishes first-vintage and mature-vintage monthly indicators:

```text
first_indicator_i,t  = lambda_i f_t + gamma_i u_t + e_i,t
mature_indicator_i,t = lambda_i f_t             + e_i,t
```

where:

- `f_t` is the common activity factor.
- `u_t` is the common monthly-indicator revision state.

This extension addresses one of the limitations of the older project: indicator revisions were not jointly represented in the model.

## 4. Benchmark Models

The full-state models are evaluated against a benchmark hierarchy:

```text
ar
bridge
standard_dfm
release_dfm
revision_dfm_kalman_em
joint_indicator_revision_dfm_full_kalman_em
```

This is important because the new model is not compared only against weak baselines.

The benchmark roles are:

- `ar`: release-specific autoregressive benchmark.
- `bridge`: mixed-frequency bridge benchmark using monthly indicators.
- `standard_dfm`: conventional factor model benchmark.
- `release_dfm`: release-structured factor model without full Kalman/EM revision state.
- `revision_dfm_kalman_em`: Kalman/EM GDP revision state model.
- `joint_indicator_revision_dfm_full_kalman_em`: full GDP and indicator revision model.

## 5. Real-Time Data and Release Timing

The project uses ALFRED/FRED vintage data and GDP release-target data.

The exact/pseudo runner uses:

```text
realtime_start <= forecast_origin_date
```

for ALFRED indicator availability.

GDP release calendar timing was improved using ALFRED `GDPC1` vintage dates. The generated calendar is:

```text
data/silver/calendars/gdp_release_calendar_alfred.csv
```

The headline A/S/T calendar coverage is:

```text
headline A/S/T calendar rows derived from ALFRED GDPC1 vintages: 240/240
```

This means that for the 2005Q1--2024Q4 headline evaluation sample, all advance, second, and third GDP release dates are derived from actual ALFRED GDP vintage dates rather than only deterministic month-end approximations.

## 6. Main Backtest Run

The full-sample exact/pseudo backtest was run with:

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --max-origins 0 \
  --max-iter 10 \
  --output-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter10
```

The output folder is:

```text
outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter10/
```

The run produced:

```text
Point forecasts:    2868
Revision forecasts: 2856
Failures:           0
```

Main output files:

```text
forecast_results.csv
revision_forecast_results.csv
metrics_summary.csv
revision_metrics_summary.csv
exact_pseudo_point_gaps.csv
exact_pseudo_revision_gaps.csv
gdp_release_calendar_used.csv
run_summary.md
```

## 7. Frozen Source of Truth

The main frozen build is:

```text
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/
```

This folder should be treated as the source of truth for writing from the current full-state version.

It contains:

```text
exact_pseudo_backtest_max_iter10/
report_package_max_iter10/
journal_evidence_package/
README.md
```

Important files:

```text
report_package_max_iter10/journal_results_draft.md
report_package_max_iter10/headline_point_results.csv
report_package_max_iter10/headline_revision_results.csv
report_package_max_iter10/convergence_diagnostics.csv
journal_evidence_package/journal_evidence_summary.md
journal_evidence_package/forecast_comparison_point_dm.csv
journal_evidence_package/density_point_metrics.csv
journal_evidence_package/calendar_audit.csv
journal_evidence_package/series_coverage_audit.csv
```

## 8. Headline Point-Forecast Results

From the frozen `max_iter=10` build:

| Timing | Checkpoint | Target | Best model | RMSE | Forecasts |
|---|---|---:|---|---:|---:|
| exact | pre_advance | A | standard_dfm; release_dfm | 3.066 | 80 |
| exact | pre_second | S | release_dfm | 0.691 | 79 |
| exact | pre_third | T | joint_indicator_revision_dfm_full_kalman_em | 0.378 | 80 |
| pseudo | pre_advance | A | standard_dfm; release_dfm | 3.052 | 80 |
| pseudo | pre_second | S | release_dfm | 0.690 | 79 |
| pseudo | pre_third | T | joint_indicator_revision_dfm_full_kalman_em | 0.378 | 80 |

Main interpretation:

- The advance release remains difficult.
- The simpler `release_dfm` is strongest for the second release.
- The full joint indicator-revision Kalman/EM model is strongest for the third release.
- The model should not be claimed to dominate all targets.

The most important positive result is:

```text
exact pre_third T:
joint_indicator_revision_dfm_full_kalman_em RMSE = 0.378
release_dfm RMSE                            = 0.445
```

This supports the claim that release/revision-aware state-space modeling is most useful after earlier same-quarter GDP releases are already public.

## 9. Headline Revision-Forecast Results

From the frozen `max_iter=10` build:

| Timing | Checkpoint | Revision target | Best model | RMSE | Sign accuracy |
|---|---|---:|---|---:|---:|
| exact | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.603 | 0.620 |
| exact | pre_second | DELTA_TS | revision_dfm_kalman_em | 0.363 | 0.557 |
| exact | pre_third | DELTA_MT | joint_indicator_revision_dfm_full_kalman_em | 1.340 | 0.450 |
| pseudo | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.603 | 0.620 |
| pseudo | pre_second | DELTA_TS | revision_dfm_kalman_em | 0.363 | 0.557 |
| pseudo | pre_third | DELTA_MT | joint_indicator_revision_dfm_full_kalman_em | 1.340 | 0.450 |

Interpretation:

- Revision magnitudes contain some useful information.
- Revision sign accuracy remains moderate.
- The paper should not claim that GDP revision direction is reliably predictable.

## 10. Report Package

The report package was generated using:

```bash
python -m full_state_space_release_revision_dfm.build_report_package \
  --source-dir outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter10 \
  --output-dir outputs/full_state_space_release_revision_dfm/report_package_max_iter10
```

It creates:

```text
headline_point_results.csv
headline_revision_results.csv
headline_exact_vs_pseudo.csv
headline_revision_exact_vs_pseudo.csv
point_metrics_full.csv
revision_metrics_full.csv
subsample_robustness_point.csv
subsample_robustness_revision.csv
subsample_robustness_point_winners.csv
subsample_robustness_revision_winners.csv
winner_stability.csv
data_coverage_summary.csv
convergence_diagnostics.csv
latex_tables.tex
journal_results_draft.md
manifest.json
figures/
```

The most useful file for writing the first results section is:

```text
outputs/full_state_space_release_revision_dfm/report_package_max_iter10/journal_results_draft.md
```

## 11. Journal Evidence Package

The journal evidence package was generated using:

```bash
python -m full_state_space_release_revision_dfm.build_journal_evidence_package
```

It creates:

```text
outputs/full_state_space_release_revision_dfm/journal_evidence_package/
```

This package adds several reviewer-facing diagnostics:

- HAC Diebold-Mariano-style forecast comparison tests.
- Clark-West-style diagnostics for nested or near-nested comparisons.
- Model confidence set proxy.
- Density forecast diagnostics.
- Cumulative squared-error loss differences.
- Calendar audit.
- Target coverage audit.
- ALFRED vintage coverage audit.

Important files:

```text
journal_evidence_summary.md
forecast_comparison_point_dm.csv
forecast_comparison_revision_dm.csv
forecast_comparison_point_clark_west.csv
forecast_comparison_revision_clark_west.csv
model_confidence_set_proxy_point.csv
model_confidence_set_proxy_revision.csv
density_point_metrics.csv
density_revision_metrics.csv
cumulative_loss_point.csv
cumulative_loss_revision.csv
calendar_audit.csv
series_coverage_audit.csv
target_coverage_audit.csv
```

This was added specifically to address the journal-style concerns:

- RMSE alone is not enough.
- Forecast comparisons need statistical diagnostics.
- Density forecast evidence is useful for a state-space model.
- Real-time data construction needs audit tables.

## 12. Density Forecast Diagnostics

The current density diagnostics use:

```text
expanding residual-calibrated Gaussian predictive variance
```

This means each model's predictive variance is estimated from its past forecast errors only. This is leakage-safe because future errors are not used.

Current density diagnostics include:

- log score;
- CRPS;
- 68% interval coverage;
- 90% interval coverage;
- predictive standard deviation.

Important caveat:

```text
These density results are not yet full model-implied Kalman predictive densities for every benchmark.
```

They are useful journal-upgrade diagnostics, but a top journal version should eventually replace or supplement them with true model-implied predictive distributions for the state-space models.

## 13. Forecast Comparison Diagnostics

The journal evidence package adds:

```text
forecast_comparison_point_dm.csv
forecast_comparison_revision_dm.csv
forecast_comparison_point_clark_west.csv
forecast_comparison_revision_clark_west.csv
```

The DM-style diagnostics use HAC/Newey-West long-run variance with lag:

```text
floor(n^(1/3))
```

The Clark-West-style diagnostics are used for nested or near-nested comparisons, especially where a larger structured model is compared with a simpler factor/release model.

Important caveat:

```text
These are conservative diagnostics, not proof that every RMSE improvement is statistically overwhelming.
```

The current evidence suggests the paper should make a careful, stage-specific claim rather than a broad dominance claim.

## 14. Cumulative Loss Figures

The evidence package generates selected cumulative loss figures:

```text
journal_evidence_package/figures/point_cumulative_loss_exact_pre_third_T_joint_vs_release.png
journal_evidence_package/figures/point_cumulative_loss_exact_pre_second_S_release_vs_standard.png
journal_evidence_package/figures/revision_cumulative_loss_exact_delta_sa_joint_vs_release.png
```

These figures help show whether a model's improvement is stable over time or driven by a few quarters.

## 15. Data Audit Improvements

The evidence package creates:

```text
calendar_audit.csv
series_coverage_audit.csv
target_coverage_audit.csv
```

The calendar audit reports:

```text
calendar rows: 960
derived_from_alfred_gdpc1_vintage_date: 641
fallback_deterministic_month_end: 319
headline A/S/T derived rows for 2005Q1-2024Q4: 240
```

The target coverage audit reports:

```text
pre_advance A: 80 forecasts
pre_second S: 79 forecasts
pre_third T: 80 forecasts
```

The missing second-release forecast count is due to incomplete RTDSM target coverage for one S-release quarter.

## 16. Convergence Diagnostics

The frozen build uses:

```text
max_iter = 10
```

The strict EM convergence flag remains low for the Kalman/EM rows in this frozen build.

A separate audit with:

```text
max_iter = 50
```

on recent origins showed:

- `revision_dfm_kalman_em` can satisfy the strict convergence rule after about 24--31 iterations.
- `joint_indicator_revision_dfm_full_kalman_em` still hits the 50-iteration cap, although relative log-likelihood improvement is already small.

Therefore, the safe wording is:

```text
The models are estimated by Kalman filtering and EM. The frozen build reports fixed-iteration EM estimates with convergence diagnostics. The simpler GDP release-revision model shows evidence of convergence in a higher-iteration audit, while the full joint indicator-revision model should still be described as a high-iteration EM estimate unless a higher-cap full-sample run verifies convergence.
```

Do not currently write:

```text
All EM estimates are fully converged maximum-likelihood estimates.
```

## 17. What Can Be Claimed Now

The strongest defensible claim is:

```text
U.S. GDP nowcasting is a release-specific prediction problem. Release-aware state-space modeling is most useful after earlier same-quarter GDP releases have become public, especially for the third GDP estimate. It is less useful for the advance estimate, where the problem remains primarily source-data aggregation.
```

This claim is supported by:

- release-specific target construction;
- exact/pseudo real-time vintage discipline;
- benchmark hierarchy;
- full-state Kalman/EM implementation;
- third-release point forecast improvement;
- revision forecast diagnostics;
- data audit tables;
- forecast comparison diagnostics;
- density forecast diagnostics.

## 18. What Still Needs Improvement Before Journal Submission

The folder is now strong enough for a serious report or working paper draft. Before a Q1/Q2 journal submission, the remaining priorities are:

1. Run full-sample `max_iter=50`, `100`, or `200` and report convergence stability.
2. Add multiple initializations for the Kalman/EM models.
3. Replace residual-calibrated density diagnostics with model-implied Kalman predictive densities.
4. Implement a formal Hansen-Lunde-Nason Model Confidence Set if targeting a top forecasting journal.
5. Add mature target robustness: `M` after 1 year, 3 years, and latest vintage.
6. Add model ablation: GDP revision state only, indicator revision state only, both states.
7. Expand cumulative loss and robustness plots.
8. Write the paper from the frozen output folder only.

## 19. Recommended Files to Read First

For a high-level project draft:

```text
docs/FULL_STATE_SPACE_PROJECT_DRAFT.md
```

For headline results:

```text
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/report_package_max_iter10/journal_results_draft.md
```

For journal evidence:

```text
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/journal_evidence_package/journal_evidence_summary.md
```

For convergence:

```text
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/report_package_max_iter10/convergence_diagnostics.csv
```

For data audit:

```text
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/journal_evidence_package/calendar_audit.csv
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/journal_evidence_package/series_coverage_audit.csv
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425/journal_evidence_package/target_coverage_audit.csv
```

## 20. Bottom Line

The `full_state` folder now contains a complete operational research prototype:

- data timing improvement;
- full state-space model implementation;
- exact/pseudo real-time backtest;
- frozen output build;
- report-ready tables;
- journal evidence diagnostics;
- project draft and work report.

It is not yet a final Q1/Q2 submission package, mainly because convergence, density forecasts, and formal model confidence tests need one more round of hardening. But it is now much stronger than a standard course project and has a clear path toward a journal-quality applied forecasting paper.

## 21. Additional Journal-Hardening Update

After the first full-state report, the codebase was upgraded further to address the main Q1/Q2 review risks.

Implemented upgrades:

- Added `indicator_revision_only_dfm_kalman_em` as an ablation model. This disables the GDP revision state while keeping the monthly indicator-revision state, so the paper can separate GDP-release revision effects from indicator-revision effects.
- Added model-implied state-space forecast variances for `revision_dfm_kalman_em`, `indicator_revision_only_dfm_kalman_em`, and `joint_indicator_revision_dfm_full_kalman_em`. The journal evidence builder now uses these variances for log score, CRPS, and coverage when available.
- Kept residual-calibrated Gaussian density as fallback only for non-state-space benchmark rows or rows without a finite model-implied variance.
- Added block-bootstrap MCS-style tables in addition to the old proxy MCS files. These are written as `model_confidence_set_block_bootstrap_point.csv` and `model_confidence_set_block_bootstrap_revision.csv`.
- Added mature-target robustness panels for `M_1y`, `M_3y`, and `M_latest` through `scripts/11_build_mature_target_robustness_panels.py`.
- Added `--gdp-release-targets` to the exact/pseudo runner so the same backtest can be rerun with each alternative mature target panel.
- Added `build_convergence_stability_table.py` to aggregate convergence and RMSE stability across multiple `max_iter` output folders.

Smoke verification completed:

```text
python -m compileall -q full_state_space_release_revision_dfm scripts
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest --max-origins 1 --max-iter 3 --output-dir outputs/full_state_space_release_revision_dfm/smoke_ablation_density
python -m full_state_space_release_revision_dfm.build_journal_evidence_package --source-dir outputs/full_state_space_release_revision_dfm/smoke_ablation_density --output-dir outputs/full_state_space_release_revision_dfm/smoke_ablation_density_journal_evidence --min-density-calibration 0 --mcs-bootstrap-reps 99
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest --max-origins 1 --max-iter 2 --gdp-release-targets data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv --output-dir outputs/full_state_space_release_revision_dfm/smoke_mature_1y
python -m full_state_space_release_revision_dfm.build_convergence_stability_table --output-dir outputs/full_state_space_release_revision_dfm/convergence_stability
```

The smoke runs produced 42 point forecasts, 42 revision forecasts, and zero failures. State-space rows now contain `forecast_variance`, `forecast_sd`, and `density_source = model_implied_state_space`.

Remaining work before a final journal claim:

- Run the full-sample high-iteration grid, at minimum `max_iter=50` and preferably `100` or `200`.
- Build report and journal evidence packages for each high-iteration run.
- Run full-sample mature-target robustness with `M_1y`, `M_3y`, and `M_latest`.
- Use the convergence stability tables to decide whether the paper can say `fully converged EM` or must say `high-iteration fixed-cap EM`.
- Treat the block-bootstrap MCS-style output as a strong diagnostic. If the target journal specifically requires an exact Hansen-Lunde-Nason implementation, add that as a final refinement.

## 22. Final Hardening Layer Added

The latest update adds the remaining operational pieces needed before a full journal-candidate run.

New code and outputs:

- `state_space_covariance_records.csv` is now written by every exact/pseudo backtest. It serializes the GDP-release predictive covariance matrix for each state-space model, origin, timing mode, and checkpoint.
- `build_variance_audit.py` creates `variance_point_audit.csv`, `variance_revision_audit.csv`, `covariance_matrix_audit.csv`, and `variance_audit_summary.md`.
- `run_initialization_audit.py` runs the exact/pseudo backtest under multiple jittered EM initializations and creates seed-level RMSE/convergence stability tables.
- `scripts/12_run_journal_candidate_pipeline.py` orchestrates the full journal-candidate workflow: calendars, mature panels, high-iteration backtests, report packages, evidence packages, variance audits, mature robustness, convergence stability, optional initialization audit, and freeze manifest.
- `docs/JOURNAL_CANDIDATE_RUNBOOK.md` now records the exact command sequence for the final full-sample evidence build.

Additional bug fix:

- `build_report_package.py` now handles very small smoke samples where some robustness subsamples are empty. This prevents smoke journal-candidate runs from failing on `subsample.iloc[0]`.

Verification completed:

```text
python -m compileall -q full_state_space_release_revision_dfm scripts
python -m full_state_space_release_revision_dfm.run_smoke_tests
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest --max-origins 1 --max-iter 3 --output-dir outputs/full_state_space_release_revision_dfm/smoke_journal_hardening
python -m full_state_space_release_revision_dfm.build_variance_audit --source-dir outputs/full_state_space_release_revision_dfm/smoke_journal_hardening --output-dir outputs/full_state_space_release_revision_dfm/smoke_journal_hardening_variance_audit
python -m full_state_space_release_revision_dfm.run_initialization_audit --seeds 1 2 --max-origins 1 --max-iter 3 --initialization-jitter 0.02 --output-root outputs/full_state_space_release_revision_dfm/smoke_initialization_audit
python scripts/12_run_journal_candidate_pipeline.py --max-iters 3 --max-origins 1 --mcs-bootstrap-reps 99 --mature-max-iter 2 --mature-variants mature_1y --run-initialization-audit --initialization-seeds 1 2 --initialization-max-iter 3 --initialization-max-origins 1 --freeze-name smoke_journal_candidate_freeze --manifest-only
```

The smoke journal-candidate pipeline completed end-to-end. It produced report packages, journal evidence packages, variance audits, mature-target smoke robustness, convergence stability tables, initialization audit tables, and a frozen manifest.

Remaining evidence task:

- The code is now ready for the full-sample journal run. The final Q1/Q2 evidence still requires running the high-iteration full-sample command in `docs/JOURNAL_CANDIDATE_RUNBOOK.md`, because smoke runs validate the pipeline but do not establish final empirical claims.
