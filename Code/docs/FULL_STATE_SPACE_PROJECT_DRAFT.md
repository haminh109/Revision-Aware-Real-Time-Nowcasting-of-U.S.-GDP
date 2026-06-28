# Preliminary Project Draft

> Historical note: this draft predates the cleaned `RADFM/main` repository
> layout. Use the manuscript package and current runbooks for submission files;
> frozen paths mentioned here are retained as project history.

## Release-Structured and Revision-Aware Full State-Space Nowcasting of U.S. GDP

### 1. Project Overview

This project studies real-time U.S. GDP nowcasting when the target is not a single final-vintage GDP series, but the sequence of official GDP releases observed by real-time users. For each reference quarter, the Bureau of Economic Analysis releases an advance estimate, a second estimate, a third estimate, and later revised or mature values. These releases form a natural release ladder:

```text
A -> S -> T -> M
```

where `A` is the advance release, `S` is the second release, `T` is the third release, and `M` is a mature GDP value defined using a later vintage. The central idea of the project is that a forecaster before the advance release faces a different problem from a forecaster before the second or third release. Before `A`, no current-quarter GDP estimate is known. Before `S`, the advance release is already public. Before `T`, both the advance and second releases are known. Therefore, a model that explicitly respects the release structure may use the information set more efficiently than a model that treats GDP as a single target.

The upgraded version of the project implements a full linear-Gaussian state-space framework with Kalman filtering, Rauch-Tung-Striebel smoothing, and EM estimation. The framework includes two structured specifications:

- `revision_dfm_kalman_em`: a full Kalman/EM release-revision DFM with a latent GDP activity state and a GDP revision state.
- `joint_indicator_revision_dfm_full_kalman_em`: a fuller model that adds a common indicator-revision state, allowing the model to distinguish first-vintage and mature-vintage monthly indicator information.

These models are evaluated against `AR`, `bridge`, `standard_dfm`, and `release_dfm` benchmarks on the same exact and pseudo real-time forecast origins.

### 2. Research Question

The main research question is:

> Does modeling the GDP release ladder and revision process improve real-time nowcasting performance for U.S. GDP releases?

This question is split into three empirical subquestions:

1. Does release-structured modeling improve point forecasts for the advance, second, and third GDP releases?
2. Does revision-aware state-space modeling improve forecasts of adjacent GDP revisions such as `S - A`, `T - S`, and `M - T`?
3. Does exact event-time information-set construction change model rankings relative to pseudo real-time timing?

The expected contribution is not that one complex model dominates every benchmark in every cell. The more defensible contribution is that release-structured conditioning is especially valuable after the first official GDP estimate is available, while the advance release remains a difficult source-data nowcasting problem.

### 3. Data Design

The project uses a real-time data architecture with three main data layers.

First, GDP release targets are constructed from release-specific GDP vintage data. The target variables are:

```text
y_A: advance GDP release
y_S: second GDP release
y_T: third GDP release
y_M: mature GDP target
```

The adjacent revision targets are:

```text
DELTA_SA = y_S - y_A
DELTA_TS = y_T - y_S
DELTA_MT = y_M - y_T
```

Second, monthly indicators are drawn from ALFRED/FRED vintage histories. These indicators cover labor markets, industrial activity, consumption, income, housing, orders, inventories, trade, and financial conditions. At each forecast origin, only vintages with `realtime_start <= forecast_origin_date` are used.

Third, GDP release calendar timing is built from ALFRED `GDPC1` vintage dates. For the headline 2005Q1--2024Q4 sample, all A/S/T GDP release calendar rows are vintage-derived:

```text
headline A/S/T calendar rows derived from ALFRED GDPC1 vintages: 240/240
```

This reduces the previous limitation where GDP release checkpoints relied only on deterministic month-end approximations.

### 4. Forecast Checkpoints

The current implementation evaluates three headline checkpoints:

```text
pre_advance: forecast y_A before the advance GDP release
pre_second:  forecast y_S before the second GDP release
pre_third:   forecast y_T before the third GDP release
```

For each checkpoint, the information set is release-specific:

- At `pre_advance`, no current-quarter GDP release is known.
- At `pre_second`, `y_A` is known but `y_S`, `y_T`, and `y_M` are not.
- At `pre_third`, `y_A` and `y_S` are known but `y_T` and `y_M` are not.

This design prevents the evaluation from mixing three different forecasting problems into one pooled GDP nowcasting score.

### 5. Model Structure

The full state-space system has a latent common activity component and a GDP release block. A simplified representation is:

```text
state_t = [monthly factors, latent GDP activity, GDP revision state]
```

For the release-revision DFM, the GDP release equations can be written conceptually as:

```text
A_t = g_t + psi_A s_t + e_A,t
S_t = g_t + psi_S s_t + e_S,t
T_t = g_t + psi_T s_t + e_T,t
M_t = g_t + 0     s_t + e_M,t
```

where `g_t` is latent GDP activity and `s_t` is a latent revision state. The mature release is used as an anchor by normalizing its revision loading to zero.

The joint indicator-revision model extends the state vector:

```text
state_t = [monthly factors, latent GDP activity, GDP revision state, indicator revision state]
```

This allows first-vintage and mature-vintage monthly indicators to differ through a common indicator-revision component:

```text
first_indicator_i,t  = lambda_i f_t + gamma_i u_t + e_i,t
mature_indicator_i,t = lambda_i f_t             + e_i,t
```

where `u_t` is the common indicator-revision state. This addresses an important limitation of the earlier version of the project, which modeled GDP release revisions but did not jointly model revisions in the monthly indicator panel.

### 6. Benchmark Hierarchy

The model is evaluated against a benchmark hierarchy:

```text
AR
bridge
standard_dfm
release_dfm
revision_dfm_kalman_em
joint_indicator_revision_dfm_full_kalman_em
```

The `AR` benchmark uses release-specific GDP dynamics only. The `bridge` benchmark maps real-time monthly indicator summaries into GDP release targets. The `standard_dfm` extracts common factors from the real-time monthly panel but treats each GDP release target separately. The `release_dfm` uses release-structured conditioning without a full Kalman/EM revision state. The two Kalman/EM models add explicit latent revision structure.

This hierarchy is important because the full model should not be compared only against weak baselines. If the structured models win, the evidence is stronger; if they do not win, the benchmark results clarify where release structure is less useful.

### 7. Preliminary Empirical Results

The frozen full-state-space build is:

```text
outputs/full_state_space_release_revision_dfm/frozen/full_state_space_max_iter10_20260425
```

The headline evaluation sample is 2005Q1--2024Q4. The build produces:

```text
point forecasts:    2868
revision forecasts: 2856
failures:           0
```

The main point-forecast winners from the `max_iter=10` build are:

| Timing | Checkpoint | Target | Best model | RMSE | Forecasts |
|---|---|---:|---|---:|---:|
| exact | pre_advance | A | standard_dfm; release_dfm | 3.066 | 80 |
| exact | pre_second | S | release_dfm | 0.691 | 79 |
| exact | pre_third | T | joint_indicator_revision_dfm_full_kalman_em | 0.378 | 80 |
| pseudo | pre_advance | A | standard_dfm; release_dfm | 3.052 | 80 |
| pseudo | pre_second | S | release_dfm | 0.690 | 79 |
| pseudo | pre_third | T | joint_indicator_revision_dfm_full_kalman_em | 0.378 | 80 |

The main revision-forecast winners are:

| Timing | Checkpoint | Revision target | Best model | RMSE | Sign accuracy |
|---|---|---:|---|---:|---:|
| exact | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.603 | 0.620 |
| exact | pre_second | DELTA_TS | revision_dfm_kalman_em | 0.363 | 0.557 |
| exact | pre_third | DELTA_MT | joint_indicator_revision_dfm_full_kalman_em | 1.340 | 0.450 |
| pseudo | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.603 | 0.620 |
| pseudo | pre_second | DELTA_TS | revision_dfm_kalman_em | 0.363 | 0.557 |
| pseudo | pre_third | DELTA_MT | joint_indicator_revision_dfm_full_kalman_em | 1.340 | 0.450 |

The strongest point-forecast result appears at the third release. The full joint indicator-revision Kalman/EM model improves the third-release RMSE relative to the simpler release DFM:

```text
exact pre_third T:
joint_indicator_revision_dfm_full_kalman_em RMSE = 0.378
release_dfm RMSE                            = 0.445
```

This is economically plausible because by the pre-third checkpoint, the model already observes the advance and second GDP releases for the same quarter. At this stage, the remaining nowcasting problem is close to a short-horizon release-revision problem, which is exactly where the revision-aware structure should help.

For the second release, the simpler `release_dfm` remains strongest. The Kalman/EM models are competitive but do not dominate:

```text
exact pre_second S:
release_dfm RMSE                            = 0.691
revision_dfm_kalman_em RMSE                 = 0.701
joint_indicator_revision_dfm_full_kalman_em = 0.789
```

For the advance release, the structured state-space models do not dominate the standard factor/release benchmark. This is also plausible because before the advance GDP estimate, the model has no same-quarter official GDP release to condition on. The task is primarily source-data aggregation rather than release-ladder updating.

### 8. Interpretation

The preliminary evidence supports a stage-specific interpretation:

1. The advance release remains difficult. The full revision-aware structure is not automatically best before the first GDP estimate is published.
2. Release-structured conditioning is useful for the second release, where the known advance estimate becomes an informative measurement of the current quarter.
3. The full joint indicator-revision Kalman/EM model is most useful for the third release, where the known advance and second releases allow the model to update latent activity and revision states.
4. Revision forecasts contain some magnitude information, but sign accuracy is still moderate. Therefore, the model should not be sold as a strong directional classifier of GDP revisions.

The safest project claim is:

> Release-aware state-space modeling improves the discipline and usefulness of real-time GDP nowcasting, especially for later GDP releases after earlier same-quarter estimates are already public. The third release is where the full joint indicator-revision Kalman/EM model shows its clearest point-forecast advantage.

### 9. Exact Versus Pseudo Timing

The exact/pseudo comparison is not designed to prove that exact timing always lowers RMSE. It tests whether event-time discipline changes the information set and model rankings.

In the current build, exact and pseudo results are very close for many cells. This implies that the model-family conclusion is not driven purely by a timing convention. The exact design is still preferable for credibility because it enforces the rule that only observations available by the forecast origin may enter the model.

### 10. Current Limitations

The project is now substantially stronger than the earlier factor-regression version, but several limitations should still be reported honestly.

First, the frozen build uses `max_iter=10`. The strict convergence flag remains zero for the Kalman/EM rows in that frozen build. A later convergence audit with `max_iter=50` on recent origins shows that `revision_dfm_kalman_em` can satisfy the strict convergence rule, while `joint_indicator_revision_dfm_full_kalman_em` still reaches the iteration cap with small but nonzero relative log-likelihood improvements. Therefore, the safe wording is: the GDP release-revision DFM can be reported as converged after a high-iteration rerun if the full-sample diagnostics confirm the audit pattern; the full joint indicator-revision model should be reported as a high-iteration or fixed-iteration EM/Kalman estimate unless a full-sample higher-cap run verifies convergence.

Second, the full joint indicator-revision model improves the third release but does not dominate every target. The second-release winner is still the simpler `release_dfm`, and the advance-release winner is tied between `standard_dfm` and `release_dfm`.

Third, the S-release target has 79 rather than 80 forecasts because one RTDSM second-release value is missing in the current data. This does not break the evaluation, but it should be stated in the data coverage note.

Fourth, the GDP release calendar is derived from ALFRED `GDPC1` vintage dates rather than a separately scraped BEA intraday historical release archive. For the headline A/S/T sample this is a strong practical solution, but it should not be described as a full official intraday archive.

Fifth, the evaluation is still based on point forecasts and revision metrics. A journal version could add density forecast evaluation, interval coverage, and richer forecast-comparison tests.

### 11. Recommended Report Structure

A report based on this model should follow this structure:

```text
1. Introduction
2. Literature Review
3. Data and Real-Time Release Design
4. Model Framework
5. Estimation and Backtest Design
6. Results
7. Robustness and Diagnostics
8. Limitations
9. Conclusion
```

The introduction should not claim that the model wins everywhere. Instead, it should say that GDP nowcasting is release-specific, and that structured models are most useful after the first GDP release enters the information set.

The methodology section should clearly separate:

- benchmark models;
- release-structured model;
- GDP revision-aware Kalman/EM model;
- full joint indicator-revision Kalman/EM model.

The results section should emphasize:

- advance release: hard, benchmark/factor structure remains competitive;
- second release: release structure helps;
- third release: full joint indicator-revision Kalman/EM performs best;
- revision forecasts: useful magnitude information, modest direction accuracy.

### 12. Next Steps Before a Journal Version

The project is now in a good position for writing a strong report. Before submitting to a journal, the next technical improvements should be:

1. Run a higher EM iteration build, such as `max_iter=25` or `max_iter=50`, and compare convergence diagnostics.
2. Add a stability table comparing `max_iter=3`, `max_iter=10`, and higher-iteration results.
3. Replace the current residual-calibrated density diagnostics with fully model-implied Kalman predictive densities for the state-space models.
4. Extend the current HAC DM and Clark-West-style diagnostics into a final forecast-comparison section with clearly stated small-sample assumptions.
5. Add a formal Model Confidence Set implementation if targeting a top forecasting journal.
6. Write the paper only from a frozen output folder, not from ad hoc intermediate outputs.

The current frozen build is sufficient for a serious course report and a strong prototype of the journal version. For a journal submission, the main remaining issue is not whether the code can run; it can. The remaining issue is whether the final empirical claims are supported by fully stabilized estimation diagnostics.

### 15. Additional Hardening Now Available

The latest codebase adds several journal-oriented diagnostics that should be used in the next frozen build.

First, the exact/pseudo runner now includes an ablation model:

```text
indicator_revision_only_dfm_kalman_em
```

This model keeps the monthly indicator-revision state but disables the GDP revision state. It allows the empirical section to distinguish three mechanisms: release structure alone, GDP revision state, and indicator revision state.

Second, the Kalman/EM rows now export model-implied predictive variances. The evidence package uses these variances for log score, CRPS, and interval coverage where available. This is stronger than the earlier residual-calibrated density-only diagnostic. Non-state-space benchmarks still use expanding residual calibration as a fallback.

Third, the evidence package now creates block-bootstrap MCS-style confidence sets in addition to the old proxy MCS files. These tables are not a license to overclaim model dominance, but they give a stronger multiple-model comparison layer for a journal-oriented appendix.

Fourth, mature-target robustness is now operational. The script

```bash
python scripts/11_build_mature_target_robustness_panels.py
```

creates `M_1y`, `M_3y`, and `M_latest` target panels. The exact/pseudo runner accepts them via:

```bash
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest \
  --gdp-release-targets data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv
```

Fifth, convergence stability can now be summarized across output folders:

```bash
python -m full_state_space_release_revision_dfm.build_convergence_stability_table
```

The next frozen empirical build should therefore be based on a high-iteration grid, not only on the old `max_iter=10` frozen folder. Recommended minimum:

```text
max_iter = 50
max_iter = 100
max_iter = 200 if compute time permits
```

The final paper wording must follow the diagnostics. If the state-space models converge across the full sample, the paper can claim converged Kalman/EM estimates. If the joint model still hits the iteration cap, the correct wording is high-iteration fixed-cap EM estimates with convergence diagnostics.

### 16. Journal-Candidate Evidence Protocol

The code now has a complete run protocol for the next evidence freeze. The operational reference is:

```text
docs/JOURNAL_CANDIDATE_RUNBOOK.md
```

The final evidence package should include:

- high-iteration exact/pseudo backtests;
- report packages;
- journal evidence packages with block-bootstrap MCS-style diagnostics;
- model-implied state-space density metrics;
- variance and covariance matrix audits;
- mature-target robustness for `M_1y`, `M_3y`, and `M_latest`;
- convergence stability across `max_iter`;
- multi-initialization stability across EM seeds;
- a frozen manifest.

The one-command runner is:

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

The smoke version of this pipeline has been run successfully. The full command is intentionally not treated as a smoke test because it can require a long compute window.

### 13. Convergence Claim Rule

The paper should use the following rule:

```text
Claim "fully converged EM" only for model classes whose full-sample backtest has convergence_rate = 1.0, or very close to 1.0 with documented exceptions, under the chosen tolerance.
```

Based on the current audit:

- `revision_dfm_kalman_em`: promising for a full convergence claim after a full-sample `max_iter=50` rerun, because the recent-origin audit reaches convergence in about 24--31 iterations.
- `joint_indicator_revision_dfm_full_kalman_em`: not yet safe for a full convergence claim, because it still hits the 50-iteration cap in the recent-origin audit.

Therefore, the best current report wording is:

```text
We estimate the release-revision and joint indicator-revision models by Kalman filtering and EM. For the reported frozen build, EM is run for a fixed maximum of 10 iterations and convergence diagnostics are reported. A separate high-iteration audit indicates that the GDP release-revision specification reaches the strict convergence rule on recent origins, while the fuller joint indicator-revision specification remains better described as a high-iteration EM estimate unless a higher-cap full-sample run verifies convergence.
```

### 14. Journal Evidence Upgrade Added

The repository now includes an additional evidence builder:

```bash
python -m full_state_space_release_revision_dfm.build_journal_evidence_package
```

This creates:

```text
outputs/full_state_space_release_revision_dfm/journal_evidence_package/
```

The package adds several items requested by a journal-style review:

- HAC Diebold-Mariano-style forecast comparison tests.
- Clark-West-style adjusted tests for nested or near-nested comparisons.
- A transparent model-confidence-set proxy.
- Expanding residual-calibrated Gaussian density scores: log score, CRPS, 68% coverage, and 90% coverage.
- Cumulative squared-error difference tables and selected figures.
- Calendar, target, and ALFRED vintage coverage audits.

The first evidence package confirms the current interpretation. For point density diagnostics, `release_dfm` and `standard_dfm` remain strongest for the advance release, `release_dfm` remains strongest for the second release, and the full joint indicator-revision model is competitive or best for the third release. The evidence package also makes the statistical caution clearer: many RMSE improvements are economically meaningful but not overwhelmingly significant under conservative HAC forecast-comparison diagnostics.

The density results should be described carefully. They are leakage-safe because each predictive variance is calibrated only from prior forecast errors, but they are not yet full Kalman model-implied predictive densities for every benchmark. This is a useful journal-upgrade diagnostic, not the final density-forecast section for a top submission.
