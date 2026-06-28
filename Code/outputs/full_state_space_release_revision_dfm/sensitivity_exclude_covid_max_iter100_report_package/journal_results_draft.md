# Journal Results Draft

Generated UTC: `2026-05-23T20:52:32+00:00`

Source output directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_exclude_covid_max_iter100`

Report package directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_exclude_covid_max_iter100_report_package`

## Data Coverage

- Point forecast rows: `5126`.
- Revision forecast rows: `5104`.
- Failure rows: `0`.
- GDP release calendar rows: `960`.
- Bias convention: `forecast_error = forecast_value - realized_value`; all bias values are mean forecast errors under this convention.
- The headline 2005Q1--2024Q4 A/S/T GDP calendar is derived from ALFRED `GDPC1` vintage dates when available; fallback rows are documented in the calendar file.

## Estimation Diagnostics

Kalman/EM rows report `convergence_rate`, `mean_iterations`, and relative final log-likelihood improvement. Mixed-frequency rows also carry numerical guard counts in the forecast-level CSV when a finite fallback is used. For a journal version, report these diagnostics next to the headline evidence rather than treating the estimator as a black box.

| table | model_id | timing_mode | checkpoint_id | outcome_id | convergence_rate | mean_iterations | median_llf_relative_last_improvement | RMSE |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 1.000 | 31.859 | 0.000 | 2.175 |
| point | revision_dfm_kalman_em | exact | pre_advance | A | 1.000 | 24.474 | 0.000 | 1.916 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 1.000 | 32.390 | 0.000 | 0.600 |
| point | revision_dfm_kalman_em | exact | pre_second | S | 1.000 | 24.805 | 0.000 | 0.592 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 1.000 | 32.218 | 0.000 | 0.373 |
| point | revision_dfm_kalman_em | exact | pre_third | T | 1.000 | 24.538 | 0.000 | 0.373 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 1.000 | 31.923 | 0.000 | 2.170 |
| point | revision_dfm_kalman_em | pseudo | pre_advance | A | 1.000 | 24.474 | 0.000 | 1.915 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 1.000 | 32.117 | 0.000 | 0.598 |
| point | revision_dfm_kalman_em | pseudo | pre_second | S | 1.000 | 24.818 | 0.000 | 0.591 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 1.000 | 32.218 | 0.000 | 0.373 |
| point | revision_dfm_kalman_em | pseudo | pre_third | T | 1.000 | 24.551 | 0.000 | 0.373 |

## Headline Point Forecast Winners

| timing_mode | checkpoint_id | target_id | best_models | best_RMSE | best_MAE | best_bias | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | A | spf | 1.619 | 1.181 | 0.195 | 78 |
| exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.557 | 0.392 | 0.059 | 77 |
| exact | pre_third | T | no_revision | 0.363 | 0.238 | -0.041 | 78 |
| pseudo | pre_advance | A | spf | 1.619 | 1.181 | 0.195 | 78 |
| pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.555 | 0.388 | 0.058 | 77 |
| pseudo | pre_third | T | no_revision | 0.363 | 0.238 | -0.041 | 78 |

Main reading:

- The advance checkpoint should be read as a monthly-information problem: bridge/MIDAS/DFM-style monthly predictors are the relevant benchmark family before any same-quarter GDP estimate is observed.
- The second and third checkpoints should be read against the no-revision benchmark. If no-revision wins, that is a substantive empirical result: official early GDP estimates are hard to improve on in point RMSE.
- State-space value should be evaluated through the full evidence package: uncertainty calibration, mature-target robustness, revision-risk diagnostics, and mechanism tables, not only the winner table.

## Headline Revision Forecast Winners

| timing_mode | checkpoint_id | revision_target_id | best_models | best_RMSE | best_MAE | best_bias | best_sign_accuracy | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.550 | 0.375 | 0.061 | 0.675 | 77 |
| exact | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.362 | 0.252 | 0.017 | 0.558 | 77 |
| exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 1.021 | 0.029 | 0.000 | 78 |
| pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.550 | 0.374 | 0.061 | 0.675 | 77 |
| pseudo | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.360 | 0.249 | 0.016 | 0.558 | 77 |
| pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 1.021 | 0.029 | 0.000 | 78 |

Revision interpretation:

- No-revision is the primary benchmark for adjacent GDP revisions because many realized revisions are small and the zero-revision forecast is hard to beat.
- Sign accuracy should be interpreted with thresholded diagnostics; near-zero revisions can make raw direction accuracy look weak even when magnitude forecasts are useful.

## Exact Versus Pseudo Timing

| model_id | checkpoint_id | target_id | exact | pseudo | exact_minus_pseudo_RMSE |
| --- | --- | --- | --- | --- | --- |
| ar | pre_advance | A | 1.955 | 1.955 | 0.000 |
| ar | pre_second | S | 2.149 | 2.149 | 0.000 |
| ar | pre_third | T | 2.212 | 2.212 | 0.000 |
| bridge | pre_advance | A | 1.958 | 1.946 | 0.012 |
| bridge | pre_second | S | 2.013 | 1.996 | 0.017 |
| bridge | pre_third | T | 1.980 | 1.970 | 0.010 |
| indicator_revision_only_dfm_kalman_em | pre_advance | A | 2.342 | 2.340 | 0.003 |
| indicator_revision_only_dfm_kalman_em | pre_second | S | 0.586 | 0.586 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | T | 0.371 | 0.371 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | A | 2.175 | 2.170 | 0.005 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | S | 0.600 | 0.598 | 0.001 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | T | 0.373 | 0.373 | -0.000 |
| midas_umidas | pre_advance | A | 8.971 | 9.055 | -0.084 |
| midas_umidas | pre_second | S | 2.323 | 2.307 | 0.017 |
| midas_umidas | pre_third | T | 2.394 | 2.387 | 0.007 |
| monthly_mixed_frequency_kalman_em | pre_advance | A | 2.180 | 2.186 | -0.006 |
| monthly_mixed_frequency_kalman_em | pre_second | S | 0.557 | 0.555 | 0.002 |
| monthly_mixed_frequency_kalman_em | pre_third | T | 0.368 | 0.367 | 0.001 |
| no_revision | pre_advance | A | 2.244 | 2.244 | 0.000 |
| no_revision | pre_second | S | 0.561 | 0.561 | 0.000 |
| no_revision | pre_third | T | 0.363 | 0.363 | 0.000 |
| release_dfm | pre_advance | A | 1.808 | 1.806 | 0.002 |
| release_dfm | pre_second | S | 0.575 | 0.575 | 0.000 |
| release_dfm | pre_third | T | 0.417 | 0.417 | -0.000 |
| revision_dfm_kalman_em | pre_advance | A | 1.916 | 1.915 | 0.001 |
| revision_dfm_kalman_em | pre_second | S | 0.592 | 0.591 | 0.000 |
| revision_dfm_kalman_em | pre_third | T | 0.373 | 0.373 | 0.000 |
| spf | pre_advance | A | 1.619 | 1.619 | 0.000 |
| spf | pre_second | S | 1.772 | 1.772 | 0.000 |
| spf | pre_third | T | 1.801 | 1.801 | 0.000 |
| standard_dfm | pre_advance | A | 1.808 | 1.806 | 0.002 |
| standard_dfm | pre_second | S | 1.941 | 1.937 | 0.004 |
| standard_dfm | pre_third | T | 1.961 | 1.958 | 0.003 |

Negative exact-minus-pseudo values mean exact event timing lowers RMSE. Positive values mean pseudo timing has lower RMSE in that cell.

Revision timing gaps:

| model_id | checkpoint_id | revision_target_id | exact | pseudo | exact_minus_pseudo_RMSE |
| --- | --- | --- | --- | --- | --- |
| ar | pre_advance | DELTA_SA | 0.611 | 0.611 | 0.000 |
| ar | pre_second | DELTA_TS | 0.405 | 0.405 | 0.000 |
| ar | pre_third | DELTA_MT | 2.343 | 2.343 | 0.000 |
| bridge | pre_advance | DELTA_SA | 0.640 | 0.646 | -0.006 |
| bridge | pre_second | DELTA_TS | 0.492 | 0.492 | -0.000 |
| bridge | pre_third | DELTA_MT | 1.819 | 1.815 | 0.004 |
| indicator_revision_only_dfm_kalman_em | pre_advance | DELTA_SA | 0.561 | 0.561 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_second | DELTA_TS | 0.362 | 0.362 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | DELTA_MT | 1.290 | 1.290 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | DELTA_SA | 0.588 | 0.588 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | DELTA_TS | 0.363 | 0.363 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | DELTA_MT | 1.292 | 1.292 | 0.000 |
| midas_umidas | pre_advance | DELTA_SA | 10.835 | 10.277 | 0.558 |
| midas_umidas | pre_second | DELTA_TS | 1.262 | 1.262 | 0.001 |
| midas_umidas | pre_third | DELTA_MT | 6.755 | 6.713 | 0.042 |
| monthly_mixed_frequency_kalman_em | pre_advance | DELTA_SA | 0.550 | 0.550 | 0.001 |
| monthly_mixed_frequency_kalman_em | pre_second | DELTA_TS | 0.362 | 0.360 | 0.001 |
| monthly_mixed_frequency_kalman_em | pre_third | DELTA_MT | 1.343 | 1.343 | -0.000 |
| no_revision | pre_advance | DELTA_SA | 0.561 | 0.561 | 0.000 |
| no_revision | pre_second | DELTA_TS | 0.362 | 0.362 | 0.000 |
| no_revision | pre_third | DELTA_MT | 1.290 | 1.290 | 0.000 |
| release_dfm | pre_advance | DELTA_SA | 0.572 | 0.572 | 0.000 |
| release_dfm | pre_second | DELTA_TS | 0.363 | 0.363 | -0.000 |
| release_dfm | pre_third | DELTA_MT | 1.333 | 1.332 | 0.001 |
| revision_dfm_kalman_em | pre_advance | DELTA_SA | 0.573 | 0.573 | -0.000 |
| revision_dfm_kalman_em | pre_second | DELTA_TS | 0.362 | 0.362 | -0.000 |
| revision_dfm_kalman_em | pre_third | DELTA_MT | 1.292 | 1.292 | 0.000 |
| spf | pre_advance | DELTA_SA | 0.561 | 0.561 | 0.000 |
| spf | pre_second | DELTA_TS | 0.362 | 0.362 | 0.000 |
| spf | pre_third | DELTA_MT | 1.290 | 1.290 | 0.000 |
| standard_dfm | pre_advance | DELTA_SA | 0.572 | 0.572 | 0.000 |
| standard_dfm | pre_second | DELTA_TS | 0.366 | 0.366 | 0.000 |
| standard_dfm | pre_third | DELTA_MT | 1.400 | 1.398 | 0.002 |

## Robustness Winners

| subsample | timing_mode | checkpoint_id | target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- |
| full_sample | exact | pre_advance | A | spf | 1.619 | 78 |
| full_sample | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.557 | 77 |
| full_sample | exact | pre_third | T | no_revision | 0.363 | 78 |
| full_sample | pseudo | pre_advance | A | spf | 1.619 | 78 |
| full_sample | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.555 | 77 |
| full_sample | pseudo | pre_third | T | no_revision | 0.363 | 78 |
| exclude_pandemic | exact | pre_advance | A | spf | 1.619 | 78 |
| exclude_pandemic | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.557 | 77 |
| exclude_pandemic | exact | pre_third | T | no_revision | 0.363 | 78 |
| exclude_pandemic | pseudo | pre_advance | A | spf | 1.619 | 78 |
| exclude_pandemic | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.555 | 77 |
| exclude_pandemic | pseudo | pre_third | T | no_revision | 0.363 | 78 |
| pre_gfc | exact | pre_advance | A | spf | 1.090 | 12 |
| pre_gfc | exact | pre_second | S | release_dfm | 0.580 | 12 |
| pre_gfc | exact | pre_third | T | revision_dfm_kalman_em | 0.185 | 12 |
| pre_gfc | pseudo | pre_advance | A | spf | 1.090 | 12 |
| pre_gfc | pseudo | pre_second | S | release_dfm | 0.580 | 12 |
| pre_gfc | pseudo | pre_third | T | revision_dfm_kalman_em | 0.185 | 12 |
| gfc_and_recovery | exact | pre_advance | A | spf | 1.213 | 28 |
| gfc_and_recovery | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.712 | 28 |
| gfc_and_recovery | exact | pre_third | T | monthly_mixed_frequency_kalman_em | 0.509 | 28 |
| gfc_and_recovery | pseudo | pre_advance | A | spf | 1.213 | 28 |
| gfc_and_recovery | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.712 | 28 |
| gfc_and_recovery | pseudo | pre_third | T | monthly_mixed_frequency_kalman_em | 0.509 | 28 |
| post_gfc_pre_pandemic | exact | pre_advance | A | bridge | 0.904 | 20 |
| post_gfc_pre_pandemic | exact | pre_second | S | revision_dfm_kalman_em | 0.416 | 19 |
| post_gfc_pre_pandemic | exact | pre_third | T | indicator_revision_only_dfm_kalman_em | 0.257 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_advance | A | bridge | 0.906 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_second | S | revision_dfm_kalman_em | 0.416 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_third | T | indicator_revision_only_dfm_kalman_em | 0.257 | 20 |
| post_pandemic | exact | pre_advance | A | spf | 2.300 | 16 |
| post_pandemic | exact | pre_second | S | no_revision | 0.213 | 16 |
| post_pandemic | exact | pre_third | T | release_dfm | 0.238 | 16 |
| post_pandemic | pseudo | pre_advance | A | spf | 2.300 | 16 |
| post_pandemic | pseudo | pre_second | S | no_revision | 0.213 | 16 |
| post_pandemic | pseudo | pre_third | T | release_dfm | 0.238 | 16 |

Revision robustness winners:

| subsample | timing_mode | checkpoint_id | revision_target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- |
| full_sample | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.550 | 77 |
| full_sample | exact | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.362 | 77 |
| full_sample | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| full_sample | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.550 | 77 |
| full_sample | pseudo | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.360 | 77 |
| full_sample | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| exclude_pandemic | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.550 | 77 |
| exclude_pandemic | exact | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.362 | 77 |
| exclude_pandemic | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| exclude_pandemic | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.550 | 77 |
| exclude_pandemic | pseudo | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.360 | 77 |
| exclude_pandemic | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| pre_gfc | exact | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.516 | 12 |
| pre_gfc | exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.194 | 12 |
| pre_gfc | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.337 | 12 |
| pre_gfc | pseudo | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.516 | 12 |
| pre_gfc | pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.194 | 12 |
| pre_gfc | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.337 | 12 |
| gfc_and_recovery | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.716 | 28 |
| gfc_and_recovery | exact | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.502 | 28 |
| gfc_and_recovery | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.472 | 28 |
| gfc_and_recovery | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.716 | 28 |
| gfc_and_recovery | pseudo | pre_second | DELTA_TS | monthly_mixed_frequency_kalman_em | 0.502 | 28 |
| gfc_and_recovery | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.472 | 28 |
| post_gfc_pre_pandemic | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.408 | 19 |
| post_gfc_pre_pandemic | exact | pre_second | DELTA_TS | standard_dfm | 0.224 | 19 |
| post_gfc_pre_pandemic | exact | pre_third | DELTA_MT | release_dfm | 1.331 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.406 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_second | DELTA_TS | standard_dfm | 0.224 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_third | DELTA_MT | release_dfm | 1.330 | 20 |
| post_pandemic | exact | pre_advance | DELTA_SA | revision_dfm_kalman_em | 0.203 | 16 |
| post_pandemic | exact | pre_second | DELTA_TS | standard_dfm | 0.227 | 16 |
| post_pandemic | exact | pre_third | DELTA_MT | release_dfm | 0.570 | 16 |
| post_pandemic | pseudo | pre_advance | DELTA_SA | revision_dfm_kalman_em | 0.203 | 16 |
| post_pandemic | pseudo | pre_second | DELTA_TS | standard_dfm | 0.227 | 16 |
| post_pandemic | pseudo | pre_third | DELTA_MT | release_dfm | 0.569 | 16 |

## Suggested Report Claim

A defensible Q1-style claim from this build is: real-time GDP nowcasting under a release ladder is best understood as a timing- and target-dependent forecast problem. Before the advance release, monthly information dominates. Before second and third releases, no-revision is a hard benchmark, so the contribution of release-ladder state-space modeling must be shown through uncertainty, revision-risk, mature-target robustness, and mechanism evidence as well as point accuracy.

## Reporting Cautions

- Do not mix these full state-space outputs with older frozen outputs unless the table explicitly labels the build.
- If the paper claims full Kalman/EM estimation, cite the files in this package and the exact/pseudo backtest outputs, not the older factor-regression-only report.
- The current generated package is traceable to forecast CSVs, but model selection should still be described as out-of-sample RMSE ranking rather than proof of universal dominance.
- One S-release quarter has incomplete RTDSM target coverage in the current data, so S and DELTA_SA/DELTA_TS headline cells have 79 forecasts rather than 80.

## Quick Narrative Anchors

Exact headline winners:

| checkpoint_id | target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- |
| pre_advance | A | spf | 1.619 | 78 |
| pre_second | S | monthly_mixed_frequency_kalman_em | 0.557 | 77 |
| pre_third | T | no_revision | 0.363 | 78 |

Pseudo headline winners:

| checkpoint_id | target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- |
| pre_advance | A | spf | 1.619 | 78 |
| pre_second | S | monthly_mixed_frequency_kalman_em | 0.555 | 77 |
| pre_third | T | no_revision | 0.363 | 78 |

## Figures

- `figures/point_rmse_by_model_exact.png`
- `figures/point_rmse_by_model_pseudo.png`
- `figures/revision_rmse_by_model_exact.png`
- `figures/exact_minus_pseudo_point_gaps.png`
