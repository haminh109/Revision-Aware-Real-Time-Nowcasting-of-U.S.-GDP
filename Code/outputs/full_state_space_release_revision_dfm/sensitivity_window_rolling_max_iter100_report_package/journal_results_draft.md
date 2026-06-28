# Journal Results Draft

Generated UTC: `2026-05-23T18:31:58+00:00`

Source output directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_window_rolling_max_iter100`

Report package directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_window_rolling_max_iter100_report_package`

## Data Coverage

- Point forecast rows: `5258`.
- Revision forecast rows: `5236`.
- Failure rows: `0`.
- GDP release calendar rows: `960`.
- Bias convention: `forecast_error = forecast_value - realized_value`; all bias values are mean forecast errors under this convention.
- The headline 2005Q1--2024Q4 A/S/T GDP calendar is derived from ALFRED `GDPC1` vintage dates when available; fallback rows are documented in the calendar file.

## Estimation Diagnostics

Kalman/EM rows report `convergence_rate`, `mean_iterations`, and relative final log-likelihood improvement. Mixed-frequency rows also carry numerical guard counts in the forecast-level CSV when a finite fallback is used. For a journal version, report these diagnostics next to the headline evidence rather than treating the estimator as a black box.

| table | model_id | timing_mode | checkpoint_id | outcome_id | convergence_rate | mean_iterations | median_llf_relative_last_improvement | RMSE |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 1.000 | 29.762 | 0.000 | 5.423 |
| point | revision_dfm_kalman_em | exact | pre_advance | A | 1.000 | 30.925 | 0.000 | 3.156 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 0.987 | 30.051 | 0.000 | 1.422 |
| point | revision_dfm_kalman_em | exact | pre_second | S | 1.000 | 31.025 | 0.000 | 0.736 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 0.975 | 31.275 | 0.000 | 0.373 |
| point | revision_dfm_kalman_em | exact | pre_third | T | 1.000 | 29.800 | 0.000 | 0.369 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 1.000 | 29.750 | 0.000 | 5.423 |
| point | revision_dfm_kalman_em | pseudo | pre_advance | A | 1.000 | 31.188 | 0.000 | 3.154 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 0.987 | 30.253 | 0.000 | 1.422 |
| point | revision_dfm_kalman_em | pseudo | pre_second | S | 1.000 | 31.582 | 0.000 | 0.737 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 0.975 | 31.212 | 0.000 | 0.373 |
| point | revision_dfm_kalman_em | pseudo | pre_third | T | 1.000 | 29.413 | 0.000 | 0.369 |

## Headline Point Forecast Winners

| timing_mode | checkpoint_id | target_id | best_models | best_RMSE | best_MAE | best_bias | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | A | spf | 2.225 | 1.338 | 0.032 | 80 |
| exact | pre_second | S | no_revision | 0.570 | 0.400 | -0.066 | 79 |
| exact | pre_third | T | no_revision | 0.362 | 0.240 | -0.048 | 80 |
| pseudo | pre_advance | A | spf | 2.225 | 1.338 | 0.032 | 80 |
| pseudo | pre_second | S | no_revision | 0.570 | 0.400 | -0.066 | 79 |
| pseudo | pre_third | T | no_revision | 0.362 | 0.240 | -0.048 | 80 |

Main reading:

- The advance checkpoint should be read as a monthly-information problem: bridge/MIDAS/DFM-style monthly predictors are the relevant benchmark family before any same-quarter GDP estimate is observed.
- The second and third checkpoints should be read against the no-revision benchmark. If no-revision wins, that is a substantive empirical result: official early GDP estimates are hard to improve on in point RMSE.
- State-space value should be evaluated through the full evidence package: uncertainty calibration, mature-target robustness, revision-risk diagnostics, and mechanism tables, not only the winner table.

## Headline Revision Forecast Winners

| timing_mode | checkpoint_id | revision_target_id | best_models | best_RMSE | best_MAE | best_bias | best_sign_accuracy | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 0.400 | -0.066 | 0.000 | 79 |
| exact | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.361 | 0.238 | -0.054 | 0.000 | 79 |
| exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 1.056 | -0.032 | 0.000 | 80 |
| pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 0.400 | -0.066 | 0.000 | 79 |
| pseudo | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.361 | 0.238 | -0.054 | 0.000 | 79 |
| pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 1.056 | -0.032 | 0.000 | 80 |

Revision interpretation:

- No-revision is the primary benchmark for adjacent GDP revisions because many realized revisions are small and the zero-revision forecast is hard to beat.
- Sign accuracy should be interpreted with thresholded diagnostics; near-zero revisions can make raw direction accuracy look weak even when magnitude forecasts are useful.

## Exact Versus Pseudo Timing

| model_id | checkpoint_id | target_id | exact | pseudo | exact_minus_pseudo_RMSE |
| --- | --- | --- | --- | --- | --- |
| ar | pre_advance | A | 10.721 | 10.721 | 0.000 |
| ar | pre_second | S | 10.524 | 10.524 | 0.000 |
| ar | pre_third | T | 9.304 | 9.304 | 0.000 |
| bridge | pre_advance | A | 3.363 | 3.384 | -0.021 |
| bridge | pre_second | S | 3.144 | 3.142 | 0.002 |
| bridge | pre_third | T | 3.207 | 3.199 | 0.008 |
| indicator_revision_only_dfm_kalman_em | pre_advance | A | 4.666 | 4.668 | -0.002 |
| indicator_revision_only_dfm_kalman_em | pre_second | S | 0.615 | 0.615 | -0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | T | 0.373 | 0.373 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | A | 5.423 | 5.423 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | S | 1.422 | 1.422 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | T | 0.373 | 0.373 | 0.000 |
| midas_umidas | pre_advance | A | 5.502 | 5.618 | -0.117 |
| midas_umidas | pre_second | S | 1.259 | 1.260 | -0.000 |
| midas_umidas | pre_third | T | 1.485 | 1.490 | -0.005 |
| monthly_mixed_frequency_kalman_em | pre_advance | A | 4.127 | 4.129 | -0.002 |
| monthly_mixed_frequency_kalman_em | pre_second | S | 0.642 | 0.645 | -0.003 |
| monthly_mixed_frequency_kalman_em | pre_third | T | 0.373 | 0.373 | 0.000 |
| no_revision | pre_advance | A | 14.711 | 14.711 | 0.000 |
| no_revision | pre_second | S | 0.570 | 0.570 | 0.000 |
| no_revision | pre_third | T | 0.362 | 0.362 | 0.000 |
| release_dfm | pre_advance | A | 3.162 | 3.144 | 0.019 |
| release_dfm | pre_second | S | 0.705 | 0.703 | 0.001 |
| release_dfm | pre_third | T | 0.532 | 0.532 | 0.000 |
| revision_dfm_kalman_em | pre_advance | A | 3.156 | 3.154 | 0.002 |
| revision_dfm_kalman_em | pre_second | S | 0.736 | 0.737 | -0.001 |
| revision_dfm_kalman_em | pre_third | T | 0.369 | 0.369 | -0.000 |
| spf | pre_advance | A | 2.225 | 2.225 | 0.000 |
| spf | pre_second | S | 2.337 | 2.337 | 0.000 |
| spf | pre_third | T | 2.380 | 2.380 | 0.000 |
| standard_dfm | pre_advance | A | 3.162 | 3.144 | 0.019 |
| standard_dfm | pre_second | S | 2.975 | 2.965 | 0.009 |
| standard_dfm | pre_third | T | 2.995 | 2.992 | 0.003 |

Negative exact-minus-pseudo values mean exact event timing lowers RMSE. Positive values mean pseudo timing has lower RMSE in that cell.

Revision timing gaps:

| model_id | checkpoint_id | revision_target_id | exact | pseudo | exact_minus_pseudo_RMSE |
| --- | --- | --- | --- | --- | --- |
| ar | pre_advance | DELTA_SA | 2.791 | 2.791 | 0.000 |
| ar | pre_second | DELTA_TS | 3.674 | 3.674 | 0.000 |
| ar | pre_third | DELTA_MT | 10.452 | 10.452 | 0.000 |
| bridge | pre_advance | DELTA_SA | 0.715 | 0.719 | -0.005 |
| bridge | pre_second | DELTA_TS | 0.544 | 0.544 | 0.001 |
| bridge | pre_third | DELTA_MT | 2.810 | 2.801 | 0.009 |
| indicator_revision_only_dfm_kalman_em | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | DELTA_SA | 0.825 | 0.823 | 0.002 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | DELTA_TS | 0.364 | 0.364 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | DELTA_MT | 1.354 | 1.354 | -0.000 |
| midas_umidas | pre_advance | DELTA_SA | 1.414 | 1.393 | 0.021 |
| midas_umidas | pre_second | DELTA_TS | 1.090 | 1.094 | -0.004 |
| midas_umidas | pre_third | DELTA_MT | 2.881 | 2.879 | 0.002 |
| monthly_mixed_frequency_kalman_em | pre_advance | DELTA_SA | 0.584 | 0.583 | 0.000 |
| monthly_mixed_frequency_kalman_em | pre_second | DELTA_TS | 0.466 | 0.469 | -0.003 |
| monthly_mixed_frequency_kalman_em | pre_third | DELTA_MT | 1.360 | 1.359 | 0.000 |
| no_revision | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| no_revision | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| no_revision | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| release_dfm | pre_advance | DELTA_SA | 0.676 | 0.675 | 0.001 |
| release_dfm | pre_second | DELTA_TS | 0.372 | 0.373 | -0.000 |
| release_dfm | pre_third | DELTA_MT | 1.619 | 1.618 | 0.001 |
| revision_dfm_kalman_em | pre_advance | DELTA_SA | 0.677 | 0.677 | 0.000 |
| revision_dfm_kalman_em | pre_second | DELTA_TS | 0.368 | 0.368 | 0.000 |
| revision_dfm_kalman_em | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| spf | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| spf | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| spf | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| standard_dfm | pre_advance | DELTA_SA | 0.676 | 0.675 | 0.001 |
| standard_dfm | pre_second | DELTA_TS | 0.376 | 0.376 | -0.000 |
| standard_dfm | pre_third | DELTA_MT | 1.843 | 1.839 | 0.004 |

## Robustness Winners

| subsample | timing_mode | checkpoint_id | target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- |
| full_sample | exact | pre_advance | A | spf | 2.225 | 80 |
| full_sample | exact | pre_second | S | no_revision | 0.570 | 79 |
| full_sample | exact | pre_third | T | no_revision | 0.362 | 80 |
| full_sample | pseudo | pre_advance | A | spf | 2.225 | 80 |
| full_sample | pseudo | pre_second | S | no_revision | 0.570 | 79 |
| full_sample | pseudo | pre_third | T | no_revision | 0.362 | 80 |
| exclude_pandemic | exact | pre_advance | A | spf | 1.619 | 78 |
| exclude_pandemic | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.556 | 77 |
| exclude_pandemic | exact | pre_third | T | no_revision | 0.363 | 78 |
| exclude_pandemic | pseudo | pre_advance | A | spf | 1.619 | 78 |
| exclude_pandemic | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.558 | 77 |
| exclude_pandemic | pseudo | pre_third | T | no_revision | 0.363 | 78 |
| pre_gfc | exact | pre_advance | A | spf | 1.090 | 12 |
| pre_gfc | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.587 | 12 |
| pre_gfc | exact | pre_third | T | indicator_revision_only_dfm_kalman_em | 0.189 | 12 |
| pre_gfc | pseudo | pre_advance | A | spf | 1.090 | 12 |
| pre_gfc | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.587 | 12 |
| pre_gfc | pseudo | pre_third | T | indicator_revision_only_dfm_kalman_em | 0.189 | 12 |
| gfc_and_recovery | exact | pre_advance | A | spf | 1.213 | 28 |
| gfc_and_recovery | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.719 | 28 |
| gfc_and_recovery | exact | pre_third | T | no_revision | 0.516 | 28 |
| gfc_and_recovery | pseudo | pre_advance | A | spf | 1.213 | 28 |
| gfc_and_recovery | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.719 | 28 |
| gfc_and_recovery | pseudo | pre_third | T | no_revision | 0.516 | 28 |
| post_gfc_pre_pandemic | exact | pre_advance | A | bridge | 0.907 | 20 |
| post_gfc_pre_pandemic | exact | pre_second | S | joint_indicator_revision_dfm_full_kalman_em | 0.410 | 19 |
| post_gfc_pre_pandemic | exact | pre_third | T | revision_dfm_kalman_em | 0.250 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_advance | A | bridge | 0.919 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_second | S | joint_indicator_revision_dfm_full_kalman_em | 0.411 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_third | T | revision_dfm_kalman_em | 0.250 | 20 |
| post_pandemic | exact | pre_advance | A | spf | 2.300 | 16 |
| post_pandemic | exact | pre_second | S | no_revision | 0.213 | 16 |
| post_pandemic | exact | pre_third | T | release_dfm | 0.234 | 16 |
| post_pandemic | pseudo | pre_advance | A | spf | 2.300 | 16 |
| post_pandemic | pseudo | pre_second | S | no_revision | 0.213 | 16 |
| post_pandemic | pseudo | pre_third | T | release_dfm | 0.234 | 16 |

Revision robustness winners:

| subsample | timing_mode | checkpoint_id | revision_target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- |
| full_sample | exact | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 79 |
| full_sample | exact | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.361 | 79 |
| full_sample | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 80 |
| full_sample | pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 79 |
| full_sample | pseudo | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.361 | 79 |
| full_sample | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 80 |
| exclude_pandemic | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.546 | 77 |
| exclude_pandemic | exact | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.362 | 77 |
| exclude_pandemic | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| exclude_pandemic | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.545 | 77 |
| exclude_pandemic | pseudo | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.362 | 77 |
| exclude_pandemic | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| pre_gfc | exact | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.583 | 12 |
| pre_gfc | exact | pre_second | DELTA_TS | release_dfm | 0.168 | 12 |
| pre_gfc | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.337 | 12 |
| pre_gfc | pseudo | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.583 | 12 |
| pre_gfc | pseudo | pre_second | DELTA_TS | release_dfm | 0.168 | 12 |
| pre_gfc | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.337 | 12 |
| gfc_and_recovery | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.690 | 28 |
| gfc_and_recovery | exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.514 | 28 |
| gfc_and_recovery | exact | pre_third | DELTA_MT | standard_dfm | 1.441 | 28 |
| gfc_and_recovery | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.689 | 28 |
| gfc_and_recovery | pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.514 | 28 |
| gfc_and_recovery | pseudo | pre_third | DELTA_MT | standard_dfm | 1.436 | 28 |
| post_gfc_pre_pandemic | exact | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.418 | 19 |
| post_gfc_pre_pandemic | exact | pre_second | DELTA_TS | standard_dfm | 0.232 | 19 |
| post_gfc_pre_pandemic | exact | pre_third | DELTA_MT | release_dfm | 1.312 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.419 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_second | DELTA_TS | standard_dfm | 0.232 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_third | DELTA_MT | release_dfm | 1.311 | 20 |
| post_pandemic | exact | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.213 | 16 |
| post_pandemic | exact | pre_second | DELTA_TS | standard_dfm | 0.223 | 16 |
| post_pandemic | exact | pre_third | DELTA_MT | revision_dfm_kalman_em | 0.586 | 16 |
| post_pandemic | pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.213 | 16 |
| post_pandemic | pseudo | pre_second | DELTA_TS | standard_dfm | 0.224 | 16 |
| post_pandemic | pseudo | pre_third | DELTA_MT | revision_dfm_kalman_em | 0.586 | 16 |

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
| pre_advance | A | spf | 2.225 | 80 |
| pre_second | S | no_revision | 0.570 | 79 |
| pre_third | T | no_revision | 0.362 | 80 |

Pseudo headline winners:

| checkpoint_id | target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- |
| pre_advance | A | spf | 2.225 | 80 |
| pre_second | S | no_revision | 0.570 | 79 |
| pre_third | T | no_revision | 0.362 | 80 |

## Figures

- `figures/point_rmse_by_model_exact.png`
- `figures/point_rmse_by_model_pseudo.png`
- `figures/revision_rmse_by_model_exact.png`
- `figures/exact_minus_pseudo_point_gaps.png`
