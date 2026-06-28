# Journal Results Draft

Generated UTC: `2026-05-23T16:55:06+00:00`

Source output directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_midas_lags4_max_iter100`

Report package directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_midas_lags4_max_iter100_report_package`

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
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 1.000 | 32.025 | 0.000 | 3.610 |
| point | revision_dfm_kalman_em | exact | pre_advance | A | 1.000 | 24.575 | 0.000 | 3.383 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 1.000 | 32.620 | 0.000 | 0.632 |
| point | revision_dfm_kalman_em | exact | pre_second | S | 1.000 | 24.810 | 0.000 | 0.695 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 1.000 | 32.413 | 0.000 | 0.372 |
| point | revision_dfm_kalman_em | exact | pre_third | T | 1.000 | 24.538 | 0.000 | 0.373 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 1.000 | 32.100 | 0.000 | 3.610 |
| point | revision_dfm_kalman_em | pseudo | pre_advance | A | 1.000 | 24.575 | 0.000 | 3.380 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 1.000 | 32.367 | 0.000 | 0.631 |
| point | revision_dfm_kalman_em | pseudo | pre_second | S | 1.000 | 24.823 | 0.000 | 0.695 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 1.000 | 32.413 | 0.000 | 0.372 |
| point | revision_dfm_kalman_em | pseudo | pre_third | T | 1.000 | 24.550 | 0.000 | 0.373 |

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
| ar | pre_advance | A | 7.137 | 7.137 | 0.000 |
| ar | pre_second | S | 6.942 | 6.942 | 0.000 |
| ar | pre_third | T | 6.854 | 6.854 | 0.000 |
| bridge | pre_advance | A | 4.874 | 4.853 | 0.020 |
| bridge | pre_second | S | 4.543 | 4.542 | 0.000 |
| bridge | pre_third | T | 4.402 | 4.399 | 0.003 |
| indicator_revision_only_dfm_kalman_em | pre_advance | A | 3.566 | 3.565 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_second | S | 0.591 | 0.591 | -0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | T | 0.371 | 0.371 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | A | 3.610 | 3.610 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | S | 0.632 | 0.631 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | T | 0.372 | 0.372 | -0.000 |
| midas_umidas | pre_advance | A | 7.756 | 7.854 | -0.098 |
| midas_umidas | pre_second | S | 3.354 | 3.350 | 0.004 |
| midas_umidas | pre_third | T | 1.438 | 1.557 | -0.119 |
| monthly_mixed_frequency_kalman_em | pre_advance | A | 3.725 | 3.729 | -0.003 |
| monthly_mixed_frequency_kalman_em | pre_second | S | 0.595 | 0.593 | 0.001 |
| monthly_mixed_frequency_kalman_em | pre_third | T | 0.369 | 0.369 | 0.000 |
| no_revision | pre_advance | A | 7.269 | 7.269 | 0.000 |
| no_revision | pre_second | S | 0.570 | 0.570 | 0.000 |
| no_revision | pre_third | T | 0.362 | 0.362 | 0.000 |
| release_dfm | pre_advance | A | 3.066 | 3.052 | 0.014 |
| release_dfm | pre_second | S | 0.691 | 0.690 | 0.000 |
| release_dfm | pre_third | T | 0.445 | 0.445 | -0.000 |
| revision_dfm_kalman_em | pre_advance | A | 3.383 | 3.380 | 0.003 |
| revision_dfm_kalman_em | pre_second | S | 0.695 | 0.695 | 0.000 |
| revision_dfm_kalman_em | pre_third | T | 0.373 | 0.373 | 0.000 |
| spf | pre_advance | A | 2.225 | 2.225 | 0.000 |
| spf | pre_second | S | 2.337 | 2.337 | 0.000 |
| spf | pre_third | T | 2.380 | 2.380 | 0.000 |
| standard_dfm | pre_advance | A | 3.066 | 3.052 | 0.014 |
| standard_dfm | pre_second | S | 2.923 | 2.916 | 0.007 |
| standard_dfm | pre_third | T | 2.907 | 2.906 | 0.001 |

Negative exact-minus-pseudo values mean exact event timing lowers RMSE. Positive values mean pseudo timing has lower RMSE in that cell.

Revision timing gaps:

| model_id | checkpoint_id | revision_target_id | exact | pseudo | exact_minus_pseudo_RMSE |
| --- | --- | --- | --- | --- | --- |
| ar | pre_advance | DELTA_SA | 0.678 | 0.678 | 0.000 |
| ar | pre_second | DELTA_TS | 0.408 | 0.408 | 0.000 |
| ar | pre_third | DELTA_MT | 3.030 | 3.030 | 0.000 |
| bridge | pre_advance | DELTA_SA | 0.718 | 0.722 | -0.004 |
| bridge | pre_second | DELTA_TS | 0.492 | 0.492 | -0.001 |
| bridge | pre_third | DELTA_MT | 1.968 | 1.965 | 0.003 |
| indicator_revision_only_dfm_kalman_em | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | DELTA_SA | 0.636 | 0.637 | -0.001 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | DELTA_TS | 0.362 | 0.362 | -0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | DELTA_MT | 1.340 | 1.340 | 0.000 |
| midas_umidas | pre_advance | DELTA_SA | 12.007 | 12.038 | -0.031 |
| midas_umidas | pre_second | DELTA_TS | 0.905 | 0.904 | 0.001 |
| midas_umidas | pre_third | DELTA_MT | 6.254 | 7.715 | -1.461 |
| monthly_mixed_frequency_kalman_em | pre_advance | DELTA_SA | 0.640 | 0.640 | 0.001 |
| monthly_mixed_frequency_kalman_em | pre_second | DELTA_TS | 0.378 | 0.377 | 0.001 |
| monthly_mixed_frequency_kalman_em | pre_third | DELTA_MT | 1.387 | 1.388 | -0.000 |
| no_revision | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| no_revision | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| no_revision | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| release_dfm | pre_advance | DELTA_SA | 0.642 | 0.641 | 0.001 |
| release_dfm | pre_second | DELTA_TS | 0.364 | 0.364 | -0.000 |
| release_dfm | pre_third | DELTA_MT | 1.386 | 1.386 | 0.001 |
| revision_dfm_kalman_em | pre_advance | DELTA_SA | 0.629 | 0.629 | 0.000 |
| revision_dfm_kalman_em | pre_second | DELTA_TS | 0.362 | 0.362 | -0.000 |
| revision_dfm_kalman_em | pre_third | DELTA_MT | 1.340 | 1.340 | 0.000 |
| spf | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| spf | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| spf | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| standard_dfm | pre_advance | DELTA_SA | 0.642 | 0.641 | 0.001 |
| standard_dfm | pre_second | DELTA_TS | 0.367 | 0.367 | 0.000 |
| standard_dfm | pre_third | DELTA_MT | 1.624 | 1.622 | 0.002 |

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
| full_sample | exact | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 79 |
| full_sample | exact | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.361 | 79 |
| full_sample | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 80 |
| full_sample | pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 79 |
| full_sample | pseudo | pre_second | DELTA_TS | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.361 | 79 |
| full_sample | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 80 |
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
