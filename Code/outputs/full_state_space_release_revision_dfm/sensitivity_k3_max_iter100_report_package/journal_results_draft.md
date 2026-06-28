# Journal Results Draft

Generated UTC: `2026-05-23T15:53:53+00:00`

Source output directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_k3_max_iter100`

Report package directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_k3_max_iter100_report_package`

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
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 1.000 | 29.825 | 0.000 | 4.612 |
| point | revision_dfm_kalman_em | exact | pre_advance | A | 0.975 | 38.825 | 0.000 | 3.795 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 1.000 | 29.772 | 0.000 | 0.642 |
| point | revision_dfm_kalman_em | exact | pre_second | S | 0.987 | 38.747 | 0.000 | 0.617 |
| point | joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 1.000 | 29.600 | 0.000 | 0.374 |
| point | revision_dfm_kalman_em | exact | pre_third | T | 0.975 | 38.950 | 0.000 | 0.372 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 1.000 | 29.788 | 0.000 | 4.686 |
| point | revision_dfm_kalman_em | pseudo | pre_advance | A | 0.975 | 38.837 | 0.000 | 3.809 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 1.000 | 29.646 | 0.000 | 0.641 |
| point | revision_dfm_kalman_em | pseudo | pre_second | S | 0.987 | 38.684 | 0.000 | 0.616 |
| point | joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 1.000 | 29.675 | 0.000 | 0.374 |
| point | revision_dfm_kalman_em | pseudo | pre_third | T | 0.975 | 39.013 | 0.000 | 0.372 |

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
| exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.568 | 0.402 | 0.044 | 0.646 | 79 |
| exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.360 | 0.238 | -0.052 | 0.532 | 79 |
| exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 1.056 | -0.032 | 0.000 | 80 |
| pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 0.400 | -0.066 | 0.000 | 79 |
| pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.360 | 0.238 | -0.052 | 0.532 | 79 |
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
| indicator_revision_only_dfm_kalman_em | pre_advance | A | 4.454 | 4.493 | -0.039 |
| indicator_revision_only_dfm_kalman_em | pre_second | S | 0.594 | 0.594 | 0.000 |
| indicator_revision_only_dfm_kalman_em | pre_third | T | 0.371 | 0.371 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | A | 4.612 | 4.686 | -0.073 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | S | 0.642 | 0.641 | 0.001 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | T | 0.374 | 0.374 | -0.000 |
| midas_umidas | pre_advance | A | 13.366 | 13.554 | -0.188 |
| midas_umidas | pre_second | S | 4.770 | 4.763 | 0.008 |
| midas_umidas | pre_third | T | 6.190 | 6.187 | 0.004 |
| monthly_mixed_frequency_kalman_em | pre_advance | A | 3.770 | 3.803 | -0.033 |
| monthly_mixed_frequency_kalman_em | pre_second | S | 0.698 | 0.630 | 0.067 |
| monthly_mixed_frequency_kalman_em | pre_third | T | 0.375 | 0.372 | 0.003 |
| no_revision | pre_advance | A | 7.269 | 7.269 | 0.000 |
| no_revision | pre_second | S | 0.570 | 0.570 | 0.000 |
| no_revision | pre_third | T | 0.362 | 0.362 | 0.000 |
| release_dfm | pre_advance | A | 3.110 | 3.088 | 0.022 |
| release_dfm | pre_second | S | 0.693 | 0.693 | 0.000 |
| release_dfm | pre_third | T | 0.464 | 0.464 | 0.000 |
| revision_dfm_kalman_em | pre_advance | A | 3.795 | 3.809 | -0.014 |
| revision_dfm_kalman_em | pre_second | S | 0.617 | 0.616 | 0.001 |
| revision_dfm_kalman_em | pre_third | T | 0.372 | 0.372 | -0.000 |
| spf | pre_advance | A | 2.225 | 2.225 | 0.000 |
| spf | pre_second | S | 2.337 | 2.337 | 0.000 |
| spf | pre_third | T | 2.380 | 2.380 | 0.000 |
| standard_dfm | pre_advance | A | 3.110 | 3.088 | 0.022 |
| standard_dfm | pre_second | S | 2.908 | 2.896 | 0.013 |
| standard_dfm | pre_third | T | 2.907 | 2.904 | 0.003 |

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
| joint_indicator_revision_dfm_full_kalman_em | pre_advance | DELTA_SA | 0.629 | 0.639 | -0.010 |
| joint_indicator_revision_dfm_full_kalman_em | pre_second | DELTA_TS | 0.360 | 0.360 | 0.000 |
| joint_indicator_revision_dfm_full_kalman_em | pre_third | DELTA_MT | 1.340 | 1.340 | -0.000 |
| midas_umidas | pre_advance | DELTA_SA | 13.867 | 13.497 | 0.370 |
| midas_umidas | pre_second | DELTA_TS | 3.124 | 3.124 | -0.000 |
| midas_umidas | pre_third | DELTA_MT | 7.879 | 7.842 | 0.037 |
| monthly_mixed_frequency_kalman_em | pre_advance | DELTA_SA | 0.568 | 0.572 | -0.004 |
| monthly_mixed_frequency_kalman_em | pre_second | DELTA_TS | 0.450 | 0.408 | 0.041 |
| monthly_mixed_frequency_kalman_em | pre_third | DELTA_MT | 1.378 | 1.378 | 0.000 |
| no_revision | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| no_revision | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| no_revision | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| release_dfm | pre_advance | DELTA_SA | 0.668 | 0.666 | 0.002 |
| release_dfm | pre_second | DELTA_TS | 0.378 | 0.378 | 0.000 |
| release_dfm | pre_third | DELTA_MT | 1.541 | 1.540 | 0.002 |
| revision_dfm_kalman_em | pre_advance | DELTA_SA | 0.623 | 0.625 | -0.001 |
| revision_dfm_kalman_em | pre_second | DELTA_TS | 0.362 | 0.362 | -0.000 |
| revision_dfm_kalman_em | pre_third | DELTA_MT | 1.340 | 1.340 | -0.000 |
| spf | pre_advance | DELTA_SA | 0.570 | 0.570 | 0.000 |
| spf | pre_second | DELTA_TS | 0.361 | 0.361 | 0.000 |
| spf | pre_third | DELTA_MT | 1.339 | 1.339 | 0.000 |
| standard_dfm | pre_advance | DELTA_SA | 0.668 | 0.666 | 0.002 |
| standard_dfm | pre_second | DELTA_TS | 0.402 | 0.402 | -0.000 |
| standard_dfm | pre_third | DELTA_MT | 1.585 | 1.585 | 0.001 |

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
| exclude_pandemic | exact | pre_second | S | no_revision | 0.561 | 77 |
| exclude_pandemic | exact | pre_third | T | no_revision | 0.363 | 78 |
| exclude_pandemic | pseudo | pre_advance | A | spf | 1.619 | 78 |
| exclude_pandemic | pseudo | pre_second | S | no_revision | 0.561 | 77 |
| exclude_pandemic | pseudo | pre_third | T | no_revision | 0.363 | 78 |
| pre_gfc | exact | pre_advance | A | spf | 1.090 | 12 |
| pre_gfc | exact | pre_second | S | revision_dfm_kalman_em | 0.618 | 12 |
| pre_gfc | exact | pre_third | T | joint_indicator_revision_dfm_full_kalman_em | 0.173 | 12 |
| pre_gfc | pseudo | pre_advance | A | spf | 1.090 | 12 |
| pre_gfc | pseudo | pre_second | S | revision_dfm_kalman_em | 0.618 | 12 |
| pre_gfc | pseudo | pre_third | T | joint_indicator_revision_dfm_full_kalman_em | 0.173 | 12 |
| gfc_and_recovery | exact | pre_advance | A | spf | 1.213 | 28 |
| gfc_and_recovery | exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.682 | 28 |
| gfc_and_recovery | exact | pre_third | T | no_revision | 0.516 | 28 |
| gfc_and_recovery | pseudo | pre_advance | A | spf | 1.213 | 28 |
| gfc_and_recovery | pseudo | pre_second | S | monthly_mixed_frequency_kalman_em | 0.680 | 28 |
| gfc_and_recovery | pseudo | pre_third | T | no_revision | 0.516 | 28 |
| post_gfc_pre_pandemic | exact | pre_advance | A | bridge | 0.904 | 20 |
| post_gfc_pre_pandemic | exact | pre_second | S | release_dfm | 0.415 | 19 |
| post_gfc_pre_pandemic | exact | pre_third | T | no_revision | 0.258 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_advance | A | bridge | 0.906 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_second | S | release_dfm | 0.415 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_third | T | monthly_mixed_frequency_kalman_em | 0.250 | 20 |
| post_pandemic | exact | pre_advance | A | standard_dfm; release_dfm | 2.274 | 16 |
| post_pandemic | exact | pre_second | S | no_revision | 0.213 | 16 |
| post_pandemic | exact | pre_third | T | indicator_revision_only_dfm_kalman_em | 0.242 | 16 |
| post_pandemic | pseudo | pre_advance | A | standard_dfm; release_dfm | 2.252 | 16 |
| post_pandemic | pseudo | pre_second | S | no_revision | 0.213 | 16 |
| post_pandemic | pseudo | pre_third | T | indicator_revision_only_dfm_kalman_em | 0.242 | 16 |

Revision robustness winners:

| subsample | timing_mode | checkpoint_id | revision_target_id | best_models | best_RMSE | best_n_forecasts |
| --- | --- | --- | --- | --- | --- | --- |
| full_sample | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.568 | 79 |
| full_sample | exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.360 | 79 |
| full_sample | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 80 |
| full_sample | pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.570 | 79 |
| full_sample | pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.360 | 79 |
| full_sample | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.339 | 80 |
| exclude_pandemic | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.535 | 77 |
| exclude_pandemic | exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.360 | 77 |
| exclude_pandemic | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| exclude_pandemic | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.534 | 77 |
| exclude_pandemic | pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.360 | 77 |
| exclude_pandemic | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.290 | 78 |
| pre_gfc | exact | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.567 | 12 |
| pre_gfc | exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.191 | 12 |
| pre_gfc | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.337 | 12 |
| pre_gfc | pseudo | pre_advance | DELTA_SA | joint_indicator_revision_dfm_full_kalman_em | 0.566 | 12 |
| pre_gfc | pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.191 | 12 |
| pre_gfc | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.337 | 12 |
| gfc_and_recovery | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.681 | 28 |
| gfc_and_recovery | exact | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.514 | 28 |
| gfc_and_recovery | exact | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.472 | 28 |
| gfc_and_recovery | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.680 | 28 |
| gfc_and_recovery | pseudo | pre_second | DELTA_TS | joint_indicator_revision_dfm_full_kalman_em | 0.514 | 28 |
| gfc_and_recovery | pseudo | pre_third | DELTA_MT | no_revision; spf; indicator_revision_only_dfm_kalman_em | 1.472 | 28 |
| post_gfc_pre_pandemic | exact | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.413 | 19 |
| post_gfc_pre_pandemic | exact | pre_second | DELTA_TS | standard_dfm | 0.231 | 19 |
| post_gfc_pre_pandemic | exact | pre_third | DELTA_MT | release_dfm | 1.340 | 20 |
| post_gfc_pre_pandemic | pseudo | pre_advance | DELTA_SA | monthly_mixed_frequency_kalman_em | 0.411 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_second | DELTA_TS | standard_dfm | 0.231 | 19 |
| post_gfc_pre_pandemic | pseudo | pre_third | DELTA_MT | release_dfm | 1.340 | 20 |
| post_pandemic | exact | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.213 | 16 |
| post_pandemic | exact | pre_second | DELTA_TS | ar | 0.228 | 16 |
| post_pandemic | exact | pre_third | DELTA_MT | release_dfm | 0.518 | 16 |
| post_pandemic | pseudo | pre_advance | DELTA_SA | no_revision; spf; indicator_revision_only_dfm_kalman_em | 0.213 | 16 |
| post_pandemic | pseudo | pre_second | DELTA_TS | ar | 0.228 | 16 |
| post_pandemic | pseudo | pre_third | DELTA_MT | release_dfm | 0.518 | 16 |

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
