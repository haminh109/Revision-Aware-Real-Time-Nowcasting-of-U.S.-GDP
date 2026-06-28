# Journal Evidence Package

Generated UTC: `2026-05-23T14:47:10+00:00`

Source backtest directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_k2_max_iter100`

## What This Adds

- HAC Diebold-Mariano-style forecast comparison tests.
- Clark-West-style adjusted tests for nested or near-nested structured comparisons.
- A model-confidence-set proxy based on squared-error loss differences.
- Block-bootstrap MCS-style confidence sets by timing/checkpoint/target.
- Model-implied state-space density evaluation where forecast variances are available, with leakage-safe residual calibration as fallback.
- Cumulative squared-error difference tables and selected figures.
- Data coverage and calendar audit tables.

## Main Caveat

State-space rows can use model-implied predictive variance from the Kalman measurement block. Non-state-space benchmarks still use leakage-safe expanding residual calibration when enough prior forecast errors exist. The MCS-style table is a transparent block-bootstrap implementation; if a target journal requires an exact Hansen-Lunde-Nason implementation, treat it as an auditable diagnostic layer rather than the final word.

## Selected DM/HAC Rows

| timing_mode | checkpoint_id | outcome_id | baseline_model | model_id | n_obs | mean_loss_diff | test_stat | p_value_model_better_one_sided |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | A | ar | spf | 80 | 45.9846 | 1.1084 | 0.1338 |
| exact | pre_advance | A | ar | release_dfm | 80 | 40.7285 | 1.0814 | 0.1397 |
| exact | pre_advance | A | ar | standard_dfm | 80 | 40.7285 | 1.0814 | 0.1397 |
| exact | pre_advance | A | ar | bridge | 80 | 27.1821 | 1.0763 | 0.1409 |
| exact | pre_advance | A | ar | revision_dfm_kalman_em | 80 | 37.6903 | 1.0706 | 0.1422 |
| exact | pre_advance | A | ar | monthly_mixed_frequency_kalman_em | 80 | 36.1480 | 1.0564 | 0.1454 |
| exact | pre_advance | A | ar | joint_indicator_revision_dfm_full_kalman_em | 80 | 32.7205 | 1.0390 | 0.1494 |
| exact | pre_advance | A | ar | indicator_revision_only_dfm_kalman_em | 80 | 33.8225 | 1.0260 | 0.1524 |
| exact | pre_advance | A | ar | no_revision | 80 | -1.9098 | -1.2209 | 0.8889 |
| exact | pre_advance | A | ar | midas_umidas | 80 | -127.7273 | -2.6386 | 0.9958 |
| exact | pre_advance | A | bridge | spf | 80 | 18.8025 | 1.1557 | 0.1239 |
| exact | pre_advance | A | bridge | release_dfm | 80 | 13.5464 | 1.0899 | 0.1379 |
| exact | pre_advance | A | bridge | standard_dfm | 80 | 13.5464 | 1.0899 | 0.1379 |
| exact | pre_advance | A | bridge | revision_dfm_kalman_em | 80 | 10.5082 | 1.0508 | 0.1467 |
| exact | pre_advance | A | bridge | monthly_mixed_frequency_kalman_em | 80 | 8.9659 | 0.9962 | 0.1596 |

## Point MCS-Style Bootstrap Rows

| timing_mode | checkpoint_id | outcome_id | model_id | mean_squared_error | included_in_mcs_alpha_10pct | final_model_count | last_set_p_value |
| --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | A | spf | 4.9504 | True | 10 | 0.4446 |
| exact | pre_advance | A | release_dfm | 10.2065 | True | 10 | 0.4446 |
| exact | pre_advance | A | standard_dfm | 10.2065 | True | 10 | 0.4446 |
| exact | pre_advance | A | revision_dfm_kalman_em | 13.2447 | True | 10 | 0.4446 |
| exact | pre_advance | A | monthly_mixed_frequency_kalman_em | 14.7870 | True | 10 | 0.4446 |
| exact | pre_advance | A | indicator_revision_only_dfm_kalman_em | 17.1125 | True | 10 | 0.4446 |
| exact | pre_advance | A | joint_indicator_revision_dfm_full_kalman_em | 18.2145 | True | 10 | 0.4446 |
| exact | pre_advance | A | bridge | 23.7529 | True | 10 | 0.4446 |
| exact | pre_advance | A | ar | 50.9350 | True | 10 | 0.4446 |
| exact | pre_advance | A | no_revision | 52.8448 | True | 10 | 0.4446 |
| exact | pre_advance | A | midas_umidas | 178.6623 | False | 10 | 0.4446 |
| exact | pre_second | S | no_revision | 0.3245 | True | 11 | 0.1499 |
| exact | pre_second | S | indicator_revision_only_dfm_kalman_em | 0.3576 | True | 11 | 0.1499 |
| exact | pre_second | S | revision_dfm_kalman_em | 0.3995 | True | 11 | 0.1499 |
| exact | pre_second | S | joint_indicator_revision_dfm_full_kalman_em | 0.4386 | True | 11 | 0.1499 |
| exact | pre_second | S | release_dfm | 0.4394 | True | 11 | 0.1499 |
| exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.5411 | True | 11 | 0.1499 |
| exact | pre_second | S | spf | 5.4636 | True | 11 | 0.1499 |
| exact | pre_second | S | standard_dfm | 9.2833 | True | 11 | 0.1499 |
| exact | pre_second | S | bridge | 20.6357 | True | 11 | 0.1499 |

## Point Density Metrics

| model_id | timing_mode | checkpoint_id | outcome_id | n_density_forecasts | mean_log_score | mean_crps | coverage_68 | coverage_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| spf | exact | pre_advance | A | 68 | -2.7151 | 1.0659 | 0.6618 | 0.8971 |
| release_dfm | exact | pre_advance | A | 68 | -3.4355 | 1.4131 | 0.7059 | 0.8824 |
| standard_dfm | exact | pre_advance | A | 68 | -3.4355 | 1.4131 | 0.7059 | 0.8824 |
| revision_dfm_kalman_em | exact | pre_advance | A | 80 | -4.3160 | 1.5222 | 0.6500 | 0.8125 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80 | -3.9618 | 1.5800 | 0.6875 | 0.8625 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80 | -5.3851 | 1.7551 | 0.6250 | 0.7500 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80 | -4.2387 | 1.7589 | 0.5750 | 0.7875 |
| bridge | exact | pre_advance | A | 68 | -4.2343 | 1.8351 | 0.7794 | 0.8971 |
| ar | exact | pre_advance | A | 68 | -5.6181 | 2.4184 | 0.8235 | 0.9265 |
| no_revision | exact | pre_advance | A | 68 | -5.6216 | 2.4988 | 0.8235 | 0.9265 |
| midas_umidas | exact | pre_advance | A | 68 | -4.1142 | 6.2809 | 0.8382 | 0.9559 |
| no_revision | exact | pre_second | S | 67 | -0.8834 | 0.3002 | 0.8657 | 0.9403 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79 | -0.9038 | 0.3337 | 0.7215 | 0.8861 |
| revision_dfm_kalman_em | exact | pre_second | S | 79 | -0.9658 | 0.3387 | 0.7595 | 0.8987 |
| release_dfm | exact | pre_second | S | 67 | -1.0504 | 0.3525 | 0.8209 | 0.9254 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79 | -1.2068 | 0.3595 | 0.7722 | 0.8861 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79 | -1.0355 | 0.3641 | 0.6835 | 0.8481 |
| spf | exact | pre_second | S | 67 | -2.6206 | 1.1638 | 0.7164 | 0.8806 |

## Revision Density Metrics

| model_id | timing_mode | checkpoint_id | outcome_id | n_density_forecasts | mean_log_score | mean_crps | coverage_68 | coverage_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| no_revision | exact | pre_advance | DELTA_SA | 67 | -0.8834 | 0.3002 | 0.8657 | 0.9403 |
| spf | exact | pre_advance | DELTA_SA | 67 | -0.8834 | 0.3002 | 0.8657 | 0.9403 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79 | -0.8757 | 0.3081 | 0.8354 | 0.9367 |
| revision_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79 | -0.9081 | 0.3250 | 0.7975 | 0.8861 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79 | -1.0019 | 0.3299 | 0.7975 | 0.8987 |
| release_dfm | exact | pre_advance | DELTA_SA | 67 | -1.0161 | 0.3401 | 0.8507 | 0.9403 |
| standard_dfm | exact | pre_advance | DELTA_SA | 67 | -1.0161 | 0.3401 | 0.8507 | 0.9403 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79 | -0.9716 | 0.3508 | 0.6709 | 0.9114 |
| ar | exact | pre_advance | DELTA_SA | 67 | -1.0821 | 0.3640 | 0.8507 | 0.9403 |
| bridge | exact | pre_advance | DELTA_SA | 67 | -1.1528 | 0.3913 | 0.8507 | 0.8955 |
| midas_umidas | exact | pre_advance | DELTA_SA | 67 | -4.0642 | 6.6520 | 0.8955 | 0.9552 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79 | -0.4166 | 0.1841 | 0.8481 | 0.9494 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79 | -0.4167 | 0.1842 | 0.8481 | 0.9494 |
| revision_dfm_kalman_em | exact | pre_second | DELTA_TS | 79 | -0.4181 | 0.1842 | 0.8481 | 0.9494 |
| no_revision | exact | pre_second | DELTA_TS | 67 | -0.6440 | 0.1969 | 0.7612 | 0.8657 |
| spf | exact | pre_second | DELTA_TS | 67 | -0.6440 | 0.1969 | 0.7612 | 0.8657 |
| release_dfm | exact | pre_second | DELTA_TS | 67 | -0.6391 | 0.2000 | 0.7612 | 0.8657 |
| standard_dfm | exact | pre_second | DELTA_TS | 67 | -0.6658 | 0.2163 | 0.7761 | 0.8657 |

## Calendar Audit

| audit_item | value | detail |
| --- | --- | --- |
| calendar_rows | 960 | gdp_release_calendar_used.csv |
| calendar_status::derived_from_alfred_gdpc1_vintage_date | 641 | all release rounds |
| calendar_status::fallback_deterministic_month_end | 319 | all release rounds |
| headline_A_S_T_calendar_status::derived_from_alfred_gdpc1_vintage_date | 240 | 2005Q1-2024Q4 |

## Figures

- `figures/point_cumulative_loss_exact_pre_third_T_joint_vs_release.png`
- `figures/point_cumulative_loss_exact_pre_second_S_release_vs_standard.png`
- `figures/revision_cumulative_loss_exact_delta_sa_joint_vs_release.png`
