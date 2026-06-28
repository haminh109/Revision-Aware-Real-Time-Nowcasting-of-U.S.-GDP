# Journal Evidence Package

Generated UTC: `2026-05-23T18:32:36+00:00`

Source backtest directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_window_rolling_max_iter100`

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
| exact | pre_advance | A | ar | spf | 80 | 109.9903 | 1.1435 | 0.1264 |
| exact | pre_advance | A | ar | joint_indicator_revision_dfm_full_kalman_em | 80 | 85.5371 | 1.1313 | 0.1290 |
| exact | pre_advance | A | ar | release_dfm | 80 | 104.9413 | 1.1262 | 0.1300 |
| exact | pre_advance | A | ar | standard_dfm | 80 | 104.9413 | 1.1262 | 0.1300 |
| exact | pre_advance | A | ar | indicator_revision_only_dfm_kalman_em | 80 | 93.1694 | 1.1230 | 0.1307 |
| exact | pre_advance | A | ar | bridge | 80 | 103.6311 | 1.1221 | 0.1309 |
| exact | pre_advance | A | ar | revision_dfm_kalman_em | 80 | 104.9830 | 1.1214 | 0.1310 |
| exact | pre_advance | A | ar | monthly_mixed_frequency_kalman_em | 80 | 97.9058 | 1.1021 | 0.1352 |
| exact | pre_advance | A | ar | midas_umidas | 80 | 84.6724 | 1.0147 | 0.1551 |
| exact | pre_advance | A | ar | no_revision | 80 | -101.4623 | -1.1511 | 0.8752 |
| exact | pre_advance | A | bridge | spf | 80 | 6.3593 | 1.4074 | 0.0797 |
| exact | pre_advance | A | bridge | release_dfm | 80 | 1.3102 | 0.9841 | 0.1625 |
| exact | pre_advance | A | bridge | standard_dfm | 80 | 1.3102 | 0.9841 | 0.1625 |
| exact | pre_advance | A | bridge | revision_dfm_kalman_em | 80 | 1.3519 | 0.8599 | 0.1949 |
| exact | pre_advance | A | bridge | indicator_revision_only_dfm_kalman_em | 80 | -10.4617 | -1.0368 | 0.8501 |

## Point MCS-Style Bootstrap Rows

| timing_mode | checkpoint_id | outcome_id | model_id | mean_squared_error | included_in_mcs_alpha_10pct | final_model_count | last_set_p_value |
| --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | A | spf | 4.9504 | True | 11 | 0.2787 |
| exact | pre_advance | A | revision_dfm_kalman_em | 9.9578 | True | 11 | 0.2787 |
| exact | pre_advance | A | release_dfm | 9.9995 | True | 11 | 0.2787 |
| exact | pre_advance | A | standard_dfm | 9.9995 | True | 11 | 0.2787 |
| exact | pre_advance | A | bridge | 11.3097 | True | 11 | 0.2787 |
| exact | pre_advance | A | monthly_mixed_frequency_kalman_em | 17.0350 | True | 11 | 0.2787 |
| exact | pre_advance | A | indicator_revision_only_dfm_kalman_em | 21.7714 | True | 11 | 0.2787 |
| exact | pre_advance | A | joint_indicator_revision_dfm_full_kalman_em | 29.4037 | True | 11 | 0.2787 |
| exact | pre_advance | A | midas_umidas | 30.2684 | True | 11 | 0.2787 |
| exact | pre_advance | A | ar | 114.9408 | True | 11 | 0.2787 |
| exact | pre_advance | A | no_revision | 216.4031 | True | 11 | 0.2787 |
| exact | pre_second | S | no_revision | 0.3245 | True | 11 | 0.2298 |
| exact | pre_second | S | indicator_revision_only_dfm_kalman_em | 0.3787 | True | 11 | 0.2298 |
| exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.4120 | True | 11 | 0.2298 |
| exact | pre_second | S | release_dfm | 0.4965 | True | 11 | 0.2298 |
| exact | pre_second | S | revision_dfm_kalman_em | 0.5415 | True | 11 | 0.2298 |
| exact | pre_second | S | midas_umidas | 1.5853 | True | 11 | 0.2298 |
| exact | pre_second | S | joint_indicator_revision_dfm_full_kalman_em | 2.0222 | True | 11 | 0.2298 |
| exact | pre_second | S | spf | 5.4636 | True | 11 | 0.2298 |
| exact | pre_second | S | standard_dfm | 8.8480 | True | 11 | 0.2298 |

## Point Density Metrics

| model_id | timing_mode | checkpoint_id | outcome_id | n_density_forecasts | mean_log_score | mean_crps | coverage_68 | coverage_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| spf | exact | pre_advance | A | 68 | -2.7151 | 1.0659 | 0.6618 | 0.8971 |
| revision_dfm_kalman_em | exact | pre_advance | A | 80 | -4.1737 | 1.3420 | 0.5500 | 0.8500 |
| release_dfm | exact | pre_advance | A | 68 | -3.4074 | 1.4282 | 0.6765 | 0.8529 |
| standard_dfm | exact | pre_advance | A | 68 | -3.4074 | 1.4282 | 0.6765 | 0.8529 |
| bridge | exact | pre_advance | A | 68 | -3.3997 | 1.5688 | 0.6618 | 0.8676 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80 | -5.5477 | 1.6172 | 0.6125 | 0.8500 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80 | -6.2858 | 1.7379 | 0.6375 | 0.7750 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80 | -10.8363 | 1.8889 | 0.5500 | 0.7000 |
| midas_umidas | exact | pre_advance | A | 68 | -3.7306 | 2.5518 | 0.6912 | 0.8824 |
| ar | exact | pre_advance | A | 68 | -7.1927 | 3.5121 | 0.7647 | 0.8824 |
| no_revision | exact | pre_advance | A | 68 | -9.1271 | 4.7189 | 0.7353 | 0.8824 |
| no_revision | exact | pre_second | S | 67 | -0.8834 | 0.3002 | 0.8657 | 0.9403 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79 | -0.9118 | 0.3331 | 0.7468 | 0.8734 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79 | -0.9728 | 0.3334 | 0.7089 | 0.8734 |
| release_dfm | exact | pre_second | S | 67 | -1.1502 | 0.3766 | 0.8358 | 0.9104 |
| revision_dfm_kalman_em | exact | pre_second | S | 79 | -1.1733 | 0.3840 | 0.7089 | 0.8861 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79 | -3.4795 | 0.5381 | 0.6456 | 0.8354 |
| midas_umidas | exact | pre_second | S | 67 | -1.7100 | 0.6970 | 0.8209 | 0.8955 |

## Revision Density Metrics

| model_id | timing_mode | checkpoint_id | outcome_id | n_density_forecasts | mean_log_score | mean_crps | coverage_68 | coverage_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| no_revision | exact | pre_advance | DELTA_SA | 67 | -0.8834 | 0.3002 | 0.8657 | 0.9403 |
| spf | exact | pre_advance | DELTA_SA | 67 | -0.8834 | 0.3002 | 0.8657 | 0.9403 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79 | -0.8465 | 0.3041 | 0.8101 | 0.9241 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79 | -0.8660 | 0.3149 | 0.7089 | 0.8987 |
| release_dfm | exact | pre_advance | DELTA_SA | 67 | -1.1085 | 0.3544 | 0.8358 | 0.9254 |
| standard_dfm | exact | pre_advance | DELTA_SA | 67 | -1.1085 | 0.3544 | 0.8358 | 0.9254 |
| revision_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79 | -1.0283 | 0.3590 | 0.7342 | 0.8608 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79 | -1.5151 | 0.3977 | 0.6962 | 0.8354 |
| bridge | exact | pre_advance | DELTA_SA | 67 | -1.1462 | 0.4007 | 0.7910 | 0.9104 |
| midas_umidas | exact | pre_advance | DELTA_SA | 67 | -1.7850 | 0.7864 | 0.7761 | 0.9104 |
| ar | exact | pre_advance | DELTA_SA | 67 | -4.5947 | 0.9192 | 0.8209 | 0.9104 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79 | -0.5238 | 0.1866 | 0.7722 | 0.8861 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79 | -0.5401 | 0.1877 | 0.7595 | 0.8861 |
| revision_dfm_kalman_em | exact | pre_second | DELTA_TS | 79 | -0.5693 | 0.1903 | 0.7342 | 0.8734 |
| no_revision | exact | pre_second | DELTA_TS | 67 | -0.6440 | 0.1969 | 0.7612 | 0.8657 |
| spf | exact | pre_second | DELTA_TS | 67 | -0.6440 | 0.1969 | 0.7612 | 0.8657 |
| standard_dfm | exact | pre_second | DELTA_TS | 67 | -0.6875 | 0.2035 | 0.7463 | 0.8806 |
| release_dfm | exact | pre_second | DELTA_TS | 67 | -0.6782 | 0.2103 | 0.7463 | 0.8507 |

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
