# Journal Evidence Package

Generated UTC: `2026-05-23T20:53:39+00:00`

Source backtest directory: `<PROJECT_ROOT>/outputs/full_state_space_release_revision_dfm/sensitivity_exclude_covid_max_iter100`

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
| exact | pre_advance | A | ar | spf | 78 | 1.2014 | 1.6212 | 0.0525 |
| exact | pre_advance | A | ar | release_dfm | 78 | 0.5546 | 0.5145 | 0.3035 |
| exact | pre_advance | A | ar | standard_dfm | 78 | 0.5546 | 0.5145 | 0.3035 |
| exact | pre_advance | A | ar | revision_dfm_kalman_em | 78 | 0.1504 | 0.1809 | 0.4282 |
| exact | pre_advance | A | ar | bridge | 78 | -0.0118 | -0.0138 | 0.5055 |
| exact | pre_advance | A | ar | monthly_mixed_frequency_kalman_em | 78 | -0.9294 | -0.7981 | 0.7876 |
| exact | pre_advance | A | ar | no_revision | 78 | -1.2130 | -1.1861 | 0.8822 |
| exact | pre_advance | A | ar | joint_indicator_revision_dfm_full_kalman_em | 78 | -0.9063 | -1.4422 | 0.9254 |
| exact | pre_advance | A | ar | indicator_revision_only_dfm_kalman_em | 78 | -1.6639 | -1.9284 | 0.9731 |
| exact | pre_advance | A | ar | midas_umidas | 78 | -76.6531 | -4.1734 | 1.0000 |
| exact | pre_advance | A | bridge | spf | 78 | 1.2132 | 2.0852 | 0.0185 |
| exact | pre_advance | A | bridge | release_dfm | 78 | 0.5664 | 1.1173 | 0.1319 |
| exact | pre_advance | A | bridge | standard_dfm | 78 | 0.5664 | 1.1173 | 0.1319 |
| exact | pre_advance | A | bridge | revision_dfm_kalman_em | 78 | 0.1622 | 0.2764 | 0.3911 |
| exact | pre_advance | A | bridge | ar | 78 | 0.0118 | 0.0138 | 0.4945 |

## Point MCS-Style Bootstrap Rows

| timing_mode | checkpoint_id | outcome_id | model_id | mean_squared_error | included_in_mcs_alpha_10pct | final_model_count | last_set_p_value |
| --- | --- | --- | --- | --- | --- | --- | --- |
| exact | pre_advance | A | spf | 2.6220 | True | 10 | 0.1479 |
| exact | pre_advance | A | release_dfm | 3.2687 | True | 10 | 0.1479 |
| exact | pre_advance | A | standard_dfm | 3.2687 | True | 10 | 0.1479 |
| exact | pre_advance | A | revision_dfm_kalman_em | 3.6729 | True | 10 | 0.1479 |
| exact | pre_advance | A | ar | 3.8234 | True | 10 | 0.1479 |
| exact | pre_advance | A | bridge | 3.8352 | True | 10 | 0.1479 |
| exact | pre_advance | A | joint_indicator_revision_dfm_full_kalman_em | 4.7297 | True | 10 | 0.1479 |
| exact | pre_advance | A | monthly_mixed_frequency_kalman_em | 4.7527 | True | 10 | 0.1479 |
| exact | pre_advance | A | no_revision | 5.0364 | True | 10 | 0.1479 |
| exact | pre_advance | A | indicator_revision_only_dfm_kalman_em | 5.4872 | True | 10 | 0.1479 |
| exact | pre_advance | A | midas_umidas | 80.4764 | False | 10 | 0.1479 |
| exact | pre_second | S | monthly_mixed_frequency_kalman_em | 0.3099 | True | 6 | 0.7283 |
| exact | pre_second | S | no_revision | 0.3142 | True | 6 | 0.7283 |
| exact | pre_second | S | release_dfm | 0.3308 | True | 6 | 0.7283 |
| exact | pre_second | S | indicator_revision_only_dfm_kalman_em | 0.3430 | True | 6 | 0.7283 |
| exact | pre_second | S | revision_dfm_kalman_em | 0.3504 | True | 6 | 0.7283 |
| exact | pre_second | S | joint_indicator_revision_dfm_full_kalman_em | 0.3597 | True | 6 | 0.7283 |
| exact | pre_second | S | spf | 3.1404 | False | 6 | 0.7283 |
| exact | pre_second | S | standard_dfm | 3.7677 | False | 6 | 0.7283 |
| exact | pre_second | S | bridge | 4.0526 | False | 6 | 0.7283 |

## Point Density Metrics

| model_id | timing_mode | checkpoint_id | outcome_id | n_density_forecasts | mean_log_score | mean_crps | coverage_68 | coverage_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| spf | exact | pre_advance | A | 66 | -2.0460 | 0.9051 | 0.6061 | 0.8485 |
| release_dfm | exact | pre_advance | A | 66 | -2.1981 | 0.9678 | 0.6364 | 0.8182 |
| standard_dfm | exact | pre_advance | A | 66 | -2.1981 | 0.9678 | 0.6364 | 0.8182 |
| bridge | exact | pre_advance | A | 66 | -2.1590 | 1.0553 | 0.7121 | 0.8636 |
| revision_dfm_kalman_em | exact | pre_advance | A | 78 | -2.1148 | 1.0562 | 0.7179 | 0.8974 |
| ar | exact | pre_advance | A | 66 | -2.1697 | 1.0717 | 0.7576 | 0.8939 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 78 | -2.2110 | 1.1304 | 0.7564 | 0.9103 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 78 | -2.3665 | 1.2223 | 0.6410 | 0.7949 |
| no_revision | exact | pre_advance | A | 66 | -2.3428 | 1.2245 | 0.7424 | 0.8788 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 78 | -2.3732 | 1.3092 | 0.6410 | 0.8333 |
| midas_umidas | exact | pre_advance | A | 66 | -3.5433 | 4.5158 | 0.8485 | 0.9848 |
| no_revision | exact | pre_second | S | 65 | -0.8655 | 0.2935 | 0.8769 | 0.9538 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 77 | -0.8439 | 0.2995 | 0.7792 | 0.9221 |
| release_dfm | exact | pre_second | S | 65 | -0.8946 | 0.3116 | 0.8462 | 0.9385 |
| revision_dfm_kalman_em | exact | pre_second | S | 77 | -0.9016 | 0.3182 | 0.7792 | 0.9221 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 77 | -0.8852 | 0.3228 | 0.7922 | 0.9221 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 77 | -0.9112 | 0.3301 | 0.7662 | 0.8961 |
| spf | exact | pre_second | S | 65 | -2.1122 | 1.0045 | 0.6923 | 0.8462 |

## Revision Density Metrics

| model_id | timing_mode | checkpoint_id | outcome_id | n_density_forecasts | mean_log_score | mean_crps | coverage_68 | coverage_90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 77 | -0.8386 | 0.2921 | 0.8052 | 0.9351 |
| no_revision | exact | pre_advance | DELTA_SA | 65 | -0.8655 | 0.2935 | 0.8769 | 0.9538 |
| spf | exact | pre_advance | DELTA_SA | 65 | -0.8655 | 0.2935 | 0.8769 | 0.9538 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 77 | -0.8634 | 0.3031 | 0.8442 | 0.9481 |
| release_dfm | exact | pre_advance | DELTA_SA | 65 | -0.8934 | 0.3098 | 0.8615 | 0.9385 |
| standard_dfm | exact | pre_advance | DELTA_SA | 65 | -0.8934 | 0.3098 | 0.8615 | 0.9385 |
| revision_dfm_kalman_em | exact | pre_advance | DELTA_SA | 77 | -0.8749 | 0.3120 | 0.8182 | 0.8961 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 77 | -0.8934 | 0.3245 | 0.7273 | 0.9091 |
| ar | exact | pre_advance | DELTA_SA | 65 | -0.9531 | 0.3286 | 0.8769 | 0.9538 |
| bridge | exact | pre_advance | DELTA_SA | 65 | -1.0126 | 0.3461 | 0.8769 | 0.9231 |
| midas_umidas | exact | pre_advance | DELTA_SA | 65 | -3.7374 | 5.2007 | 0.9231 | 0.9846 |
| revision_dfm_kalman_em | exact | pre_second | DELTA_TS | 77 | -0.4177 | 0.1834 | 0.8442 | 0.9481 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 77 | -0.4174 | 0.1836 | 0.8442 | 0.9481 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 77 | -0.4192 | 0.1837 | 0.8442 | 0.9481 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 77 | -0.4175 | 0.1867 | 0.8312 | 0.9481 |
| release_dfm | exact | pre_second | DELTA_TS | 65 | -0.6408 | 0.1963 | 0.7538 | 0.8615 |
| no_revision | exact | pre_second | DELTA_TS | 65 | -0.6531 | 0.1966 | 0.7538 | 0.8615 |
| spf | exact | pre_second | DELTA_TS | 65 | -0.6531 | 0.1966 | 0.7538 | 0.8615 |

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
