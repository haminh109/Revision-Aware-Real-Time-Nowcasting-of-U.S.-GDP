# Variance Audit

Generated UTC: `2026-05-23T18:32:37+00:00`

## Interpretation

- `share_positive_variance` and `share_finite_sd` should be 1.0 for state-space density rows.
- `mean_sd_to_rmse` far below 1 indicates under-dispersed predictive intervals; far above 1 indicates overly wide intervals.
- `share_psd` should be 1.0 for serialized release covariance matrices.

## Point Forecast Audit

| model_id | timing_mode | checkpoint_id | target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.7241 | 10.7210 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.4998 | 10.5235 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 9.3383 | 9.3038 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.7241 | 10.7210 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.4998 | 10.5235 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 9.3383 | 9.3038 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.3837 | 3.3630 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1636 | 3.1436 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.2270 | 3.2069 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.4039 | 3.3835 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1621 | 3.1421 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.2192 | 3.1992 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8830 | 2.4891 | 4.1997 | 1.6215 | 4.6948 | 4.6660 | 0.3475 | 0.3454 | 0.6375 | 0.7750 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2089 | 0.3578 | 0.4195 | 0.5770 | 0.6187 | 0.6154 | 0.9377 | 0.9326 | 0.7468 | 0.8734 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.0607 | 0.1766 | 0.4985 | 0.3646 | 0.3719 | 0.3733 | 0.9768 | 0.9805 | 0.7750 | 0.9000 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8830 | 2.4891 | 4.2007 | 1.6216 | 4.6967 | 4.6679 | 0.3474 | 0.3453 | 0.6375 | 0.7875 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2089 | 0.3578 | 0.4195 | 0.5769 | 0.6187 | 0.6154 | 0.9374 | 0.9324 | 0.7468 | 0.8734 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.0607 | 0.1766 | 0.4985 | 0.3646 | 0.3719 | 0.3733 | 0.9767 | 0.9805 | 0.7750 | 0.9000 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.8396 | 1.5601 | 3.7037 | 1.2759 | 5.4533 | 5.4225 | 0.2353 | 0.2340 | 0.5500 | 0.7000 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1803 | 0.3457 | 0.4412 | 0.5620 | 1.4301 | 1.4220 | 0.3952 | 0.3930 | 0.6456 | 0.8354 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.0574 | 0.1735 | 0.5204 | 0.3560 | 0.3719 | 0.3732 | 0.9541 | 0.9573 | 0.7500 | 0.8875 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.8435 | 1.5600 | 3.7063 | 1.2760 | 5.4532 | 5.4225 | 0.2353 | 0.2340 | 0.5625 | 0.7000 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1803 | 0.3458 | 0.4412 | 0.5619 | 1.4305 | 1.4224 | 0.3950 | 0.3928 | 0.6456 | 0.8354 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.0574 | 0.1735 | 0.5203 | 0.3560 | 0.3719 | 0.3731 | 0.9542 | 0.9574 | 0.7500 | 0.8875 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 5.4976 | 5.5017 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.2612 | 1.2591 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.4942 | 1.4852 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 5.6138 | 5.6185 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.2615 | 1.2595 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.4989 | 1.4897 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.1409 | 2.0972 | 5.5084 | 1.5565 | 4.1189 | 4.1273 | 0.3771 | 0.3779 | 0.6125 | 0.8500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1852 | 0.3187 | 0.3876 | 0.5434 | 0.6453 | 0.6418 | 0.8467 | 0.8422 | 0.7089 | 0.8734 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.0681 | 0.1432 | 0.5773 | 0.3676 | 0.3755 | 0.3733 | 0.9848 | 0.9788 | 0.7750 | 0.8750 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.1408 | 2.0967 | 5.5085 | 1.5569 | 4.1202 | 4.1294 | 0.3770 | 0.3779 | 0.6125 | 0.8500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1852 | 0.3187 | 0.3877 | 0.5434 | 0.6486 | 0.6450 | 0.8425 | 0.8378 | 0.7089 | 0.8734 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.0681 | 0.1433 | 0.5760 | 0.3675 | 0.3752 | 0.3730 | 0.9853 | 0.9794 | 0.7750 | 0.8750 | 1.0000 |
| no_revision | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 14.6976 | 14.7106 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3612 | 0.3622 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 14.6976 | 14.7106 |  |  |  |  | 0.0000 |

## Revision Forecast Audit

| model_id | timing_mode | checkpoint_id | revision_target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.8021 | 2.7907 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.6309 | 3.6743 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.5166 | 10.4519 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.8021 | 2.7907 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.6309 | 3.6743 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.5166 | 10.4519 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7162 | 0.7149 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5461 | 0.5444 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.8084 | 2.8103 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7207 | 0.7194 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5453 | 0.5436 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.7987 | 2.8015 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2197 | 0.4339 | 0.5177 | 0.6249 | 0.5695 | 0.5697 | 1.0969 | 1.0973 | 0.8101 | 0.9241 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.0620 | 0.1761 | 0.2067 | 0.3622 | 0.3595 | 0.3614 | 1.0022 | 1.0073 | 0.7722 | 0.8861 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8882 | 2.3444 | 2.9784 | 1.5571 | 1.3468 | 1.3387 | 1.1631 | 1.1561 | 0.8125 | 0.9500 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2197 | 0.4339 | 0.5177 | 0.6249 | 0.5695 | 0.5697 | 1.0969 | 1.0973 | 0.8101 | 0.9241 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.0620 | 0.1760 | 0.2067 | 0.3621 | 0.3595 | 0.3614 | 1.0021 | 1.0072 | 0.7722 | 0.8861 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8882 | 2.3444 | 2.9784 | 1.5570 | 1.3468 | 1.3387 | 1.1631 | 1.1561 | 0.8125 | 0.9500 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1821 | 0.3508 | 0.4588 | 0.5680 | 0.8248 | 0.8248 | 0.6886 | 0.6887 | 0.6962 | 0.8354 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.0585 | 0.1728 | 0.1903 | 0.3515 | 0.3624 | 0.3637 | 0.9662 | 0.9699 | 0.7595 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.5186 | 2.3530 | 3.0114 | 1.5562 | 1.3621 | 1.3543 | 1.1490 | 1.1425 | 0.7750 | 0.9500 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1821 | 0.3497 | 0.4588 | 0.5679 | 0.8228 | 0.8226 | 0.6904 | 0.6903 | 0.6962 | 0.8354 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.0585 | 0.1728 | 0.1903 | 0.3515 | 0.3624 | 0.3638 | 0.9662 | 0.9698 | 0.7595 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.5186 | 2.3530 | 3.0114 | 1.5562 | 1.3621 | 1.3543 | 1.1490 | 1.1425 | 0.7750 | 0.9500 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.4195 | 1.4144 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.0911 | 1.0902 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.8768 | 2.8811 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3996 | 1.3933 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.0950 | 1.0941 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.8744 | 2.8794 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1943 | 0.3283 | 0.3846 | 0.5507 | 0.5875 | 0.5838 | 0.9433 | 0.9374 | 0.7089 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.0691 | 0.1537 | 0.2106 | 0.3676 | 0.4690 | 0.4663 | 0.7882 | 0.7837 | 0.7595 | 0.8481 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8533 | 2.3430 | 3.0707 | 1.5609 | 1.3682 | 1.3596 | 1.1481 | 1.1409 | 0.8000 | 0.9500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1943 | 0.3284 | 0.3846 | 0.5507 | 0.5871 | 0.5834 | 0.9439 | 0.9380 | 0.7089 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.0691 | 0.1537 | 0.2113 | 0.3675 | 0.4715 | 0.4689 | 0.7839 | 0.7795 | 0.7595 | 0.8481 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8540 | 2.3430 | 3.0820 | 1.5610 | 1.3679 | 1.3593 | 1.1484 | 1.1412 | 0.8000 | 0.9500 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3595 | 0.3614 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3468 | 1.3387 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0306 | 0.0693 | 9.3377 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0304 | 0.0593 | 2.7968 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0076 | 0.0322 | 2.2092 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0306 | 0.0693 | 9.3398 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0304 | 0.0593 | 2.7967 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0076 | 0.0322 | 2.2095 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0287 | 0.0629 | 8.5309 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0289 | 0.0601 | 2.7351 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0038 | 0.0313 | 2.2227 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0287 | 0.0628 | 8.5229 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0289 | 0.0601 | 2.7352 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0038 | 0.0313 | 2.2227 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0309 | 0.0448 | 10.1182 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0292 | 0.0436 | 3.2989 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0286 | 0.0426 | 2.4881 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0309 | 0.0448 | 10.1303 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0292 | 0.0436 | 3.2990 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0286 | 0.0427 | 2.4882 |
| revision_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0277 | 0.0613 | 9.6781 |
| revision_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0277 | 0.0601 | 2.9018 |
| revision_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0135 | 0.0315 | 2.2425 |
| revision_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0277 | 0.0610 | 9.6781 |
| revision_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0277 | 0.0584 | 2.9018 |
| revision_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0135 | 0.0315 | 2.2384 |
