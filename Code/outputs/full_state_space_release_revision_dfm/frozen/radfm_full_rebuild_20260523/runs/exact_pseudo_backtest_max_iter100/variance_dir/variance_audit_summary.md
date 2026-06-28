# Variance Audit

Generated UTC: `2026-05-23T10:02:01+00:00`

## Interpretation

- `share_positive_variance` and `share_finite_sd` should be 1.0 for state-space density rows.
- `mean_sd_to_rmse` far below 1 indicates under-dispersed predictive intervals; far above 1 indicates overly wide intervals.
- `share_psd` should be 1.0 for serialized release covariance matrices.

## Point Forecast Audit

| model_id | timing_mode | checkpoint_id | target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.1818 | 7.1369 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.9863 | 6.9421 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.8968 | 6.8539 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.1818 | 7.1369 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.9863 | 6.9421 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.8968 | 6.8539 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.9037 | 4.8737 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.5713 | 4.5427 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.4295 | 4.4019 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.8835 | 4.8533 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.5708 | 4.5422 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.4263 | 4.3987 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7252 | 3.7418 | 5.7589 | 1.9439 | 3.5755 | 3.5659 | 0.5451 | 0.5437 | 0.6250 | 0.8125 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3628 | 0.3981 | 0.4160 | 0.6275 | 0.5948 | 0.5913 | 1.0611 | 1.0549 | 0.7848 | 0.9114 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1311 | 0.1429 | 0.4936 | 0.3828 | 0.3699 | 0.3708 | 1.0326 | 1.0351 | 0.8750 | 0.9375 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7246 | 3.7423 | 5.7589 | 1.9410 | 3.5743 | 3.5655 | 0.5444 | 0.5430 | 0.6250 | 0.8125 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3628 | 0.3981 | 0.4152 | 0.6275 | 0.5948 | 0.5913 | 1.0611 | 1.0549 | 0.7848 | 0.9114 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1311 | 0.1429 | 0.4936 | 0.3828 | 0.3698 | 0.3707 | 1.0327 | 1.0352 | 0.8750 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8718 | 2.6942 | 4.5955 | 1.6769 | 3.6290 | 3.6096 | 0.4646 | 0.4621 | 0.6250 | 0.7750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3108 | 0.3852 | 0.4083 | 0.6146 | 0.6322 | 0.6317 | 0.9730 | 0.9722 | 0.7468 | 0.8734 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1272 | 0.1396 | 0.4944 | 0.3773 | 0.3722 | 0.3721 | 1.0140 | 1.0140 | 0.8500 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8706 | 2.6872 | 4.5955 | 1.6742 | 3.6292 | 3.6100 | 0.4638 | 0.4613 | 0.6250 | 0.7750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3108 | 0.3852 | 0.4083 | 0.6148 | 0.6318 | 0.6313 | 0.9739 | 0.9730 | 0.7342 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1272 | 0.1396 | 0.4944 | 0.3773 | 0.3722 | 0.3721 | 1.0140 | 1.0139 | 0.8500 | 0.9375 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.3669 | 13.3665 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7814 | 4.7703 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2190 | 6.1905 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5464 | 13.5540 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7770 | 4.7628 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2140 | 6.1869 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7861 | 3.6964 | 4.6013 | 1.9240 | 3.7465 | 3.7253 | 0.5165 | 0.5135 | 0.7375 | 0.8875 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3210 | 0.3586 | 0.3773 | 0.5978 | 0.5973 | 0.5947 | 1.0052 | 1.0009 | 0.7722 | 0.9114 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1230 | 0.1453 | 0.4756 | 0.3818 | 0.3715 | 0.3692 | 1.0341 | 1.0276 | 0.8125 | 0.9500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7895 | 3.6991 | 4.6012 | 1.9248 | 3.7496 | 3.7286 | 0.5162 | 0.5133 | 0.7375 | 0.8875 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3210 | 0.3586 | 0.3773 | 0.5979 | 0.5958 | 0.5932 | 1.0078 | 1.0034 | 0.7848 | 0.9114 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1226 | 0.1452 | 0.4786 | 0.3817 | 0.3710 | 0.3687 | 1.0352 | 1.0287 | 0.8250 | 0.9500 | 1.0000 |
| no_revision | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.3127 | 7.2694 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3612 | 0.3622 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.3127 | 7.2694 |  |  |  |  | 0.0000 |

## Revision Forecast Audit

| model_id | timing_mode | checkpoint_id | revision_target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6773 | 0.6781 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4102 | 0.4079 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.0308 | 3.0296 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6773 | 0.6781 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4102 | 0.4079 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.0308 | 3.0296 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7229 | 0.7183 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4944 | 0.4917 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9786 | 1.9682 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7267 | 0.7221 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4951 | 0.4923 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9758 | 1.9653 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4018 | 0.4423 | 0.4771 | 0.6640 | 0.5695 | 0.5697 | 1.1656 | 1.1660 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1331 | 0.1438 | 0.1606 | 0.3807 | 0.3595 | 0.3614 | 1.0535 | 1.0589 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1402 | 4.5443 | 5.1676 | 2.1327 | 1.3468 | 1.3387 | 1.5930 | 1.5835 | 0.8875 | 0.9750 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4018 | 0.4423 | 0.4771 | 0.6640 | 0.5695 | 0.5697 | 1.1656 | 1.1660 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1331 | 0.1438 | 0.1606 | 0.3807 | 0.3595 | 0.3614 | 1.0535 | 1.0589 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1402 | 4.5443 | 5.1676 | 2.1327 | 1.3468 | 1.3387 | 1.5931 | 1.5835 | 0.8875 | 0.9750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3279 | 0.3907 | 0.4146 | 0.6205 | 0.6356 | 0.6360 | 0.9757 | 0.9763 | 0.7089 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1288 | 0.1406 | 0.1549 | 0.3750 | 0.3606 | 0.3619 | 1.0362 | 1.0399 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1419 | 4.5546 | 5.1594 | 2.1339 | 1.3485 | 1.3404 | 1.5921 | 1.5825 | 0.8875 | 0.9875 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3279 | 0.3907 | 0.4146 | 0.6204 | 0.6364 | 0.6370 | 0.9739 | 0.9748 | 0.7089 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1288 | 0.1406 | 0.1549 | 0.3750 | 0.3606 | 0.3619 | 1.0362 | 1.0399 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1419 | 4.5546 | 5.1594 | 2.1339 | 1.3485 | 1.3404 | 1.5921 | 1.5825 | 0.8875 | 0.9875 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.9201 | 13.8667 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1441 | 3.1241 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.7339 | 7.8786 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5309 | 13.4972 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1442 | 3.1242 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.6901 | 7.8417 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3220 | 0.3657 | 0.3814 | 0.6015 | 0.6433 | 0.6405 | 0.9391 | 0.9350 | 0.7848 | 0.9114 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1304 | 0.1458 | 0.1561 | 0.3788 | 0.3802 | 0.3778 | 1.0027 | 0.9964 | 0.8228 | 0.9367 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.0836 | 4.6443 | 5.3108 | 2.1493 | 1.3958 | 1.3871 | 1.5496 | 1.5398 | 0.8750 | 0.9750 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3220 | 0.3657 | 0.3814 | 0.6015 | 0.6427 | 0.6399 | 0.9400 | 0.9359 | 0.7848 | 0.9114 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1304 | 0.1458 | 0.1561 | 0.3789 | 0.3789 | 0.3765 | 1.0063 | 1.0000 | 0.8228 | 0.9367 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.0836 | 4.6443 | 5.3108 | 2.1493 | 1.3962 | 1.3875 | 1.5490 | 1.5393 | 0.8750 | 0.9750 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3595 | 0.3614 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3468 | 1.3387 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0635 | 0.0672 | 14.7658 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0648 | 4.9201 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0261 | 0.0428 | 4.4542 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0635 | 0.0672 | 14.7737 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0610 | 0.0648 | 4.9201 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0261 | 0.0428 | 4.4542 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0649 | 13.9800 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0609 | 0.0646 | 4.9182 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0289 | 0.0475 | 4.4774 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0649 | 13.9290 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0609 | 0.0646 | 4.9182 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0289 | 0.0474 | 4.4774 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0604 | 0.0639 | 17.7288 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0576 | 0.0626 | 5.3612 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0573 | 0.0620 | 4.8084 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0604 | 0.0640 | 17.7180 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0576 | 0.0626 | 5.3611 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0573 | 0.0619 | 4.8084 |
| revision_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0626 | 0.0663 | 16.6459 |
| revision_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0623 | 0.0657 | 4.9210 |
| revision_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0406 | 0.0519 | 4.4744 |
| revision_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0626 | 0.0663 | 16.6706 |
| revision_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0623 | 0.0657 | 4.9210 |
| revision_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0406 | 0.0519 | 4.4744 |
