# Variance Audit

Generated UTC: `2026-05-23T12:25:47+00:00`

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
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7857 | 4.1972 | 7.8610 | 2.0538 | 3.8574 | 3.8416 | 0.5346 | 0.5324 | 0.6250 | 0.8125 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3614 | 0.4073 | 0.4393 | 0.6339 | 0.6021 | 0.5984 | 1.0593 | 1.0528 | 0.7975 | 0.9114 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1299 | 0.1425 | 0.4968 | 0.3816 | 0.3710 | 0.3719 | 1.0261 | 1.0285 | 0.8500 | 0.9375 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7845 | 4.2063 | 7.8610 | 2.0570 | 3.8560 | 3.8413 | 0.5355 | 0.5335 | 0.6250 | 0.8000 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3614 | 0.4089 | 0.4393 | 0.6339 | 0.6019 | 0.5982 | 1.0598 | 1.0533 | 0.7975 | 0.9114 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1299 | 0.1425 | 0.4968 | 0.3816 | 0.3709 | 0.3718 | 1.0262 | 1.0287 | 0.8500 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8750 | 3.0745 | 6.1971 | 1.7783 | 3.7984 | 3.7754 | 0.4710 | 0.4682 | 0.6000 | 0.7625 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3559 | 0.3961 | 0.4203 | 0.6259 | 0.6439 | 0.6432 | 0.9731 | 0.9721 | 0.7468 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1257 | 0.1387 | 0.4973 | 0.3756 | 0.3728 | 0.3725 | 1.0084 | 1.0076 | 0.8375 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8746 | 3.0836 | 6.1971 | 1.7775 | 3.7974 | 3.7745 | 0.4709 | 0.4681 | 0.6000 | 0.7750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3559 | 0.3952 | 0.4203 | 0.6259 | 0.6433 | 0.6426 | 0.9740 | 0.9730 | 0.7342 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1257 | 0.1387 | 0.4973 | 0.3756 | 0.3728 | 0.3725 | 1.0084 | 1.0076 | 0.8375 | 0.9375 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.3669 | 13.3665 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7814 | 4.7703 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2190 | 6.1905 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5464 | 13.5540 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7770 | 4.7628 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2140 | 6.1869 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.1024 | 4.7639 | 7.0900 | 2.2009 | 4.6739 | 4.6544 | 0.4729 | 0.4709 | 0.7500 | 0.8750 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3391 | 0.3660 | 0.3833 | 0.6040 | 0.6163 | 0.6128 | 0.9856 | 0.9800 | 0.7975 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1370 | 0.1482 | 0.5101 | 0.3931 | 0.3748 | 0.3725 | 1.0552 | 1.0486 | 0.8375 | 0.9500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.0959 | 4.7639 | 7.0900 | 2.2012 | 4.6738 | 4.6547 | 0.4729 | 0.4710 | 0.7500 | 0.8625 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3391 | 0.3660 | 0.3833 | 0.6040 | 0.6161 | 0.6126 | 0.9858 | 0.9802 | 0.7975 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1370 | 0.1482 | 0.5082 | 0.3930 | 0.3741 | 0.3718 | 1.0571 | 1.0505 | 0.8375 | 0.9500 | 1.0000 |
| no_revision | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.3127 | 7.2694 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3612 | 0.3622 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.3127 | 7.2694 |  |  |  |  | 0.0000 |

## Revision Forecast Audit

| model_id | timing_mode | checkpoint_id | revision_target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6773 | 0.6781 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4102 | 0.4079 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.1305 | 4.1199 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6773 | 0.6781 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4102 | 0.4079 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.1305 | 4.1199 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7229 | 0.7183 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4944 | 0.4917 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.1716 | 2.1891 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7267 | 0.7221 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4951 | 0.4923 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.1708 | 2.1879 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4040 | 0.4453 | 0.4812 | 0.6671 | 0.5695 | 0.5697 | 1.1711 | 1.1715 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1319 | 0.1436 | 0.1595 | 0.3798 | 0.3595 | 0.3614 | 1.0509 | 1.0563 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 74.0000 | 74.0000 | 1.0000 | 1.0000 | 2.1726 | 2.3526 | 2.4220 | 1.5236 | 1.3652 | 1.4106 | 1.0801 | 1.1160 | 0.7838 | 0.8919 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4040 | 0.4453 | 0.4812 | 0.6670 | 0.5695 | 0.5697 | 1.1709 | 1.1713 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1324 | 0.1437 | 0.1595 | 0.3798 | 0.3595 | 0.3614 | 1.0509 | 1.0563 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 74.0000 | 74.0000 | 1.0000 | 1.0000 | 2.1725 | 2.3526 | 2.4220 | 1.5236 | 1.3652 | 1.4106 | 1.0801 | 1.1160 | 0.7838 | 0.8919 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3592 | 0.4038 | 0.4291 | 0.6328 | 0.6237 | 0.6236 | 1.0147 | 1.0145 | 0.7595 | 0.9114 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1277 | 0.1396 | 0.1537 | 0.3735 | 0.3606 | 0.3619 | 1.0321 | 1.0358 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 74.0000 | 74.0000 | 1.0000 | 1.0000 | 2.1671 | 2.3365 | 2.4129 | 1.5190 | 1.3685 | 1.4141 | 1.0741 | 1.1100 | 0.7838 | 0.8919 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3592 | 0.4038 | 0.4291 | 0.6328 | 0.6249 | 0.6249 | 1.0126 | 1.0127 | 0.7595 | 0.9114 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1277 | 0.1396 | 0.1537 | 0.3735 | 0.3605 | 0.3618 | 1.0324 | 1.0361 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 74.0000 | 74.0000 | 1.0000 | 1.0000 | 2.1671 | 2.3365 | 2.4129 | 1.5191 | 1.3685 | 1.4141 | 1.0742 | 1.1101 | 0.7838 | 0.8919 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.9201 | 13.8667 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1441 | 3.1241 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 15.4524 | 15.7083 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5309 | 13.4972 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1442 | 3.1242 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 15.4661 | 15.7364 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3450 | 0.3752 | 0.3882 | 0.6085 | 0.5741 | 0.5720 | 1.0639 | 1.0599 | 0.7848 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1363 | 0.1476 | 0.1735 | 0.3884 | 0.3853 | 0.3828 | 1.0146 | 1.0081 | 0.8608 | 0.9241 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 74.0000 | 74.0000 | 1.0000 | 1.0000 | 2.0913 | 2.2858 | 2.3529 | 1.5008 | 1.4068 | 1.4618 | 1.0267 | 1.0668 | 0.7297 | 0.8784 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3450 | 0.3752 | 0.3881 | 0.6085 | 0.5738 | 0.5716 | 1.0645 | 1.0605 | 0.7848 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1363 | 0.1478 | 0.1735 | 0.3884 | 0.3845 | 0.3820 | 1.0166 | 1.0102 | 0.8608 | 0.9241 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 74.0000 | 74.0000 | 1.0000 | 1.0000 | 2.0913 | 2.2858 | 2.3566 | 1.5008 | 1.4072 | 1.4621 | 1.0265 | 1.0665 | 0.7297 | 0.8784 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3595 | 0.3614 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 74.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3652 | 1.4106 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0636 | 0.0673 | 15.8321 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0613 | 0.0643 | 2.8828 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0333 | 0.0449 | 2.2534 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0633 | 0.0673 | 15.9159 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0617 | 0.0643 | 2.8828 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0333 | 0.0450 | 2.2534 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0614 | 0.0648 | 14.8418 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0609 | 0.0645 | 2.7439 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0372 | 0.0515 | 2.2566 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0613 | 0.0648 | 14.8418 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0613 | 0.0645 | 2.7439 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0372 | 0.0515 | 2.2566 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0603 | 0.0653 | 21.4855 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0577 | 0.0640 | 3.1446 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0577 | 0.0633 | 2.4554 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0603 | 0.0653 | 21.4817 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0577 | 0.0640 | 3.1446 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0577 | 0.0633 | 2.4554 |
| revision_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0620 | 0.0668 | 20.8572 |
| revision_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0618 | 0.0666 | 2.7736 |
| revision_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0462 | 0.0538 | 2.2580 |
| revision_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0620 | 0.0668 | 20.8503 |
| revision_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0618 | 0.0666 | 2.7736 |
| revision_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0462 | 0.0538 | 2.2580 |
