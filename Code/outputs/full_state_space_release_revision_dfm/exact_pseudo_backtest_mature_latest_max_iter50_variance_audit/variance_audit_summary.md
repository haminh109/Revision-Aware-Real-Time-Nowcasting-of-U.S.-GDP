# Variance Audit

Generated UTC: `2026-05-23T13:46:51+00:00`

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
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7888 | 3.9943 | 7.0393 | 2.0083 | 3.8695 | 3.8558 | 0.5209 | 0.5190 | 0.6375 | 0.8125 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3589 | 0.4035 | 0.4328 | 0.6298 | 0.6034 | 0.5996 | 1.0503 | 1.0438 | 0.7848 | 0.9114 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1314 | 0.1433 | 0.4969 | 0.3832 | 0.3703 | 0.3712 | 1.0324 | 1.0349 | 0.8625 | 0.9375 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.7877 | 4.0018 | 7.0393 | 2.0082 | 3.8795 | 3.8663 | 0.5194 | 0.5177 | 0.6375 | 0.8000 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3589 | 0.4035 | 0.4328 | 0.6298 | 0.6027 | 0.5990 | 1.0514 | 1.0449 | 0.7848 | 0.9114 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1314 | 0.1433 | 0.4969 | 0.3832 | 0.3702 | 0.3711 | 1.0326 | 1.0350 | 0.8625 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8710 | 2.9268 | 5.5126 | 1.7359 | 3.8281 | 3.8055 | 0.4562 | 0.4535 | 0.6000 | 0.7625 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3520 | 0.3885 | 0.4132 | 0.6203 | 0.6419 | 0.6409 | 0.9678 | 0.9664 | 0.7342 | 0.8734 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1274 | 0.1397 | 0.4969 | 0.3777 | 0.3715 | 0.3712 | 1.0175 | 1.0168 | 0.8375 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.8706 | 2.9321 | 5.5126 | 1.7354 | 3.8267 | 3.8042 | 0.4562 | 0.4535 | 0.6000 | 0.7625 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3520 | 0.3880 | 0.4132 | 0.6202 | 0.6414 | 0.6404 | 0.9685 | 0.9669 | 0.7215 | 0.8734 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1274 | 0.1397 | 0.4969 | 0.3777 | 0.3715 | 0.3712 | 1.0175 | 1.0168 | 0.8375 | 0.9375 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.3669 | 13.3665 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7814 | 4.7703 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2190 | 6.1905 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5464 | 13.5540 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7770 | 4.7628 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2140 | 6.1869 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.9862 | 4.5436 | 5.7453 | 2.1092 | 4.7157 | 4.6958 | 0.4492 | 0.4473 | 0.7500 | 0.8500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3422 | 0.3653 | 0.3820 | 0.6037 | 0.6104 | 0.6075 | 0.9938 | 0.9889 | 0.8101 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1367 | 0.1477 | 0.5085 | 0.3928 | 0.3733 | 0.3710 | 1.0586 | 1.0521 | 0.8500 | 0.9500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.9810 | 4.5422 | 5.7400 | 2.1096 | 4.7164 | 4.6968 | 0.4492 | 0.4473 | 0.7500 | 0.8500 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3422 | 0.3654 | 0.3820 | 0.6036 | 0.6103 | 0.6073 | 0.9939 | 0.9890 | 0.7975 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1367 | 0.1477 | 0.5058 | 0.3927 | 0.3725 | 0.3702 | 1.0607 | 1.0542 | 0.8500 | 0.9500 | 1.0000 |
| no_revision | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.3127 | 7.2694 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3612 | 0.3622 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.3127 | 7.2694 |  |  |  |  | 0.0000 |

## Revision Forecast Audit

| model_id | timing_mode | checkpoint_id | revision_target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6773 | 0.6781 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4102 | 0.4079 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.6306 | 3.6613 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6773 | 0.6781 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4102 | 0.4079 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.6306 | 3.6613 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7229 | 0.7183 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4944 | 0.4917 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.1407 | 2.1273 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.7267 | 0.7221 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4951 | 0.4923 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.1386 | 2.1253 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4020 | 0.4414 | 0.4766 | 0.6642 | 0.5695 | 0.5697 | 1.1660 | 1.1664 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1327 | 0.1441 | 0.1608 | 0.3811 | 0.3595 | 0.3614 | 1.0545 | 1.0599 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.8557 | 4.4752 | 4.9576 | 2.0982 | 1.3399 | 1.3391 | 1.5668 | 1.5659 | 0.8875 | 0.9750 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4020 | 0.4414 | 0.4766 | 0.6642 | 0.5695 | 0.5697 | 1.1660 | 1.1664 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1335 | 0.1441 | 0.1608 | 0.3811 | 0.3595 | 0.3614 | 1.0545 | 1.0599 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.8557 | 4.4752 | 4.9576 | 2.0982 | 1.3399 | 1.3391 | 1.5668 | 1.5659 | 0.8875 | 0.9750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3546 | 0.3946 | 0.4194 | 0.6257 | 0.6049 | 0.6055 | 1.0335 | 1.0344 | 0.7468 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1290 | 0.1407 | 0.1552 | 0.3753 | 0.3607 | 0.3619 | 1.0368 | 1.0405 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.8658 | 4.4575 | 4.9234 | 2.0948 | 1.3422 | 1.3416 | 1.5614 | 1.5607 | 0.8750 | 0.9750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3546 | 0.3946 | 0.4194 | 0.6258 | 0.6061 | 0.6067 | 1.0314 | 1.0326 | 0.7468 | 0.8861 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1290 | 0.1407 | 0.1552 | 0.3753 | 0.3606 | 0.3619 | 1.0370 | 1.0407 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.8658 | 4.4577 | 4.9234 | 2.0948 | 1.3423 | 1.3416 | 1.5614 | 1.5607 | 0.8750 | 0.9750 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.9201 | 13.8667 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1441 | 3.1241 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 9.0497 | 9.2417 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5309 | 13.4972 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1442 | 3.1242 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 9.0262 | 9.2255 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3459 | 0.3737 | 0.3879 | 0.6085 | 0.5705 | 0.5689 | 1.0696 | 1.0667 | 0.7848 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1399 | 0.1481 | 0.1731 | 0.3886 | 0.3836 | 0.3812 | 1.0195 | 1.0131 | 0.8481 | 0.9241 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.8709 | 4.5377 | 5.0683 | 2.1152 | 1.3819 | 1.3856 | 1.5265 | 1.5306 | 0.8875 | 0.9750 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3459 | 0.3736 | 0.3877 | 0.6085 | 0.5701 | 0.5686 | 1.0702 | 1.0673 | 0.7975 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1399 | 0.1481 | 0.1731 | 0.3886 | 0.3829 | 0.3805 | 1.0214 | 1.0150 | 0.8481 | 0.9241 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 3.8709 | 4.5377 | 5.0680 | 2.1152 | 1.3822 | 1.3859 | 1.5262 | 1.5303 | 0.8875 | 0.9750 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3595 | 0.3614 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3399 | 1.3391 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0637 | 0.0674 | 15.7566 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0612 | 0.0643 | 4.8554 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0298 | 0.0424 | 4.3820 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0635 | 0.0674 | 15.7816 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0608 | 0.0643 | 4.8554 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0298 | 0.0423 | 4.3820 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0617 | 0.0650 | 14.7668 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0610 | 0.0646 | 4.8256 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0338 | 0.0480 | 4.3796 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0615 | 0.0650 | 14.8030 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0615 | 0.0646 | 4.8221 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0338 | 0.0480 | 4.3796 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0605 | 0.0656 | 20.4455 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0577 | 0.0643 | 5.3731 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0575 | 0.0636 | 4.7150 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0605 | 0.0656 | 20.4440 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0577 | 0.0643 | 5.3731 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0575 | 0.0636 | 4.7150 |
| revision_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0624 | 0.0670 | 19.6978 |
| revision_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0621 | 0.0668 | 4.8495 |
| revision_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0442 | 0.0513 | 4.3918 |
| revision_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0623 | 0.0670 | 19.7182 |
| revision_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0621 | 0.0668 | 4.8495 |
| revision_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0442 | 0.0513 | 4.3918 |
