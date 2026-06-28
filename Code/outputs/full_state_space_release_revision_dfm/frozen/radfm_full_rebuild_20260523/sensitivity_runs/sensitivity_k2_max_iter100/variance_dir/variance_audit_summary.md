# Variance Audit

Generated UTC: `2026-05-23T14:47:11+00:00`

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
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.6803 | 3.4284 | 4.8437 | 1.8469 | 4.1553 | 4.1367 | 0.4465 | 0.4445 | 0.5750 | 0.7875 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3540 | 0.3951 | 0.4108 | 0.6238 | 0.6015 | 0.5980 | 1.0431 | 1.0370 | 0.7215 | 0.8861 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1313 | 0.1436 | 0.4939 | 0.3830 | 0.3689 | 0.3698 | 1.0358 | 1.0382 | 0.8750 | 0.9375 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.6533 | 3.4277 | 4.8437 | 1.8467 | 4.3209 | 4.3028 | 0.4292 | 0.4274 | 0.5875 | 0.7875 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3540 | 0.3951 | 0.4108 | 0.6239 | 0.6012 | 0.5976 | 1.0439 | 1.0377 | 0.7215 | 0.8861 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1313 | 0.1436 | 0.4939 | 0.3830 | 0.3688 | 0.3697 | 1.0360 | 1.0385 | 0.8750 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.7813 | 2.5212 | 3.9692 | 1.6052 | 4.2887 | 4.2678 | 0.3761 | 0.3743 | 0.6250 | 0.7500 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2986 | 0.3563 | 0.3825 | 0.5945 | 0.6661 | 0.6623 | 0.8977 | 0.8926 | 0.6835 | 0.8481 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1262 | 0.1394 | 0.4660 | 0.3760 | 0.3725 | 0.3725 | 1.0095 | 1.0093 | 0.8250 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.7916 | 2.5212 | 3.9692 | 1.6053 | 4.3793 | 4.3582 | 0.3683 | 0.3666 | 0.6125 | 0.7625 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2986 | 0.3567 | 0.3819 | 0.5946 | 0.6678 | 0.6638 | 0.8957 | 0.8904 | 0.6709 | 0.8481 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1262 | 0.1394 | 0.4661 | 0.3760 | 0.3725 | 0.3724 | 1.0097 | 1.0096 | 0.8250 | 0.9375 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.3669 | 13.3665 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7814 | 4.7703 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2190 | 6.1905 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5464 | 13.5540 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7770 | 4.7628 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2140 | 6.1869 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.3766 | 3.2192 | 4.2627 | 1.8331 | 3.8688 | 3.8454 | 0.4767 | 0.4738 | 0.6875 | 0.8625 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3119 | 0.3558 | 0.3681 | 0.5930 | 0.7344 | 0.7356 | 0.8061 | 0.8074 | 0.7722 | 0.8861 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1259 | 0.1418 | 0.4934 | 0.3818 | 0.3802 | 0.3780 | 1.0100 | 1.0041 | 0.7875 | 0.9250 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.3840 | 3.2304 | 4.2626 | 1.8340 | 3.8640 | 3.8407 | 0.4775 | 0.4746 | 0.6875 | 0.8625 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3149 | 0.3558 | 0.3681 | 0.5929 | 0.6981 | 0.6987 | 0.8486 | 0.8493 | 0.7722 | 0.8861 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1279 | 0.1418 | 0.4872 | 0.3818 | 0.3786 | 0.3763 | 1.0145 | 1.0085 | 0.7875 | 0.9250 | 1.0000 |
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
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4035 | 0.4420 | 0.4717 | 0.6636 | 0.5695 | 0.5697 | 1.1650 | 1.1654 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1324 | 0.1450 | 0.1601 | 0.3809 | 0.3595 | 0.3614 | 1.0539 | 1.0593 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1313 | 4.5478 | 5.1624 | 2.1328 | 1.3468 | 1.3387 | 1.5932 | 1.5836 | 0.8875 | 0.9750 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4035 | 0.4420 | 0.4717 | 0.6636 | 0.5695 | 0.5697 | 1.1649 | 1.1653 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1330 | 0.1450 | 0.1601 | 0.3809 | 0.3595 | 0.3614 | 1.0541 | 1.0595 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1313 | 4.5478 | 5.1624 | 2.1329 | 1.3468 | 1.3387 | 1.5932 | 1.5837 | 0.8875 | 0.9750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3010 | 0.3618 | 0.3845 | 0.5996 | 0.6291 | 0.6284 | 0.9542 | 0.9531 | 0.6709 | 0.9114 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1280 | 0.1404 | 0.1542 | 0.3742 | 0.3601 | 0.3616 | 1.0351 | 1.0393 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1324 | 4.5586 | 5.1599 | 2.1342 | 1.3485 | 1.3404 | 1.5922 | 1.5826 | 0.8875 | 0.9875 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3010 | 0.3636 | 0.3845 | 0.5998 | 0.6387 | 0.6383 | 0.9398 | 0.9391 | 0.6709 | 0.8987 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1280 | 0.1404 | 0.1542 | 0.3742 | 0.3602 | 0.3617 | 1.0348 | 1.0391 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1324 | 4.5586 | 5.1599 | 2.1342 | 1.3485 | 1.3404 | 1.5922 | 1.5826 | 0.8875 | 0.9875 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.9201 | 13.8667 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1441 | 3.1241 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.7339 | 7.8786 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5309 | 13.4972 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1442 | 3.1242 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.6901 | 7.8417 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3266 | 0.3619 | 0.3786 | 0.5982 | 0.6453 | 0.6427 | 0.9309 | 0.9271 | 0.7975 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1289 | 0.1424 | 0.1597 | 0.3785 | 0.4673 | 0.4662 | 0.8119 | 0.8100 | 0.7722 | 0.9114 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.0882 | 4.6195 | 5.2889 | 2.1479 | 1.3929 | 1.3842 | 1.5517 | 1.5420 | 0.8750 | 0.9750 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3266 | 0.3619 | 0.3787 | 0.5982 | 0.6450 | 0.6424 | 0.9313 | 0.9275 | 0.7975 | 0.8987 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1289 | 0.1424 | 0.1597 | 0.3785 | 0.4450 | 0.4436 | 0.8532 | 0.8506 | 0.7848 | 0.9114 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.0882 | 4.6194 | 5.2889 | 2.1479 | 1.3926 | 1.3839 | 1.5520 | 1.5423 | 0.8750 | 0.9750 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3595 | 0.3614 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3468 | 1.3387 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0624 | 0.0678 | 13.5566 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0604 | 0.0646 | 4.9172 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0272 | 0.0408 | 4.4547 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0624 | 0.0677 | 13.5567 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0614 | 0.0647 | 4.9172 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0272 | 0.0408 | 4.4547 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0608 | 0.0648 | 13.1194 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0602 | 0.0644 | 4.8649 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0314 | 0.0463 | 4.4804 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0607 | 0.0648 | 13.1049 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0607 | 0.0644 | 4.8649 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0314 | 0.0461 | 4.4804 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0601 | 0.0640 | 15.8753 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0575 | 0.0631 | 5.3397 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0573 | 0.0624 | 4.7771 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0601 | 0.0640 | 15.8745 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0575 | 0.0631 | 5.3396 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0573 | 0.0624 | 4.7770 |
| revision_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0655 | 15.9226 |
| revision_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0608 | 0.0649 | 4.9185 |
| revision_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0357 | 0.0463 | 4.4793 |
| revision_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0655 | 15.9226 |
| revision_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0608 | 0.0650 | 4.9185 |
| revision_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0357 | 0.0463 | 4.4793 |
