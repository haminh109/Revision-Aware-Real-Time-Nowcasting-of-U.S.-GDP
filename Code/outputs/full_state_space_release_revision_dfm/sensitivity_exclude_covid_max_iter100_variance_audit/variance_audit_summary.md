# Variance Audit

Generated UTC: `2026-05-23T20:53:40+00:00`

## Interpretation

- `share_positive_variance` and `share_finite_sd` should be 1.0 for state-space density rows.
- `mean_sd_to_rmse` far below 1 indicates under-dispersed predictive intervals; far above 1 indicates overly wide intervals.
- `share_psd` should be 1.0 for serialized release covariance matrices.

## Point Forecast Audit

| model_id | timing_mode | checkpoint_id | target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9541 | 1.9553 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.1426 | 2.1489 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.2047 | 2.2123 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9541 | 1.9553 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.1426 | 2.1489 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.2047 | 2.2123 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9700 | 1.9584 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.0249 | 2.0131 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9907 | 1.9796 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9572 | 1.9463 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.0072 | 1.9956 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.9806 | 1.9697 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 2.7252 | 3.7432 | 5.7589 | 1.9500 | 2.2994 | 2.3425 | 0.8325 | 0.8481 | 0.6410 | 0.8333 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3628 | 0.4002 | 0.4160 | 0.6280 | 0.5895 | 0.5856 | 1.0724 | 1.0654 | 0.7922 | 0.9221 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 0.1311 | 0.1427 | 0.4936 | 0.3828 | 0.3708 | 0.3706 | 1.0329 | 1.0324 | 0.8718 | 0.9359 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 2.7246 | 3.7437 | 5.7589 | 1.9470 | 2.2942 | 2.3397 | 0.8322 | 0.8487 | 0.6410 | 0.8333 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3628 | 0.4002 | 0.4152 | 0.6280 | 0.5894 | 0.5856 | 1.0724 | 1.0654 | 0.7922 | 0.9221 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 0.1311 | 0.1428 | 0.4936 | 0.3828 | 0.3707 | 0.3705 | 1.0331 | 1.0326 | 0.8718 | 0.9359 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 1.8719 | 2.6950 | 4.5955 | 1.6830 | 2.1516 | 2.1748 | 0.7739 | 0.7822 | 0.6410 | 0.7949 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3108 | 0.3867 | 0.4083 | 0.6152 | 0.5987 | 0.5997 | 1.0258 | 1.0275 | 0.7662 | 0.8961 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 0.1272 | 0.1395 | 0.4944 | 0.3773 | 0.3736 | 0.3727 | 1.0122 | 1.0098 | 0.8462 | 0.9359 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 1.8718 | 2.6914 | 4.5955 | 1.6802 | 2.1451 | 2.1700 | 0.7743 | 0.7833 | 0.6410 | 0.7949 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3108 | 0.3867 | 0.4083 | 0.6154 | 0.5974 | 0.5984 | 1.0285 | 1.0300 | 0.7532 | 0.9091 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 0.1272 | 0.1395 | 0.4944 | 0.3773 | 0.3737 | 0.3728 | 1.0122 | 1.0097 | 0.8462 | 0.9359 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 8.8949 | 8.9709 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.3054 | 2.3232 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.3970 | 2.3945 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 8.9584 | 9.0548 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.2944 | 2.3065 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.3872 | 2.3874 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 2.8148 | 3.7011 | 4.6013 | 1.9274 | 2.1831 | 2.1801 | 0.8841 | 0.8829 | 0.7564 | 0.9103 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3210 | 0.3593 | 0.3773 | 0.5980 | 0.5572 | 0.5567 | 1.0741 | 1.0732 | 0.7792 | 0.9221 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 0.1230 | 0.1452 | 0.4756 | 0.3818 | 0.3698 | 0.3675 | 1.0388 | 1.0323 | 0.8205 | 0.9487 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 2.8129 | 3.7014 | 4.6012 | 1.9283 | 2.1886 | 2.1863 | 0.8820 | 0.8810 | 0.7564 | 0.9103 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3210 | 0.3593 | 0.3773 | 0.5980 | 0.5556 | 0.5551 | 1.0774 | 1.0763 | 0.7922 | 0.9221 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 0.1226 | 0.1452 | 0.4786 | 0.3817 | 0.3694 | 0.3670 | 1.0399 | 1.0333 | 0.8333 | 0.9487 | 1.0000 |
| no_revision | exact | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.2575 | 2.2442 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | S | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5618 | 0.5606 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | T | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3626 | 0.3625 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | A | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.2575 | 2.2442 |  |  |  |  | 0.0000 |

## Revision Forecast Audit

| model_id | timing_mode | checkpoint_id | revision_target_id | n_forecasts | n_variance | share_positive_variance | share_finite_sd | min_forecast_variance | median_forecast_variance | max_forecast_variance | mean_forecast_sd | empirical_error_sd | RMSE | mean_sd_to_rmse | mean_sd_to_empirical_error_sd | coverage_68 | coverage_90 | model_implied_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ar | exact | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6119 | 0.6113 |  |  |  |  | 0.0000 |
| ar | exact | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4079 | 0.4053 |  |  |  |  | 0.0000 |
| ar | exact | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.3554 | 2.3430 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6119 | 0.6113 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4079 | 0.4053 |  |  |  |  | 0.0000 |
| ar | pseudo | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 2.3554 | 2.3430 |  |  |  |  | 0.0000 |
| bridge | exact | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6441 | 0.6399 |  |  |  |  | 0.0000 |
| bridge | exact | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4948 | 0.4917 |  |  |  |  | 0.0000 |
| bridge | exact | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.8278 | 1.8189 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.6501 | 0.6459 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.4949 | 0.4918 |  |  |  |  | 0.0000 |
| bridge | pseudo | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.8244 | 1.8154 |  |  |  |  | 0.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.4018 | 0.4432 | 0.4771 | 0.6643 | 0.5618 | 0.5606 | 1.1850 | 1.1824 | 0.8442 | 0.9481 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.1331 | 0.1436 | 0.1606 | 0.3806 | 0.3610 | 0.3617 | 1.0521 | 1.0542 | 0.8442 | 0.9481 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 4.1402 | 4.5532 | 5.1676 | 2.1344 | 1.2979 | 1.2899 | 1.6547 | 1.6445 | 0.8974 | 0.9872 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.4018 | 0.4432 | 0.4771 | 0.6643 | 0.5618 | 0.5606 | 1.1849 | 1.1823 | 0.8442 | 0.9481 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.1331 | 0.1437 | 0.1606 | 0.3806 | 0.3610 | 0.3617 | 1.0521 | 1.0542 | 0.8442 | 0.9481 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 4.1402 | 4.5532 | 5.1676 | 2.1344 | 1.2979 | 1.2899 | 1.6547 | 1.6445 | 0.8974 | 0.9872 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3279 | 0.3907 | 0.4146 | 0.6211 | 0.5862 | 0.5878 | 1.0566 | 1.0595 | 0.7273 | 0.9091 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.1288 | 0.1404 | 0.1549 | 0.3749 | 0.3623 | 0.3625 | 1.0341 | 1.0348 | 0.8442 | 0.9481 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 4.1419 | 4.5644 | 5.1594 | 2.1356 | 1.3003 | 1.2922 | 1.6526 | 1.6425 | 0.8974 | 0.9872 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3279 | 0.3907 | 0.4146 | 0.6210 | 0.5864 | 0.5883 | 1.0555 | 1.0589 | 0.7273 | 0.9091 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.1288 | 0.1404 | 0.1549 | 0.3749 | 0.3623 | 0.3625 | 1.0342 | 1.0348 | 0.8442 | 0.9481 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 4.1419 | 4.5644 | 5.1594 | 2.1356 | 1.3003 | 1.2922 | 1.6527 | 1.6425 | 0.8974 | 0.9872 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.8334 | 10.8351 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.2675 | 1.2622 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.6775 | 6.7547 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 10.2417 | 10.2770 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.2676 | 1.2616 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.6296 | 6.7130 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3220 | 0.3657 | 0.3814 | 0.6017 | 0.5506 | 0.5504 | 1.0933 | 1.0930 | 0.8052 | 0.9351 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.1304 | 0.1458 | 0.1561 | 0.3787 | 0.3636 | 0.3616 | 1.0473 | 1.0417 | 0.8312 | 0.9481 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 4.0836 | 4.6499 | 5.3108 | 2.1514 | 1.3498 | 1.3427 | 1.6023 | 1.5939 | 0.8846 | 0.9872 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.3220 | 0.3657 | 0.3814 | 0.6018 | 0.5499 | 0.5498 | 1.0945 | 1.0943 | 0.8052 | 0.9351 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 77.0000 | 77.0000 | 1.0000 | 1.0000 | 0.1304 | 0.1458 | 0.1561 | 0.3788 | 0.3622 | 0.3603 | 1.0515 | 1.0457 | 0.8312 | 0.9481 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 78.0000 | 78.0000 | 1.0000 | 1.0000 | 4.0836 | 4.6500 | 5.3108 | 2.1514 | 1.3503 | 1.3432 | 1.6017 | 1.5932 | 0.8846 | 0.9872 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5618 | 0.5606 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3610 | 0.3617 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 78.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.2979 | 1.2899 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 77.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5618 | 0.5606 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0635 | 0.0671 | 14.7791 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0645 | 4.9266 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0261 | 0.0429 | 4.4601 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0635 | 0.0671 | 14.7893 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0610 | 0.0645 | 4.9266 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0261 | 0.0429 | 4.4601 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0648 | 13.9836 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0609 | 0.0645 | 4.9264 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0289 | 0.0476 | 4.4831 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0649 | 13.9691 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0609 | 0.0646 | 4.9264 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0289 | 0.0475 | 4.4831 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0604 | 0.0639 | 17.7445 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0576 | 0.0626 | 5.3700 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0573 | 0.0623 | 4.8160 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0604 | 0.0640 | 17.7252 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0576 | 0.0627 | 5.3701 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0573 | 0.0620 | 4.8160 |
| revision_dfm_kalman_em | exact | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0626 | 0.0661 | 16.7584 |
| revision_dfm_kalman_em | exact | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0623 | 0.0657 | 4.9305 |
| revision_dfm_kalman_em | exact | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0406 | 0.0519 | 4.4793 |
| revision_dfm_kalman_em | pseudo | pre_advance | 78.0000 | 1.0000 | 0.0000 | 0.0626 | 0.0661 | 16.7571 |
| revision_dfm_kalman_em | pseudo | pre_second | 78.0000 | 1.0000 | 0.0000 | 0.0623 | 0.0657 | 4.9305 |
| revision_dfm_kalman_em | pseudo | pre_third | 78.0000 | 1.0000 | 0.0000 | 0.0406 | 0.0519 | 4.4793 |
