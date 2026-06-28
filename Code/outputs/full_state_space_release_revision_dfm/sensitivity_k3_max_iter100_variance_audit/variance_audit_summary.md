# Variance Audit

Generated UTC: `2026-05-23T15:54:32+00:00`

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
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.5599 | 3.3291 | 3.7727 | 1.8020 | 4.4763 | 4.4538 | 0.4046 | 0.4026 | 0.5625 | 0.7875 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3563 | 0.3927 | 0.4098 | 0.6227 | 0.5976 | 0.5941 | 1.0481 | 1.0420 | 0.7595 | 0.8987 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1315 | 0.1434 | 0.4894 | 0.3833 | 0.3703 | 0.3711 | 1.0328 | 1.0350 | 0.8625 | 0.9375 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.5552 | 3.3297 | 3.7727 | 1.8020 | 4.5153 | 4.4926 | 0.4011 | 0.3991 | 0.5625 | 0.7875 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3563 | 0.3926 | 0.4099 | 0.6227 | 0.5972 | 0.5938 | 1.0487 | 1.0427 | 0.7595 | 0.8987 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1315 | 0.1434 | 0.4894 | 0.3833 | 0.3702 | 0.3710 | 1.0331 | 1.0352 | 0.8625 | 0.9375 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.6947 | 2.4821 | 3.3955 | 1.5735 | 4.6409 | 4.6124 | 0.3411 | 0.3390 | 0.6250 | 0.7500 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2893 | 0.3388 | 0.3709 | 0.5789 | 0.6458 | 0.6420 | 0.9018 | 0.8965 | 0.6582 | 0.8734 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1264 | 0.1389 | 0.4465 | 0.3759 | 0.3742 | 0.3738 | 1.0055 | 1.0045 | 0.8250 | 0.9250 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 1.6953 | 2.4795 | 3.3955 | 1.5735 | 4.7149 | 4.6858 | 0.3358 | 0.3337 | 0.6250 | 0.7625 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2893 | 0.3388 | 0.3709 | 0.5790 | 0.6444 | 0.6406 | 0.9038 | 0.8985 | 0.6582 | 0.8734 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1264 | 0.1389 | 0.4465 | 0.3759 | 0.3743 | 0.3739 | 1.0053 | 1.0043 | 0.8250 | 0.9250 | 1.0000 |
| midas_umidas | exact | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.3669 | 13.3665 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7814 | 4.7703 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2190 | 6.1905 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | A | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5464 | 13.5540 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | S | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 4.7770 | 4.7628 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | T | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 6.2140 | 6.1869 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.1471 | 2.9794 | 4.0238 | 1.7635 | 3.7935 | 3.7700 | 0.4678 | 0.4649 | 0.6375 | 0.8375 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3102 | 0.3425 | 0.3658 | 0.5861 | 0.6952 | 0.6979 | 0.8398 | 0.8431 | 0.7722 | 0.8608 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1174 | 0.1372 | 0.4396 | 0.3697 | 0.3774 | 0.3754 | 0.9848 | 0.9795 | 0.7625 | 0.9125 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | A | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 2.1443 | 2.9796 | 4.0238 | 1.7636 | 3.8264 | 3.8026 | 0.4638 | 0.4609 | 0.6375 | 0.8375 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | S | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3134 | 0.3428 | 0.3698 | 0.5864 | 0.6293 | 0.6305 | 0.9300 | 0.9318 | 0.7722 | 0.8608 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | T | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 0.1183 | 0.1373 | 0.4438 | 0.3699 | 0.3743 | 0.3722 | 0.9937 | 0.9882 | 0.7625 | 0.9125 | 1.0000 |
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
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4038 | 0.4415 | 0.4708 | 0.6642 | 0.5695 | 0.5697 | 1.1659 | 1.1664 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1334 | 0.1442 | 0.1609 | 0.3814 | 0.3595 | 0.3614 | 1.0555 | 1.0609 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1274 | 4.5429 | 5.1744 | 2.1322 | 1.3468 | 1.3387 | 1.5927 | 1.5832 | 0.8875 | 0.9750 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.4038 | 0.4415 | 0.4706 | 0.6642 | 0.5695 | 0.5697 | 1.1660 | 1.1664 | 0.8354 | 0.9367 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1334 | 0.1442 | 0.1609 | 0.3814 | 0.3595 | 0.3614 | 1.0555 | 1.0609 | 0.8481 | 0.9494 | 1.0000 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1274 | 4.5429 | 5.1744 | 2.1322 | 1.3468 | 1.3387 | 1.5927 | 1.5832 | 0.8875 | 0.9750 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2907 | 0.3418 | 0.3748 | 0.5824 | 0.6306 | 0.6290 | 0.9259 | 0.9236 | 0.6582 | 0.9114 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1286 | 0.1403 | 0.1552 | 0.3747 | 0.3588 | 0.3603 | 1.0400 | 1.0444 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1228 | 4.5559 | 5.1698 | 2.1336 | 1.3485 | 1.3404 | 1.5918 | 1.5822 | 0.8875 | 0.9875 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.2907 | 0.3419 | 0.3748 | 0.5824 | 0.6397 | 0.6385 | 0.9122 | 0.9106 | 0.6582 | 0.8987 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1286 | 0.1403 | 0.1552 | 0.3747 | 0.3588 | 0.3603 | 1.0400 | 1.0444 | 0.8481 | 0.9494 | 1.0000 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.1228 | 4.5559 | 5.1698 | 2.1336 | 1.3485 | 1.3404 | 1.5918 | 1.5822 | 0.8875 | 0.9875 | 1.0000 |
| midas_umidas | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.9201 | 13.8667 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1441 | 3.1241 |  |  |  |  | 0.0000 |
| midas_umidas | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.7339 | 7.8786 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 13.5309 | 13.4972 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 3.1442 | 3.1242 |  |  |  |  | 0.0000 |
| midas_umidas | pseudo | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 7.6901 | 7.8417 |  |  |  |  | 0.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3210 | 0.3496 | 0.3706 | 0.5917 | 0.5699 | 0.5680 | 1.0416 | 1.0382 | 0.7975 | 0.8861 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1189 | 0.1376 | 0.1484 | 0.3678 | 0.4499 | 0.4495 | 0.8181 | 0.8174 | 0.7722 | 0.8861 | 1.0000 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.0951 | 4.6481 | 5.3190 | 2.1529 | 1.3864 | 1.3777 | 1.5626 | 1.5529 | 0.8750 | 0.9875 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | DELTA_SA | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.3210 | 0.3496 | 0.3706 | 0.5918 | 0.5745 | 0.5724 | 1.0340 | 1.0302 | 0.7975 | 0.8861 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | DELTA_TS | 79.0000 | 79.0000 | 1.0000 | 1.0000 | 0.1189 | 0.1376 | 0.1484 | 0.3680 | 0.4095 | 0.4083 | 0.9013 | 0.8987 | 0.7722 | 0.8861 | 1.0000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | DELTA_MT | 80.0000 | 80.0000 | 1.0000 | 1.0000 | 4.0951 | 4.6483 | 5.3190 | 2.1528 | 1.3862 | 1.3776 | 1.5628 | 1.5530 | 0.8750 | 0.9875 | 1.0000 |
| no_revision | exact | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_second | DELTA_TS | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.3595 | 0.3614 |  |  |  |  | 0.0000 |
| no_revision | exact | pre_third | DELTA_MT | 80.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 1.3468 | 1.3387 |  |  |  |  | 0.0000 |
| no_revision | pseudo | pre_advance | DELTA_SA | 79.0000 | 0.0000 | 0.0000 | 0.0000 |  |  |  |  | 0.5695 | 0.5697 |  |  |  |  | 0.0000 |

## Covariance Matrix Audit

| model_id | timing_mode | checkpoint_id | n_matrices | share_psd | max_asymmetry | min_eigenvalue_min | min_eigenvalue_median | max_eigenvalue_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| indicator_revision_only_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0643 | 0.0676 | 13.2342 |
| indicator_revision_only_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0616 | 0.0649 | 4.9100 |
| indicator_revision_only_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0263 | 0.0447 | 4.4531 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0643 | 0.0676 | 13.2279 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0613 | 0.0650 | 4.9100 |
| indicator_revision_only_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0252 | 0.0447 | 4.4531 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0614 | 0.0649 | 12.9876 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0646 | 4.8351 |
| joint_indicator_revision_dfm_full_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0321 | 0.0484 | 4.4791 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0614 | 0.0649 | 12.9883 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0611 | 0.0646 | 4.8351 |
| joint_indicator_revision_dfm_full_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0312 | 0.0484 | 4.4791 |
| monthly_mixed_frequency_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0596 | 0.0626 | 14.7937 |
| monthly_mixed_frequency_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0576 | 0.0614 | 5.2748 |
| monthly_mixed_frequency_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0568 | 0.0602 | 4.8000 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0596 | 0.0626 | 14.8057 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0578 | 0.0614 | 5.2741 |
| monthly_mixed_frequency_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0569 | 0.0602 | 4.8001 |
| revision_dfm_kalman_em | exact | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0608 | 0.0651 | 14.7084 |
| revision_dfm_kalman_em | exact | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0605 | 0.0648 | 4.8831 |
| revision_dfm_kalman_em | exact | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0387 | 0.0455 | 4.4824 |
| revision_dfm_kalman_em | pseudo | pre_advance | 80.0000 | 1.0000 | 0.0000 | 0.0608 | 0.0651 | 14.7094 |
| revision_dfm_kalman_em | pseudo | pre_second | 80.0000 | 1.0000 | 0.0000 | 0.0605 | 0.0648 | 4.8831 |
| revision_dfm_kalman_em | pseudo | pre_third | 80.0000 | 1.0000 | 0.0000 | 0.0387 | 0.0453 | 4.4824 |
