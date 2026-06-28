from __future__ import annotations

import math

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm import (
    JointIndicatorRevisionDFMConfig,
    ReleaseRevisionDFMConfig,
    fit_joint_indicator_revision_dfm,
    fit_release_revision_dfm,
    forecast_gdp_releases,
    forecast_indicator_revisions,
    forecast_release_row,
    gaussian_crps,
    gaussian_log_score,
    measurement_forecast_distribution,
)
from full_state_space_release_revision_dfm.example_synthetic import make_synthetic_data
from full_state_space_release_revision_dfm.mixed_frequency_release_kalman import (
    MixedFrequencyReleaseKalmanConfig,
    forecast_mixed_frequency_release_kalman,
)
from full_state_space_release_revision_dfm.q2_benchmarks import forecast_midas_umidas, forecast_no_revision


def _assert_finite_mapping(values: dict[str, float]) -> None:
    for key, value in values.items():
        if not math.isfinite(value):
            raise AssertionError(f"{key} is not finite: {value}")


def main() -> None:
    first, mature, releases = make_synthetic_data(n_obs=72, n_monthly=6, seed=123)

    release_result = fit_release_revision_dfm(
        first,
        releases,
        config=ReleaseRevisionDFMConfig(max_iter=15, tolerance=1e-4),
    )
    if len(release_result.loglikelihood_history) < 2:
        raise AssertionError("release/revision EM did not run multiple likelihood evaluations")
    _assert_finite_mapping(forecast_release_row(release_result))

    joint_result = fit_joint_indicator_revision_dfm(
        first,
        releases,
        monthly_mature_panel=mature,
        config=JointIndicatorRevisionDFMConfig(max_iter=15, tolerance=1e-4),
    )
    if len(joint_result.loglikelihood_history) < 2:
        raise AssertionError("joint indicator-revision EM did not run multiple likelihood evaluations")
    _assert_finite_mapping(forecast_gdp_releases(joint_result))
    revision_frame = forecast_indicator_revisions(joint_result)
    if revision_frame["revision_forecast"].isna().any():
        raise AssertionError("indicator revision forecasts contain NaN")

    state = joint_result.smoother.smoothed_state[-1]
    cov = joint_result.smoother.smoothed_cov[-1]
    release_start = len(joint_result.indicator_names) * 2
    density = measurement_forecast_distribution(
        joint_result.params,
        state,
        cov,
        rows=np.arange(release_start, release_start + 4),
    )
    if not np.isfinite(density.mean).all() or not np.isfinite(density.variance).all():
        raise AssertionError("density forecast contains non-finite values")
    scores = gaussian_log_score(density.mean, density.mean, density.variance)
    crps = gaussian_crps(density.mean, density.mean, density.variance)
    if not np.isfinite(scores).all() or not np.isfinite(crps).all():
        raise AssertionError("density scores contain non-finite values")

    quarters = pd.Index([f"{year}:Q{quarter}" for year in range(2010, 2018) for quarter in range(1, 5)])
    release_train = pd.DataFrame(
        {
            "A": np.linspace(1.0, 3.0, len(quarters)),
            "S": np.linspace(1.1, 3.1, len(quarters)),
            "T": np.linspace(1.15, 3.15, len(quarters)),
            "M": np.linspace(1.2, 3.2, len(quarters)),
        },
        index=quarters,
    )
    release_train.loc[quarters[-1], ["S", "T", "M"]] = np.nan
    no_revision = forecast_no_revision(release_train, "pre_second")
    if not math.isclose(no_revision["S"], no_revision["A"]):
        raise AssertionError("no_revision benchmark did not set S-A revision to zero")
    monthly_index = pd.date_range("2009-01-01", periods=120, freq="MS")
    monthly_panel = pd.DataFrame(
        {
            "PAYEMS": 100.0 + np.arange(len(monthly_index)) * 0.2,
            "UNRATE": 5.0 + np.sin(np.arange(len(monthly_index)) / 6.0),
        },
        index=monthly_index,
    )
    midas_forecasts = forecast_midas_umidas(monthly_panel, release_train, "pre_second", n_lags=3)
    _assert_finite_mapping(midas_forecasts)
    mf_forecasts, mf_variances, mf_covariance, mf_diagnostics = forecast_mixed_frequency_release_kalman(
        monthly_panel,
        release_train,
        str(quarters[-1]),
        MixedFrequencyReleaseKalmanConfig(max_iter=2),
    )
    _assert_finite_mapping(mf_forecasts)
    _assert_finite_mapping(mf_variances)
    if mf_covariance.shape != (4, 4) or not np.isfinite(mf_covariance.to_numpy(dtype=float)).all():
        raise AssertionError("mixed-frequency Kalman covariance is invalid")
    if int(mf_diagnostics["n_iter"]) < 1:
        raise AssertionError("mixed-frequency Kalman diagnostics did not record EM iterations")

    print("full_state_space_release_revision_dfm smoke tests passed")
    print("release/revision llf tail:", [round(x, 3) for x in release_result.loglikelihood_history[-3:]])
    print("joint indicator-revision llf tail:", [round(x, 3) for x in joint_result.loglikelihood_history[-3:]])


if __name__ == "__main__":
    main()
