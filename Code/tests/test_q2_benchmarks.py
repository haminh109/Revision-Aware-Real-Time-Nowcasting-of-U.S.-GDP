from __future__ import annotations

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.mixed_frequency_release_kalman import (
    MixedFrequencyReleaseKalmanConfig,
    forecast_mixed_frequency_release_kalman,
)
from full_state_space_release_revision_dfm.q2_benchmarks import (
    SPFBenchmark,
    forecast_midas_umidas,
    forecast_no_revision,
)


def _release_train(n_quarters: int = 32) -> pd.DataFrame:
    quarters = pd.Index([f"{year}:Q{quarter}" for year in range(2010, 2018) for quarter in range(1, 5)])
    quarters = quarters[:n_quarters]
    values = np.linspace(1.0, 3.0, len(quarters))
    return pd.DataFrame(
        {
            "A": values,
            "S": values + 0.10,
            "T": values + 0.15,
            "M": values + 0.20,
        },
        index=quarters,
    )


def test_no_revision_sets_second_release_revision_to_zero() -> None:
    releases = _release_train()
    releases.loc[releases.index[-1], ["S", "T", "M"]] = np.nan
    forecasts = forecast_no_revision(releases, "pre_second")
    assert forecasts["S"] == forecasts["A"]
    assert forecasts["T"] == forecasts["A"]


def test_midas_umidas_returns_finite_release_forecasts() -> None:
    releases = _release_train()
    releases.loc[releases.index[-1], ["T", "M"]] = np.nan
    monthly_index = pd.date_range("2009-01-01", periods=120, freq="MS")
    monthly = pd.DataFrame(
        {
            "PAYEMS": 100.0 + np.arange(len(monthly_index)) * 0.1,
            "UNRATE": 5.0 + np.sin(np.arange(len(monthly_index)) / 5.0),
            "INDPRO": 90.0 + np.arange(len(monthly_index)) * 0.2,
        },
        index=monthly_index,
    )
    forecasts = forecast_midas_umidas(monthly, releases, "pre_third", n_lags=4)
    assert set(forecasts) == {"A", "S", "T", "M"}
    assert np.isfinite(list(forecasts.values())).all()


def test_spf_alignment_uses_latest_available_forecast(tmp_path) -> None:
    path = tmp_path / "spf.csv"
    pd.DataFrame(
        [
            {
                "forecast_origin_date": "2024-01-01",
                "target_quarter": "2024:Q1",
                "target_id": "A",
                "forecast_value": 1.0,
            },
            {
                "forecast_origin_date": "2024-02-01",
                "target_quarter": "2024:Q1",
                "target_id": "A",
                "forecast_value": 2.0,
            },
        ]
    ).to_csv(path, index=False)
    benchmark = SPFBenchmark.from_csv(path)
    assert benchmark is not None
    forecasts = benchmark.forecast_for_origin("2024:Q1", pd.Timestamp("2024-02-15"))
    assert forecasts == {"A": 2.0}


def test_monthly_mixed_frequency_kalman_returns_release_density() -> None:
    releases = _release_train()
    target_quarter = str(releases.index[-1])
    releases.loc[target_quarter, ["S", "T", "M"]] = np.nan
    monthly_index = pd.date_range("2009-01-01", periods=120, freq="MS")
    monthly = pd.DataFrame(
        {
            "PAYEMS": 100.0 + np.arange(len(monthly_index)) * 0.1,
            "UNRATE": 5.0 + np.sin(np.arange(len(monthly_index)) / 5.0),
            "INDPRO": 90.0 + np.arange(len(monthly_index)) * 0.2,
        },
        index=monthly_index,
    )
    forecasts, variances, covariance, diagnostics = forecast_mixed_frequency_release_kalman(
        monthly,
        releases,
        target_quarter,
        MixedFrequencyReleaseKalmanConfig(max_iter=2),
    )
    assert set(forecasts) == {"A", "S", "T", "M"}
    assert np.isfinite(list(forecasts.values())).all()
    assert np.isfinite(list(variances.values())).all()
    assert covariance.shape == (4, 4)
    assert np.isfinite(covariance.to_numpy(dtype=float)).all()
    assert diagnostics["n_iter"] >= 1
