from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.build_journal_evidence_package import (
    _bootstrap_loss_difference_ci,
    _common_sample_metrics,
    _revision_threshold_diagnostics,
)
from full_state_space_release_revision_dfm.exact_pseudo_backtest import (
    ExactPseudoBacktestConfig,
    _apply_estimation_window,
    _evaluation_positions,
)
from full_state_space_release_revision_dfm.mixed_frequency_release_kalman import (
    MixedFrequencyReleaseKalmanConfig,
    forecast_mixed_frequency_release_kalman,
)
from full_state_space_release_revision_dfm.q2_benchmarks import normalize_spf_rgdp_growth


def _release_train(n_quarters: int = 64) -> pd.DataFrame:
    quarters = pd.Index([f"{year}:Q{quarter}" for year in range(2000, 2025) for quarter in range(1, 5)])
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


def test_public_spf_normalizer_maps_horizons_to_release_targets() -> None:
    raw = pd.DataFrame({"YEAR": [2024], "QUARTER": [1], "DRGDP2": [2.5], "DRGDP3": [2.0]})
    normalized = normalize_spf_rgdp_growth(raw)
    assert {"forecast_origin_date", "target_quarter", "target_id", "forecast_value"}.issubset(normalized.columns)
    assert set(normalized["target_id"]) == {"A", "S", "T", "M"}
    assert set(normalized.loc[normalized["spf_column"].eq("DRGDP2"), "target_quarter"]) == {"2024:Q1"}
    assert set(normalized.loc[normalized["spf_column"].eq("DRGDP3"), "target_quarter"]) == {"2024:Q2"}
    assert normalized["forecast_origin_date"].max() <= pd.Timestamp("2024-02-29")


def test_evaluation_positions_can_exclude_covid_quarters() -> None:
    releases = _release_train(90)
    config = ExactPseudoBacktestConfig(
        repo_root=Path("."),
        output_dir=Path("."),
        eval_start="2020:Q1",
        eval_end="2020:Q4",
        min_train=1,
        max_origins=0,
        exclude_quarters=("2020:Q2", "2020:Q3"),
    )
    labels = [str(releases.index[pos]) for pos in _evaluation_positions(releases, config)]
    assert "2020:Q2" not in labels
    assert "2020:Q3" not in labels


def test_rolling_window_trims_release_history() -> None:
    releases = _release_train(64)
    dates = pd.date_range("1999-01-01", periods=240, freq="MS")
    monthly = pd.DataFrame({"PAYEMS": np.arange(len(dates), dtype=float) + 100.0}, index=dates)
    q_features = pd.DataFrame({"PAYEMS": np.arange(len(releases), dtype=float)}, index=releases.index)
    config = ExactPseudoBacktestConfig(
        repo_root=Path("."),
        output_dir=Path("."),
        min_train=12,
        estimation_window="rolling",
        rolling_window_quarters=12,
    )
    windowed, first_q, current_q, monthly_window, start = _apply_estimation_window(releases, q_features, q_features, monthly, config)
    assert len(windowed) == 13
    assert start == str(windowed.index[0])
    assert first_q.index.equals(windowed.index)
    assert current_q.index.equals(windowed.index)
    assert monthly_window.index.min() >= pd.Timestamp("2010-01-01")


def test_mixed_frequency_guards_report_finite_degenerate_run() -> None:
    releases = _release_train(32)
    target_quarter = str(releases.index[-1])
    releases.loc[target_quarter, ["S", "T", "M"]] = np.nan
    monthly_index = pd.date_range("2000-01-01", periods=120, freq="MS")
    monthly = pd.DataFrame(
        {
            "PAYEMS": np.ones(len(monthly_index)) * 100.0,
            "INDPRO": np.ones(len(monthly_index)) * 90.0,
            "UNRATE": np.ones(len(monthly_index)) * 5.0,
        },
        index=monthly_index,
    )
    forecasts, variances, covariance, diagnostics = forecast_mixed_frequency_release_kalman(
        monthly,
        releases,
        target_quarter,
        MixedFrequencyReleaseKalmanConfig(max_iter=2),
    )
    assert np.isfinite(list(forecasts.values())).all()
    assert np.isfinite(list(variances.values())).all()
    assert np.isfinite(covariance.to_numpy(dtype=float)).all()
    assert "numerical_guard_event_count" in diagnostics


def test_revision_threshold_diagnostics_and_common_sample() -> None:
    frame = pd.DataFrame(
        {
            "model_id": ["no_revision", "candidate", "spf", "no_revision", "candidate", "spf"] * 2,
            "timing_mode": ["exact"] * 12,
            "checkpoint_id": ["pre_second"] * 12,
            "revision_target_id": ["DELTA_TS"] * 12,
            "target_id": ["DELTA_TS"] * 12,
            "target_quarter": ["2020:Q1"] * 6 + ["2020:Q2"] * 6,
            "forecast_origin": ["o1"] * 6 + ["o2"] * 6,
            "forecast_origin_date": ["2020-01-01"] * 6 + ["2020-04-01"] * 6,
            "forecast_value": [0.0, 0.3, 0.0, 0.0, -0.4, 0.0] * 2,
            "realized_value": [0.25, 0.25, 0.25, -0.35, -0.35, -0.35] * 2,
        }
    )
    frame["forecast_error"] = frame["forecast_value"] - frame["realized_value"]
    diagnostics = _revision_threshold_diagnostics(frame)
    assert {0.0, 0.1, 0.2}.issubset(set(diagnostics["threshold_abs_revision"]))
    point_frame = frame.drop(columns=["revision_target_id"]).copy()
    point_frame["target_id"] = "S"
    common = _common_sample_metrics(point_frame, "target_id", anchor_model="spf")
    assert not common.empty


def test_bootstrap_loss_difference_ci_schema() -> None:
    quarters = [f"2020:Q{q}" for q in [1, 2, 3, 4]] + [f"2021:Q{q}" for q in [1, 2, 3, 4]]
    rows = []
    for quarter in quarters:
        rows.append(
            {
                "model_id": "ar",
                "timing_mode": "exact",
                "checkpoint_id": "pre_advance",
                "target_id": "A",
                "target_quarter": quarter,
                "forecast_origin": quarter,
                "forecast_value": 0.0,
                "realized_value": 1.0,
                "forecast_error": -1.0,
            }
        )
        rows.append(
            {
                "model_id": "candidate",
                "timing_mode": "exact",
                "checkpoint_id": "pre_advance",
                "target_id": "A",
                "target_quarter": quarter,
                "forecast_origin": quarter,
                "forecast_value": 0.8,
                "realized_value": 1.0,
                "forecast_error": -0.2,
            }
        )
    result = _bootstrap_loss_difference_ci(pd.DataFrame(rows), "target_id", bootstrap_reps=25)
    assert {"rmse_diff_ci_low_95", "mae_diff_ci_high_95", "positive_diff_interpretation"}.issubset(result.columns)
