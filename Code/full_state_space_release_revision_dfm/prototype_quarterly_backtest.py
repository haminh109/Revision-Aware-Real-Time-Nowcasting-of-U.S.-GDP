from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.data_adapter import (
    align_quarterly_model_panels,
    load_alfred_first_mature_monthly_panels,
    load_gdp_release_panel,
)
from full_state_space_release_revision_dfm.density import (
    gaussian_crps,
    gaussian_log_score,
    measurement_forecast_distribution,
)
from full_state_space_release_revision_dfm.joint_indicator_revision_dfm import (
    JointIndicatorRevisionDFMConfig,
    fit_joint_indicator_revision_dfm,
    forecast_gdp_releases,
)


MODEL_ID = "joint_indicator_revision_dfm_full_kalman_em"
TIMING_MODE = "prototype_quarterly"
RELEASE_ORDER = ("A", "S", "T", "M")
CHECKPOINT_TARGETS = {
    "pre_advance": "A",
    "pre_second": "S",
    "pre_third": "T",
}
REVISION_TARGETS = {
    "DELTA_SA": ("pre_advance", "S", "A"),
    "DELTA_TS": ("pre_second", "T", "S"),
    "DELTA_MT": ("pre_third", "M", "T"),
}
DEFAULT_SERIES = (
    "PAYEMS",
    "UNRATE",
    "INDPRO",
    "TCU",
    "W875RX1",
    "RSAFS",
    "HOUST",
    "PERMIT",
    "DGORDER",
    "BUSINV",
    "BOPGSTB",
    "GS10",
    "TB3MS",
)


@dataclass(slots=True)
class PrototypeBacktestConfig:
    repo_root: Path
    output_dir: Path
    series_ids: tuple[str, ...] = DEFAULT_SERIES
    min_train: int = 48
    max_origins: int = 24
    n_factors: int = 1
    max_iter: int = 8
    tolerance: float = 1e-4
    maturity_lag_quarters: int = 12
    start: str | None = "1992-01-01"
    end: str | None = None


def _load_aligned_panels(config: PrototypeBacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    first_monthly, mature_monthly = load_alfred_first_mature_monthly_panels(
        config.repo_root / "data/bronze/indicators/alfred_monthly_long.csv",
        series_ids=list(config.series_ids),
        start=config.start,
        end=config.end,
    )
    releases = load_gdp_release_panel(config.repo_root / "data/bronze/targets/gdp_release_targets.csv")
    first_q, mature_q, release_q = align_quarterly_model_panels(first_monthly, mature_monthly, releases)
    available = [
        col
        for col in config.series_ids
        if col in first_q.columns and col in mature_q.columns and first_q[col].notna().sum() >= config.min_train
    ]
    if len(available) < 2:
        raise ValueError("At least two sufficiently populated indicator series are required")
    return first_q.loc[:, available], mature_q.loc[:, available], release_q.loc[:, RELEASE_ORDER]


def _mask_release_panel_for_checkpoint(
    releases: pd.DataFrame,
    origin_pos: int,
    checkpoint_id: str,
    maturity_lag_quarters: int,
) -> pd.DataFrame:
    panel = releases.iloc[: origin_pos + 1].copy()
    positions = np.arange(len(panel))
    panel.loc[positions > origin_pos - maturity_lag_quarters, "M"] = np.nan
    current_idx = panel.index[-1]

    if checkpoint_id == "pre_advance":
        panel.loc[current_idx, ["A", "S", "T", "M"]] = np.nan
    elif checkpoint_id == "pre_second":
        panel.loc[current_idx, ["S", "T", "M"]] = np.nan
    elif checkpoint_id == "pre_third":
        panel.loc[current_idx, ["T", "M"]] = np.nan
    else:
        raise ValueError(f"Unknown checkpoint_id: {checkpoint_id}")
    return panel


def _mask_mature_indicators(
    mature_q: pd.DataFrame,
    origin_pos: int,
    maturity_lag_quarters: int,
) -> pd.DataFrame:
    panel = mature_q.iloc[: origin_pos + 1].copy()
    positions = np.arange(len(panel))
    panel.iloc[positions > origin_pos - maturity_lag_quarters, :] = np.nan
    return panel


def _release_distribution_row(result, target_id: str) -> tuple[float, float, float, float, float, float]:
    release_start = len(result.indicator_names) * (2 if result.has_mature_indicators else 1)
    row = release_start + RELEASE_ORDER.index(target_id)
    dist = measurement_forecast_distribution(
        result.params,
        result.smoother.smoothed_state[-1],
        result.smoother.smoothed_cov[-1],
        [row],
    )
    mean = float(dist.mean[0] * result.release_scale + result.release_mean)
    variance = float(dist.variance[0] * result.release_scale**2)
    lower = float(dist.lower[0] * result.release_scale + result.release_mean)
    upper = float(dist.upper[0] * result.release_scale + result.release_mean)
    return mean, variance, lower, upper, float(dist.mean[0]), float(dist.variance[0])


def _evaluation_positions(releases: pd.DataFrame, config: PrototypeBacktestConfig) -> list[int]:
    realized = releases.loc[:, ["A", "S", "T"]].dropna()
    eligible_labels = list(realized.index)
    eligible_positions = [releases.index.get_loc(label) for label in eligible_labels]
    eligible_positions = [pos for pos in eligible_positions if pos >= config.min_train]
    if config.max_origins and config.max_origins > 0:
        eligible_positions = eligible_positions[-config.max_origins :]
    return eligible_positions


def _point_metrics(forecasts: pd.DataFrame) -> pd.DataFrame:
    if forecasts.empty:
        return pd.DataFrame()

    def summarize(group: pd.DataFrame) -> pd.Series:
        error = group["forecast_value"] - group["realized_value"]
        sign_hit = np.sign(group["forecast_value"]) == np.sign(group["realized_value"])
        return pd.Series(
            {
                "n_forecasts": int(error.notna().sum()),
                "RMSE": float(np.sqrt(np.mean(error**2))),
                "MAE": float(np.mean(np.abs(error))),
                "bias": float(np.mean(error)),
                "relative_RMSFE": np.nan,
                "DM_test": np.nan,
                "sign_accuracy": float(sign_hit.mean()),
                "mean_log_score": float(group["log_score"].mean()),
                "mean_CRPS": float(group["crps"].mean()),
                "interval_95_coverage": float(group["interval_95_hit"].mean()),
                "convergence_rate": float(group["converged"].mean()),
                "mean_iterations": float(group["n_iter"].mean()),
            }
        )

    return (
        forecasts.groupby(["model_id", "timing_mode", "checkpoint_id", "target_id"], dropna=False)
        .apply(summarize, include_groups=False)
        .reset_index()
    )


def _revision_metrics(revisions: pd.DataFrame) -> pd.DataFrame:
    if revisions.empty:
        return pd.DataFrame()

    def summarize(group: pd.DataFrame) -> pd.Series:
        error = group["forecast_value"] - group["realized_value"]
        sign_hit = np.sign(group["forecast_value"]) == np.sign(group["realized_value"])
        return pd.Series(
            {
                "n_forecasts": int(error.notna().sum()),
                "RMSE": float(np.sqrt(np.mean(error**2))),
                "MAE": float(np.mean(np.abs(error))),
                "bias": float(np.mean(error)),
                "relative_RMSFE": np.nan,
                "DM_test": np.nan,
                "sign_accuracy": float(sign_hit.mean()),
                "convergence_rate": float(group["converged"].mean()),
                "mean_iterations": float(group["n_iter"].mean()),
            }
        )

    return (
        revisions.groupby(["model_id", "timing_mode", "checkpoint_id", "revision_target_id"], dropna=False)
        .apply(summarize, include_groups=False)
        .reset_index()
    )


def run_prototype_backtest(config: PrototypeBacktestConfig) -> dict[str, pd.DataFrame]:
    first_q, mature_q, releases = _load_aligned_panels(config)
    positions = _evaluation_positions(releases, config)
    point_rows: list[dict[str, object]] = []
    revision_rows: list[dict[str, object]] = []
    failure_rows: list[dict[str, object]] = []

    model_config = JointIndicatorRevisionDFMConfig(
        n_factors=config.n_factors,
        max_iter=config.max_iter,
        tolerance=config.tolerance,
        verbose=False,
    )

    for origin_pos in positions:
        target_quarter = str(releases.index[origin_pos])
        first_train = first_q.iloc[: origin_pos + 1]
        mature_train = _mask_mature_indicators(mature_q, origin_pos, config.maturity_lag_quarters)

        for checkpoint_id, target_id in CHECKPOINT_TARGETS.items():
            realized_value = float(releases.iloc[origin_pos][target_id])
            if not np.isfinite(realized_value):
                continue
            release_train = _mask_release_panel_for_checkpoint(
                releases,
                origin_pos,
                checkpoint_id,
                config.maturity_lag_quarters,
            )
            try:
                result = fit_joint_indicator_revision_dfm(
                    first_train,
                    release_train,
                    monthly_mature_panel=mature_train,
                    config=model_config,
                )
                forecasts = forecast_gdp_releases(result)
                forecast_value, variance, lower, upper, mean_std, variance_std = _release_distribution_row(result, target_id)
                log_score = float(
                    gaussian_log_score(
                        np.array([(realized_value - result.release_mean) / result.release_scale]),
                        np.array([mean_std]),
                        np.array([variance_std]),
                    )[0]
                )
                crps = float(
                    gaussian_crps(
                        np.array([(realized_value - result.release_mean) / result.release_scale]),
                        np.array([mean_std]),
                        np.array([variance_std]),
                    )[0]
                    * result.release_scale
                )
                point_rows.append(
                    {
                        "model_id": MODEL_ID,
                        "timing_mode": TIMING_MODE,
                        "forecast_origin": f"{target_quarter}:{checkpoint_id}",
                        "checkpoint_id": checkpoint_id,
                        "target_id": target_id,
                        "target_quarter": target_quarter,
                        "forecast_value": forecast_value,
                        "realized_value": realized_value,
                        "forecast_error": forecast_value - realized_value,
                        "forecast_variance": variance,
                        "interval_95_lower": lower,
                        "interval_95_upper": upper,
                        "interval_95_hit": lower <= realized_value <= upper,
                        "log_score": log_score,
                        "crps": crps,
                        "converged": result.converged,
                        "n_iter": result.n_iter,
                        "loglikelihood": result.loglikelihood_history[-1],
                        "n_indicators": len(result.indicator_names),
                        "revision_target_flag": False,
                    }
                )

                for revision_id, (revision_checkpoint, high_release, low_release) in REVISION_TARGETS.items():
                    if revision_checkpoint != checkpoint_id:
                        continue
                    high_realized = releases.iloc[origin_pos][high_release]
                    low_realized = releases.iloc[origin_pos][low_release]
                    if not np.isfinite(high_realized) or not np.isfinite(low_realized):
                        continue
                    revision_rows.append(
                        {
                            "model_id": MODEL_ID,
                            "timing_mode": TIMING_MODE,
                            "forecast_origin": f"{target_quarter}:{checkpoint_id}",
                            "checkpoint_id": checkpoint_id,
                            "target_id": revision_id,
                            "revision_target_id": revision_id,
                            "target_quarter": target_quarter,
                            "forecast_value": float(forecasts[high_release] - forecasts[low_release]),
                            "realized_value": float(high_realized - low_realized),
                            "forecast_error": float((forecasts[high_release] - forecasts[low_release]) - (high_realized - low_realized)),
                            "converged": result.converged,
                            "n_iter": result.n_iter,
                            "loglikelihood": result.loglikelihood_history[-1],
                            "n_indicators": len(result.indicator_names),
                            "revision_target_flag": True,
                        }
                    )
            except Exception as exc:  # pragma: no cover - operational logging path
                failure_rows.append(
                    {
                        "model_id": MODEL_ID,
                        "timing_mode": TIMING_MODE,
                        "forecast_origin": f"{target_quarter}:{checkpoint_id}",
                        "checkpoint_id": checkpoint_id,
                        "target_id": target_id,
                        "target_quarter": target_quarter,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )

    point_forecasts = pd.DataFrame(point_rows)
    revision_forecasts = pd.DataFrame(revision_rows)
    failures = pd.DataFrame(failure_rows)
    point_summary = _point_metrics(point_forecasts)
    revision_summary = _revision_metrics(revision_forecasts)
    return {
        "point_forecasts": point_forecasts,
        "revision_forecasts": revision_forecasts,
        "point_metrics": point_summary,
        "revision_metrics": revision_summary,
        "failures": failures,
    }


def write_outputs(outputs: dict[str, pd.DataFrame], config: PrototypeBacktestConfig) -> None:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    file_map = {
        "point_forecasts": "prototype_quarterly_forecasts.csv",
        "revision_forecasts": "prototype_quarterly_revision_forecasts.csv",
        "point_metrics": "prototype_quarterly_point_metrics.csv",
        "revision_metrics": "prototype_quarterly_revision_metrics.csv",
        "failures": "prototype_quarterly_failures.csv",
    }
    for key, filename in file_map.items():
        outputs[key].to_csv(config.output_dir / filename, index=False)
    _write_summary(outputs, config)


def _write_summary(outputs: dict[str, pd.DataFrame], config: PrototypeBacktestConfig) -> None:
    lines = [
        "# Prototype Quarterly Full State-Space Backtest",
        "",
        "This is an operational smoke/backtest layer for the full Kalman/EM joint indicator-revision DFM.",
        "It is not the final exact/pseudo event-time journal evaluation.",
        "",
        "## Configuration",
        "",
        f"- model_id: `{MODEL_ID}`",
        f"- timing_mode: `{TIMING_MODE}`",
        f"- min_train: `{config.min_train}` quarters",
        f"- max_origins: `{config.max_origins}` (`0` means all eligible origins)",
        f"- n_factors: `{config.n_factors}`",
        f"- max_iter: `{config.max_iter}`",
        f"- maturity_lag_quarters: `{config.maturity_lag_quarters}`",
        f"- indicators: `{', '.join(config.series_ids)}`",
        "",
        "## Leakage Controls In This Prototype",
        "",
        "- Current-quarter target releases are masked according to `pre_advance`, `pre_second`, and `pre_third` checkpoint rules.",
        "- Mature GDP release values are masked for the current and previous maturity-lag quarters.",
        "- Mature indicator values are masked for the current and previous maturity-lag quarters.",
        "- Monthly indicators are quarterly averaged; this is still coarser than the exact event-time snapshot builder.",
        "",
        "## Output Files",
        "",
        "- `prototype_quarterly_forecasts.csv`",
        "- `prototype_quarterly_revision_forecasts.csv`",
        "- `prototype_quarterly_point_metrics.csv`",
        "- `prototype_quarterly_revision_metrics.csv`",
        "- `prototype_quarterly_failures.csv`",
        "",
    ]
    for key, title in [("point_metrics", "Point Metrics"), ("revision_metrics", "Revision Metrics")]:
        frame = outputs[key]
        lines.extend([f"## {title}", ""])
        if frame.empty:
            lines.append("No rows were produced.")
        else:
            lines.append(frame.round(4).to_string(index=False))
        lines.append("")
    failures = outputs["failures"]
    lines.extend(["## Failures", ""])
    lines.append("No failures." if failures.empty else failures.to_string(index=False))
    lines.append("")
    lines.extend(
        [
            "## Interpretation Rule",
            "",
            "Use these CSVs to validate that the full state-space model is operational.",
            "Do not replace the frozen submission results until this model is wired into the exact/pseudo real-time snapshot pipeline and compared against the existing AR, bridge, standard DFM, release DFM, and revision DFM benchmarks.",
            "",
        ]
    )
    (config.output_dir / "prototype_quarterly_run_summary.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Defaults to <repo-root>/outputs/full_state_space_release_revision_dfm.",
    )
    parser.add_argument("--series", nargs="*", default=list(DEFAULT_SERIES))
    parser.add_argument("--min-train", type=int, default=48)
    parser.add_argument("--max-origins", type=int, default=24, help="Use most recent N origins; pass 0 for all.")
    parser.add_argument("--n-factors", type=int, default=1)
    parser.add_argument("--max-iter", type=int, default=8)
    parser.add_argument("--tolerance", type=float, default=1e-4)
    parser.add_argument("--maturity-lag-quarters", type=int, default=12)
    parser.add_argument("--start", default="1992-01-01")
    parser.add_argument("--end", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir or args.repo_root / "outputs/full_state_space_release_revision_dfm"
    config = PrototypeBacktestConfig(
        repo_root=args.repo_root,
        output_dir=output_dir,
        series_ids=tuple(args.series),
        min_train=args.min_train,
        max_origins=args.max_origins,
        n_factors=args.n_factors,
        max_iter=args.max_iter,
        tolerance=args.tolerance,
        maturity_lag_quarters=args.maturity_lag_quarters,
        start=args.start,
        end=args.end,
    )
    outputs = run_prototype_backtest(config)
    write_outputs(outputs, config)
    print(f"Wrote prototype outputs to {config.output_dir}")
    print(f"Point forecasts: {len(outputs['point_forecasts'])}")
    print(f"Revision forecasts: {len(outputs['revision_forecasts'])}")
    print(f"Failures: {len(outputs['failures'])}")


if __name__ == "__main__":
    main()
