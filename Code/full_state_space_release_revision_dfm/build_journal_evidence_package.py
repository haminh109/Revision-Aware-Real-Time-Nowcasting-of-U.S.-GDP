from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from math import erfc, pi, sqrt
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.density import gaussian_crps, gaussian_log_score


DEFAULT_SOURCE_DIR = Path("outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest_max_iter10")
DEFAULT_OUTPUT_DIR = Path("outputs/full_state_space_release_revision_dfm/journal_evidence_package")
BASELINE_MODELS = ("ar", "no_revision", "bridge", "midas_umidas", "spf", "standard_dfm", "release_dfm")
STRUCTURED_MODELS = ("release_dfm", "revision_dfm_kalman_em", "joint_indicator_revision_dfm_full_kalman_em")
KEY_COLUMNS = ("timing_mode", "checkpoint_id")
MIN_VARIANCE = 1e-8
DEFAULT_MCS_BOOTSTRAP_REPS = 499
DEFAULT_RANDOM_SEED = 20260425


def _quarter_to_period(label: str) -> pd.Period:
    year, quarter = str(label).split(":Q")
    return pd.Period(year=int(year), quarter=int(quarter), freq="Q")


def _period_to_label(period: pd.Period) -> str:
    return f"{period.year}:Q{period.quarter}"


def _normal_sf(value: float) -> float:
    if not np.isfinite(value):
        return float("nan")
    return 0.5 * erfc(value / sqrt(2.0))


def _normal_cdf(value: float) -> float:
    if not np.isfinite(value):
        return float("nan")
    return 0.5 * erfc(-value / sqrt(2.0))


def _normal_two_sided(value: float) -> float:
    if not np.isfinite(value):
        return float("nan")
    return erfc(abs(value) / sqrt(2.0))


def _hac_standard_error(values: np.ndarray) -> tuple[float, int]:
    clean = np.asarray(values, dtype=float)
    clean = clean[np.isfinite(clean)]
    n_obs = clean.size
    if n_obs < 5:
        return float("nan"), 0
    centered = clean - clean.mean()
    lag = max(1, int(np.floor(n_obs ** (1.0 / 3.0))))
    gamma0 = float(np.dot(centered, centered) / n_obs)
    long_run_var = gamma0
    for ell in range(1, lag + 1):
        cov = float(np.dot(centered[ell:], centered[:-ell]) / n_obs)
        weight = 1.0 - ell / (lag + 1.0)
        long_run_var += 2.0 * weight * cov
    if long_run_var <= 0.0 or not np.isfinite(long_run_var):
        return float("nan"), lag
    return sqrt(long_run_var / n_obs), lag


def _comparison_stat(loss_diff: pd.Series) -> dict[str, float]:
    clean = pd.to_numeric(loss_diff, errors="coerce").dropna().to_numpy(dtype=float)
    n_obs = clean.size
    se, lag = _hac_standard_error(clean)
    mean_diff = float(clean.mean()) if n_obs else float("nan")
    stat = mean_diff / se if np.isfinite(se) and se > 0 else float("nan")
    return {
        "n_obs": int(n_obs),
        "mean_loss_diff": mean_diff,
        "hac_lag": int(lag),
        "test_stat": stat,
        "p_value_two_sided": _normal_two_sided(stat),
        "p_value_model_better_one_sided": _normal_sf(stat),
    }


def _load_backtest_outputs(source_dir: Path) -> dict[str, pd.DataFrame]:
    source_dir = source_dir.resolve()
    required = {
        "point": "forecast_results.csv",
        "revision": "revision_forecast_results.csv",
        "point_metrics": "metrics_summary.csv",
        "revision_metrics": "revision_metrics_summary.csv",
        "calendar": "gdp_release_calendar_used.csv",
    }
    outputs: dict[str, pd.DataFrame] = {}
    for name, filename in required.items():
        path = source_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")
        parse_dates = ["forecast_origin_date"] if name in {"point", "revision"} else None
        outputs[name] = pd.read_csv(path, parse_dates=parse_dates)
    return outputs


def _prepare_forecast_frame(frame: pd.DataFrame, outcome_col: str) -> pd.DataFrame:
    work = frame.copy()
    work["target_period"] = work["target_quarter"].map(_quarter_to_period)
    work["squared_error"] = pd.to_numeric(work["forecast_error"], errors="coerce") ** 2
    work["absolute_error"] = pd.to_numeric(work["forecast_error"], errors="coerce").abs()
    work["outcome_id"] = work[outcome_col].astype(str)
    return work


def _pairwise_forecast_tests(frame: pd.DataFrame, outcome_col: str) -> pd.DataFrame:
    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[dict[str, object]] = []
    group_cols = [*KEY_COLUMNS, "outcome_id"]
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        models = sorted(group["model_id"].astype(str).unique())
        for baseline in BASELINE_MODELS:
            baseline_rows = group.loc[group["model_id"].eq(baseline)]
            if baseline_rows.empty:
                continue
            baseline_aligned = baseline_rows.set_index("target_quarter")
            for model in models:
                if model == baseline:
                    continue
                model_rows = group.loc[group["model_id"].eq(model)].set_index("target_quarter")
                joined = model_rows[["forecast_error", "forecast_value", "realized_value"]].join(
                    baseline_aligned[["forecast_error", "forecast_value", "realized_value"]],
                    how="inner",
                    lsuffix="_model",
                    rsuffix="_baseline",
                )
                if joined.empty:
                    continue
                model_loss = joined["forecast_error_model"] ** 2
                baseline_loss = joined["forecast_error_baseline"] ** 2
                loss_diff = baseline_loss - model_loss
                stats = _comparison_stat(loss_diff)
                rows.append(
                    {
                        **key_values,
                        "baseline_model": baseline,
                        "model_id": model,
                        "test_type": "DM_HAC_squared_error",
                        "positive_mean_interpretation": "model_has_lower_squared_error_than_baseline",
                        **stats,
                    }
                )
    return pd.DataFrame(rows)


def _clark_west_tests(frame: pd.DataFrame, outcome_col: str) -> pd.DataFrame:
    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[dict[str, object]] = []
    group_cols = [*KEY_COLUMNS, "outcome_id"]
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        models = sorted(group["model_id"].astype(str).unique())
        for baseline in ("standard_dfm", "release_dfm"):
            baseline_rows = group.loc[group["model_id"].eq(baseline)]
            if baseline_rows.empty:
                continue
            baseline_aligned = baseline_rows.set_index("target_quarter")
            for model in models:
                if model == baseline or model not in STRUCTURED_MODELS:
                    continue
                model_rows = group.loc[group["model_id"].eq(model)].set_index("target_quarter")
                joined = model_rows[["forecast_error", "forecast_value"]].join(
                    baseline_aligned[["forecast_error", "forecast_value"]],
                    how="inner",
                    lsuffix="_model",
                    rsuffix="_baseline",
                )
                if joined.empty:
                    continue
                adjusted_loss_diff = (
                    joined["forecast_error_baseline"] ** 2
                    - (joined["forecast_error_model"] ** 2 - (joined["forecast_value_baseline"] - joined["forecast_value_model"]) ** 2)
                )
                stats = _comparison_stat(adjusted_loss_diff)
                rows.append(
                    {
                        **key_values,
                        "baseline_model": baseline,
                        "model_id": model,
                        "test_type": "Clark_West_style_HAC",
                        "positive_mean_interpretation": "larger_model_improves_over_baseline_after_adjustment",
                        **stats,
                    }
                )
    return pd.DataFrame(rows)


def _mcs_proxy(frame: pd.DataFrame, outcome_col: str, alpha: float = 0.10) -> pd.DataFrame:
    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[dict[str, object]] = []
    group_cols = [*KEY_COLUMNS, "outcome_id"]
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        mean_losses = group.groupby("model_id")["squared_error"].mean().sort_values()
        if mean_losses.empty:
            continue
        best_model = str(mean_losses.index[0])
        best_rows = group.loc[group["model_id"].eq(best_model)].set_index("target_quarter")
        for model, mean_loss in mean_losses.items():
            model_rows = group.loc[group["model_id"].eq(model)].set_index("target_quarter")
            joined = model_rows[["squared_error"]].join(best_rows[["squared_error"]], how="inner", lsuffix="_model", rsuffix="_best")
            if str(model) == best_model:
                p_not_worse = 1.0
                stat = 0.0
                n_obs = int(len(joined))
            else:
                diff = joined["squared_error_model"] - joined["squared_error_best"]
                stats = _comparison_stat(diff)
                stat = stats["test_stat"]
                n_obs = stats["n_obs"]
                p_not_worse = _normal_sf(stat)
            rows.append(
                {
                    **key_values,
                    "model_id": model,
                    "best_model": best_model,
                    "mean_squared_error": float(mean_loss),
                    "n_obs": n_obs,
                    "test_stat_vs_best": stat,
                    "p_value_worse_than_best_one_sided": p_not_worse,
                    "included_in_proxy_set_alpha_10pct": bool(p_not_worse >= alpha),
                    "method_note": "Proxy only; not a formal Hansen-Lunde-Nason MCS implementation.",
                }
            )
    return pd.DataFrame(rows)


def _moving_block_sample_indices(n_obs: int, block_length: int, rng: np.random.Generator) -> np.ndarray:
    if n_obs <= 0:
        return np.array([], dtype=int)
    block_length = max(1, min(int(block_length), n_obs))
    starts = rng.integers(0, n_obs, size=int(np.ceil(n_obs / block_length)))
    indices: list[int] = []
    for start in starts:
        block = (np.arange(block_length, dtype=int) + int(start)) % n_obs
        indices.extend(block.tolist())
        if len(indices) >= n_obs:
            break
    return np.asarray(indices[:n_obs], dtype=int)


def _average_loss_differentials(losses: pd.DataFrame) -> pd.DataFrame:
    values = losses.to_numpy(dtype=float)
    if values.shape[1] <= 1:
        return pd.DataFrame(index=losses.index)
    out = {}
    columns = list(losses.columns)
    for idx, model in enumerate(columns):
        others = np.delete(values, idx, axis=1)
        out[model] = values[:, idx] - others.mean(axis=1)
    return pd.DataFrame(out, index=losses.index)


def _mcs_block_bootstrap(
    frame: pd.DataFrame,
    outcome_col: str,
    alpha: float = 0.10,
    bootstrap_reps: int = DEFAULT_MCS_BOOTSTRAP_REPS,
    block_length: int | None = None,
    seed: int = DEFAULT_RANDOM_SEED,
) -> pd.DataFrame:
    """Build an MCS-style block-bootstrap confidence set by checkpoint.

    The routine follows the MCS elimination idea: test equal predictive
    ability within a model set, then remove the model with the largest mean
    loss when the bootstrap p-value rejects. It is intentionally transparent
    and deterministic so reviewers can audit it from the output CSV.
    """

    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[dict[str, object]] = []
    group_cols = [*KEY_COLUMNS, "outcome_id"]
    rng = np.random.default_rng(seed)
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        losses = group.pivot_table(index="target_quarter", columns="model_id", values="squared_error", aggfunc="mean")
        if losses.empty:
            continue
        period_index = pd.Series(losses.index, index=losses.index).map(_quarter_to_period)
        losses = losses.loc[period_index.sort_values().index]
        losses = losses.dropna(axis=1, how="all")
        initial_models = [str(col) for col in losses.columns]
        current = initial_models.copy()
        removed: dict[str, dict[str, object]] = {}
        step = 0
        last_p_value = float("nan")
        last_test_stat = float("nan")
        while len(current) > 1:
            current_losses = losses.loc[:, current].dropna()
            n_obs = len(current_losses)
            if n_obs < 8:
                break
            differentials = _average_loss_differentials(current_losses)
            means = differentials.mean(axis=0)
            centered = differentials - means
            eff_block = block_length or max(1, int(np.floor(n_obs ** (1.0 / 3.0))))
            boot_means = np.zeros((bootstrap_reps, len(current)), dtype=float)
            centered_values = centered.to_numpy(dtype=float)
            for b in range(bootstrap_reps):
                idx = _moving_block_sample_indices(n_obs, eff_block, rng)
                boot_means[b] = centered_values[idx].mean(axis=0)
            boot_se = boot_means.std(axis=0, ddof=1)
            boot_se = np.where(boot_se > MIN_VARIANCE, boot_se, np.nan)
            observed_t = means.to_numpy(dtype=float) / boot_se
            if not np.isfinite(observed_t).any():
                break
            test_stat = float(np.nanmax(observed_t))
            boot_stats = np.nanmax(boot_means / boot_se, axis=1)
            boot_stats = boot_stats[np.isfinite(boot_stats)]
            if boot_stats.size == 0:
                break
            p_value = float((1.0 + np.sum(boot_stats >= test_stat)) / (boot_stats.size + 1.0))
            last_p_value = p_value
            last_test_stat = test_stat
            if p_value >= alpha:
                break
            worst_model = str(current_losses.mean(axis=0).idxmax())
            step += 1
            removed[worst_model] = {
                "elimination_step": step,
                "p_value_at_elimination": p_value,
                "test_stat_at_elimination": test_stat,
                "n_obs_at_elimination": int(n_obs),
            }
            current.remove(worst_model)
        mean_losses = losses.mean(axis=0, skipna=True)
        final_losses = losses.loc[:, current].dropna() if current else pd.DataFrame()
        final_n = int(len(final_losses)) if not final_losses.empty else 0
        eff_block_final = block_length or max(1, int(np.floor(max(final_n, 1) ** (1.0 / 3.0))))
        for model in initial_models:
            removal = removed.get(model, {})
            rows.append(
                {
                    **key_values,
                    "model_id": model,
                    "mean_squared_error": float(mean_losses.loc[model]) if model in mean_losses.index else float("nan"),
                    "initial_model_count": int(len(initial_models)),
                    "final_model_count": int(len(current)),
                    "n_obs_final_set": final_n,
                    "alpha": float(alpha),
                    "bootstrap_reps": int(bootstrap_reps),
                    "block_length": int(eff_block_final),
                    "included_in_mcs_alpha_10pct": bool(model in current),
                    "elimination_step": removal.get("elimination_step", np.nan),
                    "p_value_at_elimination": removal.get("p_value_at_elimination", np.nan),
                    "test_stat_at_elimination": removal.get("test_stat_at_elimination", np.nan),
                    "last_set_p_value": last_p_value,
                    "last_set_test_stat": last_test_stat,
                    "method_note": "Block-bootstrap MCS-style elimination using average squared-loss differentials.",
                }
            )
    return pd.DataFrame(rows)


def _bootstrap_loss_difference_ci(
    frame: pd.DataFrame,
    outcome_col: str,
    *,
    bootstrap_reps: int = DEFAULT_MCS_BOOTSTRAP_REPS,
    seed: int = DEFAULT_RANDOM_SEED,
) -> pd.DataFrame:
    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[dict[str, object]] = []
    rng = np.random.default_rng(seed + 17)
    group_cols = [*KEY_COLUMNS, "outcome_id"]
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        models = sorted(group["model_id"].astype(str).unique())
        for baseline in BASELINE_MODELS:
            baseline_rows = group.loc[group["model_id"].eq(baseline)].set_index("target_quarter")
            if baseline_rows.empty:
                continue
            for model in models:
                if model == baseline:
                    continue
                model_rows = group.loc[group["model_id"].eq(model)].set_index("target_quarter")
                joined = model_rows[["forecast_error"]].join(
                    baseline_rows[["forecast_error"]],
                    how="inner",
                    lsuffix="_model",
                    rsuffix="_baseline",
                ).dropna()
                n_obs = len(joined)
                if n_obs < 8:
                    continue
                e_model = joined["forecast_error_model"].to_numpy(dtype=float)
                e_base = joined["forecast_error_baseline"].to_numpy(dtype=float)
                observed_rmse_diff = float(np.sqrt(np.mean(e_base**2)) - np.sqrt(np.mean(e_model**2)))
                observed_mae_diff = float(np.mean(np.abs(e_base)) - np.mean(np.abs(e_model)))
                block_length = max(1, int(np.floor(n_obs ** (1.0 / 3.0))))
                rmse_boot = np.zeros(bootstrap_reps, dtype=float)
                mae_boot = np.zeros(bootstrap_reps, dtype=float)
                for b in range(bootstrap_reps):
                    idx = _moving_block_sample_indices(n_obs, block_length, rng)
                    base_b = e_base[idx]
                    model_b = e_model[idx]
                    rmse_boot[b] = np.sqrt(np.mean(base_b**2)) - np.sqrt(np.mean(model_b**2))
                    mae_boot[b] = np.mean(np.abs(base_b)) - np.mean(np.abs(model_b))
                rows.append(
                    {
                        **key_values,
                        "baseline_model": baseline,
                        "model_id": model,
                        "n_obs": int(n_obs),
                        "bootstrap_reps": int(bootstrap_reps),
                        "block_length": int(block_length),
                        "rmse_diff_baseline_minus_model": observed_rmse_diff,
                        "rmse_diff_ci_low_95": float(np.quantile(rmse_boot, 0.025)),
                        "rmse_diff_ci_high_95": float(np.quantile(rmse_boot, 0.975)),
                        "mae_diff_baseline_minus_model": observed_mae_diff,
                        "mae_diff_ci_low_95": float(np.quantile(mae_boot, 0.025)),
                        "mae_diff_ci_high_95": float(np.quantile(mae_boot, 0.975)),
                        "positive_diff_interpretation": "baseline_has_higher_error_than_model",
                    }
                )
    return pd.DataFrame(rows)


def _expanding_density_records(frame: pd.DataFrame, outcome_col: str, min_calibration: int) -> pd.DataFrame:
    work = _prepare_forecast_frame(frame, outcome_col).sort_values(["model_id", "timing_mode", "checkpoint_id", "outcome_id", "target_period"])
    rows: list[dict[str, object]] = []
    group_cols = ["model_id", *KEY_COLUMNS, "outcome_id"]
    z_68 = 1.0
    z_90 = 1.6448536269514722
    effective_min_calibration = max(int(min_calibration), 2)
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        group = group.sort_values("target_period").reset_index(drop=True)
        prior_errors: list[float] = []
        for _, row in group.iterrows():
            error = float(row["forecast_error"])
            model_variance = row.get("forecast_variance", np.nan)
            if np.isfinite(model_variance) and float(model_variance) > MIN_VARIANCE:
                variance = float(model_variance)
                density_method = str(row.get("density_source", "model_implied_state_space"))
                calibration_n = int(len(prior_errors))
            elif len(prior_errors) >= effective_min_calibration:
                variance = float(np.var(prior_errors, ddof=1))
                variance = max(variance, MIN_VARIANCE)
                density_method = "expanding_residual_calibrated_gaussian"
                calibration_n = int(len(prior_errors))
            else:
                variance = float("nan")
                density_method = ""
                calibration_n = int(len(prior_errors))
            if np.isfinite(variance):
                mean = float(row["forecast_value"])
                realized = float(row["realized_value"])
                sd = sqrt(variance)
                z_realized = (realized - mean) / sd if sd > 0 else float("nan")
                pit = _normal_cdf(z_realized)
                prob_below_zero = _normal_cdf((0.0 - mean) / sd) if sd > 0 else float("nan")
                prob_large_positive = 1.0 - _normal_cdf((0.2 - mean) / sd) if sd > 0 else float("nan")
                prob_large_negative = _normal_cdf((-0.2 - mean) / sd) if sd > 0 else float("nan")
                log_score = float(gaussian_log_score(np.array([realized]), np.array([mean]), np.array([variance]))[0])
                crps = float(gaussian_crps(np.array([realized]), np.array([mean]), np.array([variance]))[0])
                rows.append(
                    {
                        **key_values,
                        "forecast_origin": row["forecast_origin"],
                        "forecast_origin_date": row["forecast_origin_date"],
                        "target_quarter": row["target_quarter"],
                        "forecast_value": mean,
                        "realized_value": realized,
                        "forecast_error": error,
                        "calibration_n": calibration_n,
                        "predictive_variance": variance,
                        "predictive_sd": sd,
                        "log_score": log_score,
                        "crps": crps,
                        "pit": pit,
                        "prob_below_zero": prob_below_zero,
                        "prob_large_positive_revision_gt_0p2": prob_large_positive,
                        "prob_large_negative_revision_lt_minus_0p2": prob_large_negative,
                        "realized_below_zero": bool(realized < 0.0),
                        "realized_large_positive_revision_gt_0p2": bool(realized > 0.2),
                        "realized_large_negative_revision_lt_minus_0p2": bool(realized < -0.2),
                        "lower_68": mean - z_68 * sd,
                        "upper_68": mean + z_68 * sd,
                        "covered_68": bool((realized >= mean - z_68 * sd) and (realized <= mean + z_68 * sd)),
                        "lower_90": mean - z_90 * sd,
                        "upper_90": mean + z_90 * sd,
                        "covered_90": bool((realized >= mean - z_90 * sd) and (realized <= mean + z_90 * sd)),
                        "density_method": density_method,
                    }
                )
            if np.isfinite(error):
                prior_errors.append(error)
    return pd.DataFrame(rows)


def _density_metrics(records: pd.DataFrame) -> pd.DataFrame:
    if records.empty:
        return pd.DataFrame()
    group_cols = ["model_id", *KEY_COLUMNS, "outcome_id"]
    rows: list[dict[str, object]] = []
    for keys, group in records.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        rows.append(
            {
                **key_values,
                "n_density_forecasts": int(len(group)),
                "mean_log_score": float(group["log_score"].mean()),
                "mean_crps": float(group["crps"].mean()),
                "coverage_68": float(group["covered_68"].mean()),
                "coverage_90": float(group["covered_90"].mean()),
                "mean_predictive_sd": float(group["predictive_sd"].mean()),
                "pit_mean": float(group["pit"].mean()) if "pit" in group else np.nan,
                "pit_std": float(group["pit"].std(ddof=1)) if "pit" in group and len(group) > 1 else np.nan,
                "brier_below_zero": float(np.mean((group["prob_below_zero"] - group["realized_below_zero"].astype(float)) ** 2))
                if {"prob_below_zero", "realized_below_zero"}.issubset(group.columns)
                else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _cumulative_loss_difference(frame: pd.DataFrame, outcome_col: str) -> pd.DataFrame:
    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[pd.DataFrame] = []
    group_cols = [*KEY_COLUMNS, "outcome_id"]
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        models = sorted(group["model_id"].astype(str).unique())
        for baseline in BASELINE_MODELS:
            baseline_rows = group.loc[group["model_id"].eq(baseline)].set_index("target_quarter")
            if baseline_rows.empty:
                continue
            for model in models:
                if model == baseline:
                    continue
                model_rows = group.loc[group["model_id"].eq(model)].set_index("target_quarter")
                joined = model_rows[["forecast_error", "target_period"]].join(
                    baseline_rows[["forecast_error"]],
                    how="inner",
                    lsuffix="_model",
                    rsuffix="_baseline",
                )
                if joined.empty:
                    continue
                joined = joined.sort_values("target_period").reset_index().rename(columns={"index": "target_quarter"})
                loss_diff = joined["forecast_error_baseline"] ** 2 - joined["forecast_error_model"] ** 2
                out = pd.DataFrame(
                    {
                        **key_values,
                        "baseline_model": baseline,
                        "model_id": model,
                        "target_quarter": joined["target_quarter"],
                        "loss_diff_baseline_minus_model": loss_diff,
                        "cumulative_loss_diff": loss_diff.cumsum(),
                    }
                )
                rows.append(out)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _external_benchmark_coverage(point: pd.DataFrame, revision: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for table_name, frame, outcome_col in [
        ("point", point, "target_id"),
        ("revision", revision, "revision_target_id"),
    ]:
        if frame.empty:
            rows.append({"table": table_name, "model_id": "spf", "status": "no_forecast_rows"})
            continue
        for keys, group in frame.groupby([*KEY_COLUMNS, outcome_col], dropna=False):
            key_values = dict(zip([*KEY_COLUMNS, "outcome_id"], keys))
            all_targets = set(group["target_quarter"].astype(str))
            for model_id, model_group in group.groupby("model_id", dropna=False):
                model_targets = set(model_group["target_quarter"].astype(str))
                rows.append(
                    {
                        "table": table_name,
                        **key_values,
                        "model_id": model_id,
                        "n_targets": int(len(model_targets)),
                        "share_of_group_targets": float(len(model_targets) / max(len(all_targets), 1)),
                        "first_target_quarter": min(model_targets) if model_targets else "",
                        "last_target_quarter": max(model_targets) if model_targets else "",
                        "status": "available",
                    }
                )
            if "spf" not in set(group["model_id"].astype(str)):
                rows.append(
                    {
                        "table": table_name,
                        **key_values,
                        "model_id": "spf",
                        "n_targets": 0,
                        "share_of_group_targets": 0.0,
                        "first_target_quarter": "",
                        "last_target_quarter": "",
                        "status": "missing_or_not_supplied",
                    }
                )
    return pd.DataFrame(rows)


def _common_sample_metrics(frame: pd.DataFrame, outcome_col: str, anchor_model: str = "spf") -> pd.DataFrame:
    if frame.empty or anchor_model not in set(frame["model_id"].astype(str)):
        return pd.DataFrame()
    work = _prepare_forecast_frame(frame, outcome_col)
    rows: list[dict[str, object]] = []
    for keys, group in work.groupby([*KEY_COLUMNS, "outcome_id"], dropna=False):
        key_values = dict(zip([*KEY_COLUMNS, "outcome_id"], keys))
        anchor_targets = set(group.loc[group["model_id"].eq(anchor_model), "target_quarter"].astype(str))
        if not anchor_targets:
            continue
        for model_id, model_group in group.groupby("model_id", dropna=False):
            subset = model_group.loc[model_group["target_quarter"].astype(str).isin(anchor_targets)].dropna(
                subset=["forecast_error"]
            )
            errors = pd.to_numeric(subset["forecast_error"], errors="coerce").dropna()
            if errors.empty:
                continue
            rows.append(
                {
                    **key_values,
                    "anchor_model": anchor_model,
                    "model_id": model_id,
                    "n_common_forecasts": int(errors.size),
                    "RMSE": float(np.sqrt(np.mean(errors**2))),
                    "MAE": float(np.mean(np.abs(errors))),
                    "bias": float(errors.mean()),
                    "first_target_quarter": str(subset["target_quarter"].min()),
                    "last_target_quarter": str(subset["target_quarter"].max()),
                }
            )
    return pd.DataFrame(rows)


def _revision_threshold_diagnostics(revision: pd.DataFrame) -> pd.DataFrame:
    if revision.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    thresholds = [0.0, 0.1, 0.2]
    work = revision.copy()
    work["forecast_value"] = pd.to_numeric(work["forecast_value"], errors="coerce")
    work["realized_value"] = pd.to_numeric(work["realized_value"], errors="coerce")
    for keys, group in work.groupby(["model_id", *KEY_COLUMNS, "revision_target_id"], dropna=False):
        key_values = dict(zip(["model_id", *KEY_COLUMNS, "outcome_id"], keys))
        clean = group.dropna(subset=["forecast_value", "realized_value"]).copy()
        for threshold in thresholds:
            subset = clean.loc[clean["realized_value"].abs() > threshold].copy()
            if subset.empty:
                rows.append({**key_values, "threshold_abs_revision": threshold, "n_obs": 0})
                continue
            actual_positive = subset["realized_value"] > 0.0
            pred_positive = subset["forecast_value"] > 0.0
            tp = int((actual_positive & pred_positive).sum())
            tn = int((~actual_positive & ~pred_positive).sum())
            fp = int((~actual_positive & pred_positive).sum())
            fn = int((actual_positive & ~pred_positive).sum())
            pos_total = tp + fn
            neg_total = tn + fp
            predicted_positive_total = tp + fp
            sensitivity = tp / pos_total if pos_total else np.nan
            specificity = tn / neg_total if neg_total else np.nan
            precision = tp / predicted_positive_total if predicted_positive_total else np.nan
            rows.append(
                {
                    **key_values,
                    "threshold_abs_revision": threshold,
                    "n_obs": int(len(subset)),
                    "sign_accuracy": float((np.sign(subset["forecast_value"]) == np.sign(subset["realized_value"])).mean()),
                    "balanced_accuracy": float(np.nanmean([sensitivity, specificity])),
                    "sensitivity": float(sensitivity) if np.isfinite(sensitivity) else np.nan,
                    "specificity": float(specificity) if np.isfinite(specificity) else np.nan,
                    "precision": float(precision) if np.isfinite(precision) else np.nan,
                    "actual_positive_pred_positive": tp,
                    "actual_positive_pred_nonpositive": fn,
                    "actual_negative_pred_positive": fp,
                    "actual_negative_pred_nonpositive": tn,
                    "mean_abs_realized_revision": float(subset["realized_value"].abs().mean()),
                    "mean_abs_forecast_revision": float(subset["forecast_value"].abs().mean()),
                }
            )
    return pd.DataFrame(rows)


def _revision_magnitude_bins(revision: pd.DataFrame) -> pd.DataFrame:
    if revision.empty:
        return pd.DataFrame()
    work = revision.copy()
    work["abs_realized_revision"] = pd.to_numeric(work["realized_value"], errors="coerce").abs()
    work["squared_error"] = pd.to_numeric(work["forecast_error"], errors="coerce") ** 2
    work["absolute_error"] = pd.to_numeric(work["forecast_error"], errors="coerce").abs()
    bins = [-np.inf, 0.1, 0.2, 0.5, np.inf]
    labels = ["abs_le_0p1", "abs_0p1_0p2", "abs_0p2_0p5", "abs_gt_0p5"]
    work["revision_size_bin"] = pd.cut(work["abs_realized_revision"], bins=bins, labels=labels)
    rows: list[dict[str, object]] = []
    for keys, group in work.groupby(["model_id", *KEY_COLUMNS, "revision_target_id", "revision_size_bin"], dropna=False, observed=False):
        key_values = dict(zip(["model_id", *KEY_COLUMNS, "outcome_id", "revision_size_bin"], keys))
        clean = group.dropna(subset=["squared_error", "absolute_error"])
        if clean.empty:
            continue
        rows.append(
            {
                **key_values,
                "n_obs": int(len(clean)),
                "RMSE": float(np.sqrt(clean["squared_error"].mean())),
                "MAE": float(clean["absolute_error"].mean()),
                "bias": float(pd.to_numeric(clean["forecast_error"], errors="coerce").mean()),
            }
        )
    return pd.DataFrame(rows)


def _release_mechanism_analysis(point: pd.DataFrame) -> pd.DataFrame:
    if point.empty or "no_revision" not in set(point["model_id"].astype(str)):
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    work = _prepare_forecast_frame(point, "target_id")
    for keys, group in work.groupby([*KEY_COLUMNS, "outcome_id"], dropna=False):
        key_values = dict(zip([*KEY_COLUMNS, "outcome_id"], keys))
        if key_values["checkpoint_id"] not in {"pre_second", "pre_third"}:
            continue
        baseline = group.loc[group["model_id"].eq("no_revision")].set_index("target_quarter")
        if baseline.empty:
            continue
        for model_id, model_group in group.groupby("model_id", dropna=False):
            if model_id == "no_revision":
                continue
            joined = model_group.set_index("target_quarter")[["forecast_error", "forecast_value", "realized_value"]].join(
                baseline[["forecast_error", "forecast_value"]],
                how="inner",
                lsuffix="_model",
                rsuffix="_no_revision",
            ).dropna()
            if joined.empty:
                continue
            official_revision = joined["realized_value"] - joined["forecast_value_no_revision"]
            loss_gain = joined["forecast_error_no_revision"] ** 2 - joined["forecast_error_model"] ** 2
            abs_revision = official_revision.abs()
            bins = pd.cut(abs_revision, bins=[-np.inf, 0.1, 0.2, 0.5, np.inf], labels=["abs_le_0p1", "abs_0p1_0p2", "abs_0p2_0p5", "abs_gt_0p5"])
            for bin_label in bins.cat.categories:
                idx = bins.index[bins == bin_label]
                sub_gain = loss_gain.loc[idx]
                sub_revision = official_revision.loc[idx]
                if sub_gain.empty:
                    continue
                rows.append(
                    {
                        **key_values,
                        "model_id": model_id,
                        "revision_size_bin": str(bin_label),
                        "n_obs": int(len(sub_gain)),
                        "mean_squared_loss_gain_vs_no_revision": float(sub_gain.mean()),
                        "share_positive_loss_gain": float((sub_gain > 0.0).mean()),
                        "mean_official_revision": float(sub_revision.mean()),
                        "mean_abs_official_revision": float(sub_revision.abs().mean()),
                    }
                )
    return pd.DataFrame(rows)


def _series_coverage_audit(repo_root: Path) -> pd.DataFrame:
    path = repo_root / "data/bronze/indicators/alfred_monthly_long.csv"
    if not path.exists():
        return pd.DataFrame(
            [{"audit_item": "series_coverage", "status": "missing", "path": str(path)}]
        )
    usecols = ["series_id", "series_frequency", "observation_date", "realtime_start", "value_numeric"]
    raw = pd.read_csv(path, usecols=usecols, parse_dates=["observation_date", "realtime_start"], low_memory=False)
    raw["value_numeric"] = pd.to_numeric(raw["value_numeric"], errors="coerce")
    rows = []
    for series_id, group in raw.groupby("series_id", dropna=False):
        valid = group.loc[group["value_numeric"].notna()]
        rows.append(
            {
                "series_id": series_id,
                "frequency_values": ";".join(sorted(group["series_frequency"].astype(str).unique())),
                "n_rows": int(len(group)),
                "n_valid_values": int(len(valid)),
                "n_observation_periods": int(group["observation_date"].nunique()),
                "n_realtime_vintages": int(group["realtime_start"].nunique()),
                "first_observation_date": group["observation_date"].min().date().isoformat(),
                "last_observation_date": group["observation_date"].max().date().isoformat(),
                "first_realtime_start": group["realtime_start"].min().date().isoformat(),
                "last_realtime_start": group["realtime_start"].max().date().isoformat(),
                "missing_value_share": float(1.0 - len(valid) / max(len(group), 1)),
            }
        )
    return pd.DataFrame(rows).sort_values("series_id").reset_index(drop=True)


def _target_coverage_audit(repo_root: Path, point_forecasts: pd.DataFrame, revision_forecasts: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    target_path = repo_root / "data/bronze/targets/gdp_release_targets.csv"
    if target_path.exists():
        targets = pd.read_csv(target_path)
        rows.append({"audit_item": "gdp_target_file_rows", "value": int(len(targets)), "detail": str(target_path)})
        for col in [c for c in ["A", "S", "T", "M"] if c in targets.columns]:
            rows.append(
                {
                    "audit_item": f"gdp_target_nonmissing::{col}",
                    "value": int(pd.to_numeric(targets[col], errors="coerce").notna().sum()),
                    "detail": str(target_path),
                }
            )
    for label, frame, outcome_col in [
        ("point", point_forecasts, "target_id"),
        ("revision", revision_forecasts, "revision_target_id"),
    ]:
        if frame.empty:
            continue
        for keys, group in frame.groupby(["timing_mode", "checkpoint_id", outcome_col]):
            timing_mode, checkpoint_id, outcome_id = keys
            rows.append(
                {
                    "audit_item": f"{label}_forecast_coverage::{timing_mode}::{checkpoint_id}::{outcome_id}",
                    "value": int(group.groupby("model_id")["target_quarter"].nunique().min()),
                    "detail": "minimum unique target quarters across compared models",
                }
            )
    return pd.DataFrame(rows)


def _calendar_audit(calendar: pd.DataFrame) -> pd.DataFrame:
    rows = [
        {"audit_item": "calendar_rows", "value": int(len(calendar)), "detail": "gdp_release_calendar_used.csv"},
    ]
    if "derivation_status" in calendar.columns:
        for status, count in calendar["derivation_status"].astype(str).value_counts().sort_index().items():
            rows.append({"audit_item": f"calendar_status::{status}", "value": int(count), "detail": "all release rounds"})
        if {"target_quarter", "release_round"}.issubset(calendar.columns):
            headline = calendar.loc[
                calendar["release_round"].isin(["A", "S", "T"])
                & calendar["target_quarter"].map(lambda q: _quarter_to_period(q) >= _quarter_to_period("2005:Q1"))
                & calendar["target_quarter"].map(lambda q: _quarter_to_period(q) <= _quarter_to_period("2024:Q4"))
            ]
            for status, count in headline["derivation_status"].astype(str).value_counts().sort_index().items():
                rows.append({"audit_item": f"headline_A_S_T_calendar_status::{status}", "value": int(count), "detail": "2005Q1-2024Q4"})
    return pd.DataFrame(rows)


def _write_figures(output_dir: Path, cumulative_point: pd.DataFrame, cumulative_revision: pd.DataFrame) -> list[Path]:
    figure_dir = output_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover
        (figure_dir / "FIGURE_GENERATION_SKIPPED.txt").write_text(str(exc), encoding="utf-8")
        return []
    figure_paths: list[Path] = []
    examples = [
        (
            cumulative_point,
            "exact",
            "pre_third",
            "T",
            "release_dfm",
            "joint_indicator_revision_dfm_full_kalman_em",
            "point_cumulative_loss_exact_pre_third_T_joint_vs_release.png",
        ),
        (
            cumulative_point,
            "exact",
            "pre_second",
            "S",
            "standard_dfm",
            "release_dfm",
            "point_cumulative_loss_exact_pre_second_S_release_vs_standard.png",
        ),
        (
            cumulative_revision,
            "exact",
            "pre_advance",
            "DELTA_SA",
            "release_dfm",
            "joint_indicator_revision_dfm_full_kalman_em",
            "revision_cumulative_loss_exact_delta_sa_joint_vs_release.png",
        ),
    ]
    for frame, timing, checkpoint, outcome, baseline, model, filename in examples:
        if frame.empty:
            continue
        subset = frame.loc[
            frame["timing_mode"].eq(timing)
            & frame["checkpoint_id"].eq(checkpoint)
            & frame["outcome_id"].eq(outcome)
            & frame["baseline_model"].eq(baseline)
            & frame["model_id"].eq(model)
        ].copy()
        if subset.empty:
            continue
        subset["period"] = subset["target_quarter"].map(_quarter_to_period)
        subset = subset.sort_values("period")
        fig, ax = plt.subplots(figsize=(9, 4.8))
        ax.plot(subset["target_quarter"], subset["cumulative_loss_diff"], color="#315f72", linewidth=1.8)
        ax.axhline(0.0, color="black", linewidth=0.8)
        ax.set_title(f"Cumulative squared-error gain: {model} vs {baseline}")
        ax.set_ylabel("Cumulative baseline loss minus model loss")
        ax.set_xlabel("Target quarter")
        ax.tick_params(axis="x", rotation=90, labelsize=6)
        fig.tight_layout()
        path = figure_dir / filename
        fig.savefig(path, dpi=180)
        plt.close(fig)
        figure_paths.append(path)
    return figure_paths


def _markdown_table(frame: pd.DataFrame, columns: Iterable[str], digits: int = 4, max_rows: int = 20) -> str:
    cols = [col for col in columns if col in frame.columns]
    if not cols or frame.empty:
        return "_No rows._"
    display = frame.loc[:, cols].head(max_rows)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    rows = []
    for _, row in display.iterrows():
        values = []
        for col in cols:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                values.append("" if not np.isfinite(value) else f"{value:.{digits}f}")
            else:
                values.append(str(value))
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join([header, sep, *rows])


def _write_summary(
    output_dir: Path,
    source_dir: Path,
    point_dm: pd.DataFrame,
    point_mcs_bootstrap: pd.DataFrame,
    point_density: pd.DataFrame,
    revision_density: pd.DataFrame,
    calendar_audit: pd.DataFrame,
    figure_paths: list[Path],
) -> Path:
    lines = [
        "# Journal Evidence Package",
        "",
        f"Generated UTC: `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`",
        "",
        f"Source backtest directory: `{source_dir.resolve()}`",
        "",
        "## What This Adds",
        "",
        "- HAC Diebold-Mariano-style forecast comparison tests.",
        "- Clark-West-style adjusted tests for nested or near-nested structured comparisons.",
        "- A model-confidence-set proxy based on squared-error loss differences.",
        "- Block-bootstrap MCS-style confidence sets by timing/checkpoint/target.",
        "- Model-implied state-space density evaluation where forecast variances are available, with leakage-safe residual calibration as fallback.",
        "- Cumulative squared-error difference tables and selected figures.",
        "- Data coverage and calendar audit tables.",
        "",
        "## Main Caveat",
        "",
        "State-space rows can use model-implied predictive variance from the Kalman measurement block. Non-state-space benchmarks still use leakage-safe expanding residual calibration when enough prior forecast errors exist. The MCS-style table is a transparent block-bootstrap implementation; if a target journal requires an exact Hansen-Lunde-Nason implementation, treat it as an auditable diagnostic layer rather than the final word.",
        "",
        "## Selected DM/HAC Rows",
        "",
        _markdown_table(
            point_dm.sort_values(["timing_mode", "checkpoint_id", "outcome_id", "baseline_model", "p_value_model_better_one_sided"]),
            [
                "timing_mode",
                "checkpoint_id",
                "outcome_id",
                "baseline_model",
                "model_id",
                "n_obs",
                "mean_loss_diff",
                "test_stat",
                "p_value_model_better_one_sided",
            ],
            max_rows=15,
        ),
        "",
        "## Point MCS-Style Bootstrap Rows",
        "",
        _markdown_table(
            point_mcs_bootstrap.sort_values(["timing_mode", "checkpoint_id", "outcome_id", "included_in_mcs_alpha_10pct", "mean_squared_error"], ascending=[True, True, True, False, True]),
            [
                "timing_mode",
                "checkpoint_id",
                "outcome_id",
                "model_id",
                "mean_squared_error",
                "included_in_mcs_alpha_10pct",
                "final_model_count",
                "last_set_p_value",
            ],
            max_rows=20,
        ),
        "",
        "## Point Density Metrics",
        "",
        _markdown_table(
            point_density.sort_values(["timing_mode", "checkpoint_id", "outcome_id", "mean_crps"]),
            [
                "model_id",
                "timing_mode",
                "checkpoint_id",
                "outcome_id",
                "n_density_forecasts",
                "mean_log_score",
                "mean_crps",
                "coverage_68",
                "coverage_90",
            ],
            max_rows=18,
        ),
        "",
        "## Revision Density Metrics",
        "",
        _markdown_table(
            revision_density.sort_values(["timing_mode", "checkpoint_id", "outcome_id", "mean_crps"]),
            [
                "model_id",
                "timing_mode",
                "checkpoint_id",
                "outcome_id",
                "n_density_forecasts",
                "mean_log_score",
                "mean_crps",
                "coverage_68",
                "coverage_90",
            ],
            max_rows=18,
        ),
        "",
        "## Calendar Audit",
        "",
        _markdown_table(calendar_audit, ["audit_item", "value", "detail"], max_rows=20),
        "",
    ]
    if figure_paths:
        lines.extend(["## Figures", ""])
        for path in figure_paths:
            lines.append(f"- `{path.relative_to(output_dir)}`")
        lines.append("")
    path = output_dir / "journal_evidence_summary.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_journal_evidence_package(
    repo_root: Path,
    source_dir: Path,
    output_dir: Path,
    min_density_calibration: int = 12,
    mcs_bootstrap_reps: int = DEFAULT_MCS_BOOTSTRAP_REPS,
) -> dict[str, Path]:
    repo_root = repo_root.resolve()
    source_dir = (repo_root / source_dir).resolve() if not source_dir.is_absolute() else source_dir.resolve()
    output_dir = (repo_root / output_dir).resolve() if not output_dir.is_absolute() else output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = _load_backtest_outputs(source_dir)
    point = outputs["point"]
    revision = outputs["revision"]

    point_dm = _pairwise_forecast_tests(point, "target_id")
    revision_dm = _pairwise_forecast_tests(revision, "revision_target_id")
    point_cw = _clark_west_tests(point, "target_id")
    revision_cw = _clark_west_tests(revision, "revision_target_id")
    point_mcs = _mcs_proxy(point, "target_id")
    revision_mcs = _mcs_proxy(revision, "revision_target_id")
    point_mcs_bootstrap = _mcs_block_bootstrap(point, "target_id", bootstrap_reps=mcs_bootstrap_reps)
    revision_mcs_bootstrap = _mcs_block_bootstrap(revision, "revision_target_id", bootstrap_reps=mcs_bootstrap_reps)
    point_bootstrap_ci = _bootstrap_loss_difference_ci(point, "target_id", bootstrap_reps=mcs_bootstrap_reps)
    revision_bootstrap_ci = _bootstrap_loss_difference_ci(revision, "revision_target_id", bootstrap_reps=mcs_bootstrap_reps)
    point_density_records = _expanding_density_records(point, "target_id", min_density_calibration)
    revision_density_records = _expanding_density_records(revision, "revision_target_id", min_density_calibration)
    point_density_metrics = _density_metrics(point_density_records)
    revision_density_metrics = _density_metrics(revision_density_records)
    point_cumulative = _cumulative_loss_difference(point, "target_id")
    revision_cumulative = _cumulative_loss_difference(revision, "revision_target_id")
    external_coverage = _external_benchmark_coverage(point, revision)
    point_common_spf = _common_sample_metrics(point, "target_id", anchor_model="spf")
    revision_common_spf = _common_sample_metrics(revision, "revision_target_id", anchor_model="spf")
    revision_thresholds = _revision_threshold_diagnostics(revision)
    revision_bins = _revision_magnitude_bins(revision)
    mechanism = _release_mechanism_analysis(point)
    calendar_audit = _calendar_audit(outputs["calendar"])
    series_audit = _series_coverage_audit(repo_root)
    target_audit = _target_coverage_audit(repo_root, point, revision)
    figure_paths = _write_figures(output_dir, point_cumulative, revision_cumulative)

    paths = {
        "forecast_comparison_point_dm": output_dir / "forecast_comparison_point_dm.csv",
        "forecast_comparison_revision_dm": output_dir / "forecast_comparison_revision_dm.csv",
        "forecast_comparison_point_clark_west": output_dir / "forecast_comparison_point_clark_west.csv",
        "forecast_comparison_revision_clark_west": output_dir / "forecast_comparison_revision_clark_west.csv",
        "model_confidence_set_proxy_point": output_dir / "model_confidence_set_proxy_point.csv",
        "model_confidence_set_proxy_revision": output_dir / "model_confidence_set_proxy_revision.csv",
        "model_confidence_set_block_bootstrap_point": output_dir / "model_confidence_set_block_bootstrap_point.csv",
        "model_confidence_set_block_bootstrap_revision": output_dir / "model_confidence_set_block_bootstrap_revision.csv",
        "bootstrap_loss_difference_point": output_dir / "bootstrap_loss_difference_point.csv",
        "bootstrap_loss_difference_revision": output_dir / "bootstrap_loss_difference_revision.csv",
        "density_point_records": output_dir / "density_point_records.csv",
        "density_revision_records": output_dir / "density_revision_records.csv",
        "density_point_metrics": output_dir / "density_point_metrics.csv",
        "density_revision_metrics": output_dir / "density_revision_metrics.csv",
        "cumulative_loss_point": output_dir / "cumulative_loss_point.csv",
        "cumulative_loss_revision": output_dir / "cumulative_loss_revision.csv",
        "external_benchmark_coverage": output_dir / "external_benchmark_coverage.csv",
        "common_sample_spf_point": output_dir / "common_sample_spf_point.csv",
        "common_sample_spf_revision": output_dir / "common_sample_spf_revision.csv",
        "revision_threshold_diagnostics": output_dir / "revision_threshold_diagnostics.csv",
        "revision_sign_threshold_diagnostics": output_dir / "revision_sign_threshold_diagnostics.csv",
        "revision_magnitude_bins": output_dir / "revision_magnitude_bins.csv",
        "release_mechanism_analysis": output_dir / "release_mechanism_analysis.csv",
        "calendar_audit": output_dir / "calendar_audit.csv",
        "series_coverage_audit": output_dir / "series_coverage_audit.csv",
        "target_coverage_audit": output_dir / "target_coverage_audit.csv",
    }
    point_dm.to_csv(paths["forecast_comparison_point_dm"], index=False)
    revision_dm.to_csv(paths["forecast_comparison_revision_dm"], index=False)
    point_cw.to_csv(paths["forecast_comparison_point_clark_west"], index=False)
    revision_cw.to_csv(paths["forecast_comparison_revision_clark_west"], index=False)
    point_mcs.to_csv(paths["model_confidence_set_proxy_point"], index=False)
    revision_mcs.to_csv(paths["model_confidence_set_proxy_revision"], index=False)
    point_mcs_bootstrap.to_csv(paths["model_confidence_set_block_bootstrap_point"], index=False)
    revision_mcs_bootstrap.to_csv(paths["model_confidence_set_block_bootstrap_revision"], index=False)
    point_bootstrap_ci.to_csv(paths["bootstrap_loss_difference_point"], index=False)
    revision_bootstrap_ci.to_csv(paths["bootstrap_loss_difference_revision"], index=False)
    point_density_records.to_csv(paths["density_point_records"], index=False)
    revision_density_records.to_csv(paths["density_revision_records"], index=False)
    point_density_metrics.to_csv(paths["density_point_metrics"], index=False)
    revision_density_metrics.to_csv(paths["density_revision_metrics"], index=False)
    point_cumulative.to_csv(paths["cumulative_loss_point"], index=False)
    revision_cumulative.to_csv(paths["cumulative_loss_revision"], index=False)
    external_coverage.to_csv(paths["external_benchmark_coverage"], index=False)
    point_common_spf.to_csv(paths["common_sample_spf_point"], index=False)
    revision_common_spf.to_csv(paths["common_sample_spf_revision"], index=False)
    revision_thresholds.to_csv(paths["revision_threshold_diagnostics"], index=False)
    revision_thresholds.to_csv(paths["revision_sign_threshold_diagnostics"], index=False)
    revision_bins.to_csv(paths["revision_magnitude_bins"], index=False)
    mechanism.to_csv(paths["release_mechanism_analysis"], index=False)
    calendar_audit.to_csv(paths["calendar_audit"], index=False)
    series_audit.to_csv(paths["series_coverage_audit"], index=False)
    target_audit.to_csv(paths["target_coverage_audit"], index=False)
    paths["summary"] = _write_summary(output_dir, source_dir, point_dm, point_mcs_bootstrap, point_density_metrics, revision_density_metrics, calendar_audit, figure_paths)
    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "repo_root": str(repo_root),
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "min_density_calibration": min_density_calibration,
        "mcs_bootstrap_reps": int(mcs_bootstrap_reps),
        "output_files": {name: str(path) for name, path in paths.items()},
        "figures": [str(path) for path in figure_paths],
        "method_notes": [
            "DM tests use HAC/Newey-West long-run variance with lag floor(n^(1/3)).",
            "Clark-West rows are diagnostic approximations for nested or near-nested comparisons.",
            "MCS proxy output is kept for continuity; block-bootstrap MCS-style output is also generated.",
            "Bootstrap RMSE/MAE difference confidence intervals use moving-block resampling of aligned forecast errors.",
            "State-space density forecasts use model-implied forecast variances when present; other rows use expanding residual-calibrated Gaussian variances.",
            "SPF common-sample tables are generated only when an SPF benchmark is present in the source forecast records.",
            "Revision diagnostics report thresholded direction accuracy because near-zero GDP revisions can dominate raw sign accuracy.",
        ],
    }
    paths["manifest"] = output_dir / "manifest.json"
    paths["manifest"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build journal-facing evidence tables from frozen exact/pseudo backtest outputs.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-density-calibration", type=int, default=12)
    parser.add_argument("--mcs-bootstrap-reps", type=int, default=DEFAULT_MCS_BOOTSTRAP_REPS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = build_journal_evidence_package(
        repo_root=args.repo_root,
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        min_density_calibration=args.min_density_calibration,
        mcs_bootstrap_reps=args.mcs_bootstrap_reps,
    )
    print(f"Wrote journal evidence package to {(args.repo_root / args.output_dir).resolve() if not args.output_dir.is_absolute() else args.output_dir.resolve()}")
    for name, path in paths.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
