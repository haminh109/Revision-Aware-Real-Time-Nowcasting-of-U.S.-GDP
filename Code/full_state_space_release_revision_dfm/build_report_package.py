from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from math import erfc, sqrt
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


DEFAULT_SOURCE_DIR = Path("outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest")
DEFAULT_OUTPUT_DIR = Path("outputs/full_state_space_release_revision_dfm/report_package")
TIMING_ORDER = ("exact", "pseudo")
CHECKPOINT_ORDER = ("pre_advance", "pre_second", "pre_third")
TARGET_ORDER = ("A", "S", "T")
REVISION_ORDER = ("DELTA_SA", "DELTA_TS", "DELTA_MT")
STRUCTURED_MODELS = {
    "release_dfm",
    "revision_dfm_kalman_em",
    "joint_indicator_revision_dfm_full_kalman_em",
}
PAPER_MODEL_ORDER = {
    "ar": 0,
    "no_revision": 1,
    "bridge": 2,
    "midas_umidas": 3,
    "spf": 4,
    "monthly_mixed_frequency_kalman_em": 5,
    "standard_dfm": 6,
    "release_dfm": 7,
    "revision_dfm_kalman_em": 8,
    "indicator_revision_only_dfm_kalman_em": 9,
    "joint_indicator_revision_dfm_full_kalman_em": 10,
}


def _quarter_to_period(label: str) -> pd.Period:
    year, quarter = str(label).split(":Q")
    return pd.Period(year=int(year), quarter=int(quarter), freq="Q")


def _safe_float(value: object) -> float:
    if value is None:
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _fmt_float(value: object, digits: int = 3) -> str:
    number = _safe_float(value)
    if not np.isfinite(number):
        return ""
    return f"{number:.{digits}f}"


def _load_required_csv(source_dir: Path, name: str, allow_empty: bool = False, **kwargs: object) -> pd.DataFrame:
    path = source_dir / name
    if not path.exists():
        raise FileNotFoundError(f"Required output file is missing: {path}")
    try:
        return pd.read_csv(path, **kwargs)
    except pd.errors.EmptyDataError:
        if allow_empty:
            return pd.DataFrame()
        raise


def _load_outputs(source_dir: Path) -> dict[str, pd.DataFrame]:
    outputs = {
        "forecast_results": _load_required_csv(source_dir, "forecast_results.csv", parse_dates=["forecast_origin_date"]),
        "revision_forecast_results": _load_required_csv(
            source_dir,
            "revision_forecast_results.csv",
            parse_dates=["forecast_origin_date"],
        ),
        "metrics_summary": _load_required_csv(source_dir, "metrics_summary.csv"),
        "revision_metrics_summary": _load_required_csv(source_dir, "revision_metrics_summary.csv"),
        "exact_pseudo_point_gaps": _load_required_csv(source_dir, "exact_pseudo_point_gaps.csv"),
        "exact_pseudo_revision_gaps": _load_required_csv(source_dir, "exact_pseudo_revision_gaps.csv"),
        "gdp_release_calendar": _load_required_csv(
            source_dir,
            "gdp_release_calendar_used.csv",
            parse_dates=["public_release_date"],
        ),
        "failures": _load_required_csv(source_dir, "failures.csv", allow_empty=True),
    }
    return outputs


def _sort_metrics(frame: pd.DataFrame, id_col: str) -> pd.DataFrame:
    out = frame.copy()
    out["_timing_order"] = out["timing_mode"].map({name: i for i, name in enumerate(TIMING_ORDER)}).fillna(99)
    out["_checkpoint_order"] = out["checkpoint_id"].map({name: i for i, name in enumerate(CHECKPOINT_ORDER)}).fillna(99)
    if id_col == "target_id":
        out["_id_order"] = out[id_col].map({name: i for i, name in enumerate(TARGET_ORDER)}).fillna(99)
    else:
        out["_id_order"] = out[id_col].map({name: i for i, name in enumerate(REVISION_ORDER)}).fillna(99)
    out["_model_order"] = out["model_id"].map(PAPER_MODEL_ORDER).fillna(99)
    sort_cols = ["_timing_order", "_checkpoint_order", "_id_order", "RMSE", "_model_order", "model_id"]
    out = out.sort_values(sort_cols).drop(columns=["_timing_order", "_checkpoint_order", "_id_order", "_model_order"])
    return out.reset_index(drop=True)


def _winner_table(metrics: pd.DataFrame, id_col: str, tie_tol: float = 1e-10) -> pd.DataFrame:
    group_cols = [col for col in ["subsample", "timing_mode", "checkpoint_id", id_col] if col in metrics.columns]
    rows: list[dict[str, object]] = []
    for key, group in metrics.groupby(group_cols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        row = dict(zip(group_cols, key))
        ordered = group.sort_values(["RMSE", "model_id"]).reset_index(drop=True)
        min_rmse = float(ordered["RMSE"].iloc[0])
        tied = ordered.loc[np.abs(ordered["RMSE"] - min_rmse) <= tie_tol].copy()
        best_models = "; ".join(sorted(tied["model_id"].astype(str).unique(), key=lambda x: PAPER_MODEL_ORDER.get(x, 99)))
        best = ordered.iloc[0]
        row.update(
            {
                "best_models": best_models,
                "best_RMSE": min_rmse,
                "best_MAE": best.get("MAE", np.nan),
                "best_bias": best.get("bias", np.nan),
                "best_relative_RMSFE": best.get("relative_RMSFE", np.nan),
                "best_DM_test": best.get("DM_test", np.nan),
                "best_sign_accuracy": best.get("sign_accuracy", np.nan),
                "best_n_forecasts": int(best.get("n_forecasts", len(group))),
                "n_models_compared": int(group["model_id"].nunique()),
                "structured_family_win": any(model in STRUCTURED_MODELS for model in best_models.split("; ")),
            }
        )
        rows.append(row)
    winners = pd.DataFrame(rows)
    return _sort_winner_table(winners, id_col)


def _sort_winner_table(frame: pd.DataFrame, id_col: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if "subsample" in out.columns:
        subsample_order = {
            "full_sample": 0,
            "exclude_pandemic": 1,
            "pre_gfc": 2,
            "gfc_and_recovery": 3,
            "post_gfc_pre_pandemic": 4,
            "post_pandemic": 5,
        }
        out["_subsample_order"] = out["subsample"].map(subsample_order).fillna(99)
    else:
        out["_subsample_order"] = 0
    out["_timing_order"] = out["timing_mode"].map({name: i for i, name in enumerate(TIMING_ORDER)}).fillna(99)
    out["_checkpoint_order"] = out["checkpoint_id"].map({name: i for i, name in enumerate(CHECKPOINT_ORDER)}).fillna(99)
    order_values = TARGET_ORDER if id_col == "target_id" else REVISION_ORDER
    out["_id_order"] = out[id_col].map({name: i for i, name in enumerate(order_values)}).fillna(99)
    out = out.sort_values(["_subsample_order", "_timing_order", "_checkpoint_order", "_id_order"])
    return out.drop(columns=["_subsample_order", "_timing_order", "_checkpoint_order", "_id_order"]).reset_index(drop=True)


def _normal_pvalue(z_stat: float) -> float:
    if not np.isfinite(z_stat):
        return float("nan")
    return erfc(abs(z_stat) / sqrt(2.0))


def _hac_dm_pvalue(loss_diff: pd.Series) -> float:
    values = pd.to_numeric(loss_diff, errors="coerce").dropna().to_numpy(dtype=float)
    n = values.size
    if n < 8:
        return float("nan")
    centered = values - values.mean()
    max_lag = max(1, int(np.floor(n ** (1.0 / 3.0))))
    gamma0 = float(np.dot(centered, centered) / n)
    long_run_var = gamma0
    for lag in range(1, max_lag + 1):
        cov = float(np.dot(centered[lag:], centered[:-lag]) / n)
        weight = 1.0 - lag / (max_lag + 1.0)
        long_run_var += 2.0 * weight * cov
    if long_run_var <= 0.0:
        return float("nan")
    dm_stat = values.mean() / sqrt(long_run_var / n)
    return _normal_pvalue(dm_stat)


def _metric_summary(frame: pd.DataFrame, id_col: str) -> pd.DataFrame:
    group_cols = [col for col in ["subsample", "model_id", "timing_mode", "checkpoint_id", id_col] if col in frame.columns]
    rows: list[dict[str, object]] = []
    for key, group in frame.groupby(group_cols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        row = dict(zip(group_cols, key))
        clean = group.dropna(subset=["forecast_value", "realized_value", "forecast_error"]).copy()
        errors = pd.to_numeric(clean["forecast_error"], errors="coerce").dropna()
        if errors.empty:
            continue
        forecast = pd.to_numeric(clean["forecast_value"], errors="coerce")
        realized = pd.to_numeric(clean["realized_value"], errors="coerce")
        row.update(
            {
                "n_forecasts": int(errors.size),
                "RMSE": float(np.sqrt(np.mean(np.square(errors)))),
                "MAE": float(np.mean(np.abs(errors))),
                "bias": float(np.mean(errors)),
                "sign_accuracy": float(np.mean(np.sign(forecast) == np.sign(realized))),
                "convergence_rate": float(pd.Series(clean.get("converged", np.nan)).astype("boolean").mean())
                if "converged" in clean
                else float("nan"),
                "mean_iterations": float(pd.to_numeric(clean.get("n_iter", np.nan), errors="coerce").mean())
                if "n_iter" in clean
                else float("nan"),
                "mean_llf_relative_last_improvement": float(
                    pd.to_numeric(clean.get("llf_relative_last_improvement", np.nan), errors="coerce").mean()
                )
                if "llf_relative_last_improvement" in clean
                else float("nan"),
                "median_llf_relative_last_improvement": float(
                    pd.to_numeric(clean.get("llf_relative_last_improvement", np.nan), errors="coerce").median()
                )
                if "llf_relative_last_improvement" in clean
                else float("nan"),
                "max_llf_relative_last_improvement": float(
                    pd.to_numeric(clean.get("llf_relative_last_improvement", np.nan), errors="coerce").max()
                )
                if "llf_relative_last_improvement" in clean
                else float("nan"),
            }
        )
        rows.append(row)
    metrics = pd.DataFrame(rows)
    if metrics.empty:
        return metrics
    rel_group_cols = [col for col in ["subsample", "timing_mode", "checkpoint_id", id_col] if col in metrics.columns]
    ar = metrics.loc[metrics["model_id"].eq("ar"), rel_group_cols + ["RMSE"]].rename(columns={"RMSE": "ar_RMSE"})
    metrics = metrics.merge(ar, on=rel_group_cols, how="left")
    metrics["relative_RMSFE"] = metrics["RMSE"] / metrics["ar_RMSE"]
    metrics = metrics.drop(columns=["ar_RMSE"])
    return metrics


def _attach_dm_against_ar(frame: pd.DataFrame, metrics: pd.DataFrame, id_col: str) -> pd.DataFrame:
    if frame.empty or metrics.empty:
        return metrics
    result = metrics.copy()
    result["DM_test_vs_ar_normal_approx"] = np.nan
    group_cols = [col for col in ["subsample", "timing_mode", "checkpoint_id", id_col] if col in frame.columns]
    merge_cols = [col for col in group_cols + ["target_quarter"] if col in frame.columns]
    if "forecast_origin" in frame.columns:
        merge_cols.append("forecast_origin")
    for metric_idx, metric_row in result.iterrows():
        if metric_row["model_id"] == "ar":
            result.loc[metric_idx, "DM_test_vs_ar_normal_approx"] = 1.0
            continue
        mask = pd.Series(True, index=frame.index)
        for col in group_cols:
            mask &= frame[col].astype(str).eq(str(metric_row[col]))
        model_rows = frame.loc[mask & frame["model_id"].astype(str).eq(str(metric_row["model_id"])), merge_cols + ["forecast_error"]]
        ar_rows = frame.loc[mask & frame["model_id"].astype(str).eq("ar"), merge_cols + ["forecast_error"]]
        if model_rows.empty or ar_rows.empty:
            continue
        merged = model_rows.merge(ar_rows, on=merge_cols, suffixes=("_model", "_ar"))
        if merged.empty:
            continue
        loss_diff = np.square(pd.to_numeric(merged["forecast_error_model"], errors="coerce")) - np.square(
            pd.to_numeric(merged["forecast_error_ar"], errors="coerce")
        )
        result.loc[metric_idx, "DM_test_vs_ar_normal_approx"] = _hac_dm_pvalue(loss_diff)
    return result


def _subsample_frames(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    work = frame.copy()
    work["_period"] = work["target_quarter"].map(_quarter_to_period)
    pandemic = {_quarter_to_period("2020:Q2"), _quarter_to_period("2020:Q3")}
    definitions = {
        "full_sample": pd.Series(True, index=work.index),
        "exclude_pandemic": ~work["_period"].isin(pandemic),
        "pre_gfc": work["_period"] <= _quarter_to_period("2007:Q4"),
        "gfc_and_recovery": (work["_period"] >= _quarter_to_period("2008:Q1"))
        & (work["_period"] <= _quarter_to_period("2014:Q4")),
        "post_gfc_pre_pandemic": (work["_period"] >= _quarter_to_period("2015:Q1"))
        & (work["_period"] <= _quarter_to_period("2019:Q4")),
        "post_pandemic": work["_period"] >= _quarter_to_period("2021:Q1"),
    }
    for name, mask in definitions.items():
        subset = work.loc[mask].drop(columns=["_period"]).copy()
        subset["subsample"] = name
        out[name] = subset
    return out


def _build_subsample_metrics(frame: pd.DataFrame, id_col: str) -> pd.DataFrame:
    metrics = []
    for subset in _subsample_frames(frame).values():
        if subset.empty:
            continue
        metrics.append(_metric_summary(subset, id_col))
    combined = pd.concat(metrics, ignore_index=True) if metrics else pd.DataFrame()
    if combined.empty:
        return combined
    enriched_frames = []
    for subset in _subsample_frames(frame).values():
        if subset.empty or "subsample" not in subset.columns:
            continue
        subset_metrics = combined.loc[combined["subsample"].eq(subset["subsample"].iloc[0])].copy()
        if subset_metrics.empty:
            continue
        enriched_frames.append(_attach_dm_against_ar(subset, subset_metrics, id_col))
    if not enriched_frames:
        return pd.DataFrame()
    return pd.concat(enriched_frames, ignore_index=True)


def _build_data_coverage(outputs: dict[str, pd.DataFrame], source_dir: Path) -> pd.DataFrame:
    forecasts = outputs["forecast_results"]
    revisions = outputs["revision_forecast_results"]
    calendar = outputs["gdp_release_calendar"]
    failures = outputs["failures"]
    rows: list[dict[str, object]] = [
        {"coverage_item": "source_dir", "value": str(source_dir)},
        {"coverage_item": "point_forecast_rows", "value": int(len(forecasts))},
        {"coverage_item": "revision_forecast_rows", "value": int(len(revisions))},
        {"coverage_item": "failure_rows", "value": int(len(failures))},
        {"coverage_item": "calendar_rows", "value": int(len(calendar))},
    ]
    if "derivation_status" in calendar.columns:
        status_counts = calendar["derivation_status"].astype(str).value_counts().sort_index()
        for status, count in status_counts.items():
            rows.append({"coverage_item": f"calendar_status::{status}", "value": int(count)})
        headline = calendar.loc[
            calendar["release_round"].isin(TARGET_ORDER)
            & calendar["target_quarter"].map(lambda q: _quarter_to_period(str(q)) >= _quarter_to_period("2005:Q1"))
            & calendar["target_quarter"].map(lambda q: _quarter_to_period(str(q)) <= _quarter_to_period("2024:Q4"))
        ]
        if not headline.empty:
            headline_counts = headline["derivation_status"].astype(str).value_counts().sort_index()
            for status, count in headline_counts.items():
                rows.append({"coverage_item": f"headline_A_S_T_calendar_status::{status}", "value": int(count)})
    for (timing, checkpoint, target), group in forecasts.groupby(["timing_mode", "checkpoint_id", "target_id"]):
        rows.append(
            {
                "coverage_item": f"point_coverage::{timing}::{checkpoint}::{target}",
                "value": int(group.groupby("model_id")["target_quarter"].nunique().min()),
            }
        )
    for (timing, checkpoint, revision), group in revisions.groupby(["timing_mode", "checkpoint_id", "revision_target_id"]):
        rows.append(
            {
                "coverage_item": f"revision_coverage::{timing}::{checkpoint}::{revision}",
                "value": int(group.groupby("model_id")["target_quarter"].nunique().min()),
            }
        )
    return pd.DataFrame(rows)


def _markdown_table(frame: pd.DataFrame, columns: Iterable[str], digits: int = 3) -> str:
    cols = list(columns)
    if frame.empty:
        return "_No rows._"
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = []
    for _, row in frame.loc[:, cols].iterrows():
        values = []
        for col in cols:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                values.append(_fmt_float(value, digits))
            else:
                values.append(str(value))
        body.append("| " + " | ".join(values) + " |")
    return "\n".join([header, sep, *body])


def _latex_escape(value: object) -> str:
    text = _fmt_float(value) if isinstance(value, (float, np.floating)) else str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _latex_table(frame: pd.DataFrame, columns: Iterable[str], caption: str, label: str) -> str:
    cols = list(columns)
    lines = [
        r"\begin{table}[!htbp]",
        r"\centering",
        rf"\caption{{{_latex_escape(caption)}}}",
        rf"\label{{{_latex_escape(label)}}}",
        r"\small",
        r"\begin{tabular}{" + "l" * len(cols) + r"}",
        r"\toprule",
        " & ".join(_latex_escape(col) for col in cols) + r" \\",
        r"\midrule",
    ]
    for _, row in frame.loc[:, cols].iterrows():
        lines.append(" & ".join(_latex_escape(row[col]) for col in cols) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{table}"])
    return "\n".join(lines)


def _write_latex_tables(
    output_dir: Path,
    point_winners: pd.DataFrame,
    revision_winners: pd.DataFrame,
    point_gaps: pd.DataFrame,
    revision_gaps: pd.DataFrame,
) -> Path:
    pieces = [
        "% Auto-generated by full_state_space_release_revision_dfm.build_report_package.",
        "% Requires \\usepackage{booktabs}.",
        "",
        _latex_table(
            point_winners,
            [
                "timing_mode",
                "checkpoint_id",
                "target_id",
                "best_models",
                "best_RMSE",
                "best_MAE",
                "best_bias",
                "best_n_forecasts",
            ],
            "Headline point-forecast winners",
            "tab:headline_point_winners",
        ),
        "",
        _latex_table(
            revision_winners,
            [
                "timing_mode",
                "checkpoint_id",
                "revision_target_id",
                "best_models",
                "best_RMSE",
                "best_MAE",
                "best_bias",
                "best_sign_accuracy",
                "best_n_forecasts",
            ],
            "Headline revision-forecast winners",
            "tab:headline_revision_winners",
        ),
        "",
        _latex_table(
            point_gaps,
            ["model_id", "checkpoint_id", "target_id", "exact", "pseudo", "exact_minus_pseudo_RMSE"],
            "Exact-minus-pseudo point-forecast RMSE gaps",
            "tab:exact_pseudo_point_gaps",
        ),
        "",
        _latex_table(
            revision_gaps,
            ["model_id", "checkpoint_id", "revision_target_id", "exact", "pseudo", "exact_minus_pseudo_RMSE"],
            "Exact-minus-pseudo revision-forecast RMSE gaps",
            "tab:exact_pseudo_revision_gaps",
        ),
        "",
    ]
    path = output_dir / "latex_tables.tex"
    path.write_text("\n".join(pieces), encoding="utf-8")
    return path


def _write_figures(output_dir: Path, point_metrics: pd.DataFrame, revision_metrics: pd.DataFrame, point_gaps: pd.DataFrame) -> list[Path]:
    figure_dir = output_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover - optional reporting dependency
        (figure_dir / "FIGURE_GENERATION_SKIPPED.txt").write_text(str(exc), encoding="utf-8")
        return paths

    for timing in TIMING_ORDER:
        subset = point_metrics.loc[point_metrics["timing_mode"].eq(timing)].copy()
        if subset.empty:
            continue
        fig, axes = plt.subplots(1, 3, figsize=(15, 4), sharex=False)
        for ax, checkpoint, target in zip(axes, CHECKPOINT_ORDER, TARGET_ORDER):
            cell = subset.loc[subset["checkpoint_id"].eq(checkpoint) & subset["target_id"].eq(target)].copy()
            cell = cell.sort_values("RMSE", ascending=True)
            ax.barh(cell["model_id"], cell["RMSE"], color="#315f72")
            ax.set_title(f"{timing}: {target} at {checkpoint}")
            ax.set_xlabel("RMSE")
            ax.invert_yaxis()
        fig.tight_layout()
        path = figure_dir / f"point_rmse_by_model_{timing}.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        paths.append(path)

    if not revision_metrics.empty:
        exact_revision = revision_metrics.loc[revision_metrics["timing_mode"].eq("exact")].copy()
        fig, axes = plt.subplots(1, 3, figsize=(15, 4), sharex=False)
        for ax, checkpoint, revision in zip(axes, CHECKPOINT_ORDER, REVISION_ORDER):
            cell = exact_revision.loc[
                exact_revision["checkpoint_id"].eq(checkpoint)
                & exact_revision["revision_target_id"].eq(revision)
            ].copy()
            cell = cell.sort_values("RMSE", ascending=True)
            ax.barh(cell["model_id"], cell["RMSE"], color="#7a5c2e")
            ax.set_title(f"exact: {revision}")
            ax.set_xlabel("RMSE")
            ax.invert_yaxis()
        fig.tight_layout()
        path = figure_dir / "revision_rmse_by_model_exact.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        paths.append(path)

    gap_subset = point_gaps.loc[point_gaps["model_id"].isin(STRUCTURED_MODELS | {"standard_dfm"})].copy()
    if not gap_subset.empty:
        gap_subset["label"] = gap_subset["model_id"] + " / " + gap_subset["target_id"]
        fig, ax = plt.subplots(figsize=(10, 5))
        colors = ["#9a3412" if value > 0 else "#2f6f4e" for value in gap_subset["exact_minus_pseudo_RMSE"]]
        ax.barh(gap_subset["label"], gap_subset["exact_minus_pseudo_RMSE"], color=colors)
        ax.axvline(0.0, color="black", linewidth=0.8)
        ax.set_xlabel("Exact RMSE minus pseudo RMSE")
        ax.set_title("Timing gaps for factor and structured models")
        fig.tight_layout()
        path = figure_dir / "exact_minus_pseudo_point_gaps.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        paths.append(path)
    return paths


def _winner_stability(point_winners: pd.DataFrame, revision_winners: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for table_name, frame, id_col in [
        ("point", point_winners, "target_id"),
        ("revision", revision_winners, "revision_target_id"),
    ]:
        if frame.empty:
            continue
        grouped_cols = [id_col]
        if "subsample" in frame.columns:
            grouped_cols.append("subsample")
        for key, group in frame.groupby(grouped_cols, dropna=False):
            if not isinstance(key, tuple):
                key = (key,)
            row = {"table": table_name}
            row.update(dict(zip(grouped_cols, key)))
            split_models = []
            for value in group["best_models"].astype(str):
                split_models.extend(model.strip() for model in value.split(";") if model.strip())
            row.update(
                {
                    "n_cells": int(len(group)),
                    "structured_family_cells": int(group["structured_family_win"].sum()),
                    "structured_family_share": float(group["structured_family_win"].mean()),
                    "distinct_winning_models": "; ".join(sorted(set(split_models), key=lambda x: PAPER_MODEL_ORDER.get(x, 99))),
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def _build_convergence_diagnostics(point_metrics: pd.DataFrame, revision_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    if not point_metrics.empty:
        point = point_metrics.copy()
        point["table"] = "point"
        point["outcome_id"] = point["target_id"]
        rows.append(point)
    if not revision_metrics.empty:
        revision = revision_metrics.copy()
        revision["table"] = "revision"
        revision["outcome_id"] = revision["revision_target_id"]
        rows.append(revision)
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    columns = [
        "table",
        "model_id",
        "timing_mode",
        "checkpoint_id",
        "outcome_id",
        "n_forecasts",
        "convergence_rate",
        "mean_iterations",
        "mean_llf_relative_last_improvement",
        "median_llf_relative_last_improvement",
        "max_llf_relative_last_improvement",
        "RMSE",
        "MAE",
        "bias",
    ]
    existing = [col for col in columns if col in combined.columns]
    return combined.loc[:, existing].sort_values(["table", "timing_mode", "checkpoint_id", "outcome_id", "model_id"])


def _generate_markdown(
    source_dir: Path,
    output_dir: Path,
    point_winners: pd.DataFrame,
    revision_winners: pd.DataFrame,
    point_gaps: pd.DataFrame,
    revision_gaps: pd.DataFrame,
    data_coverage: pd.DataFrame,
    convergence_diagnostics: pd.DataFrame,
    point_subsample_winners: pd.DataFrame,
    revision_subsample_winners: pd.DataFrame,
    figure_paths: list[Path],
) -> str:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    coverage_map = dict(zip(data_coverage["coverage_item"], data_coverage["value"]))
    exact_point = point_winners.loc[point_winners["timing_mode"].eq("exact")]
    pseudo_point = point_winners.loc[point_winners["timing_mode"].eq("pseudo")]
    lines = [
        "# Journal Results Draft",
        "",
        f"Generated UTC: `{now}`",
        "",
        f"Source output directory: `{source_dir}`",
        "",
        f"Report package directory: `{output_dir}`",
        "",
        "## Data Coverage",
        "",
        f"- Point forecast rows: `{coverage_map.get('point_forecast_rows', '')}`.",
        f"- Revision forecast rows: `{coverage_map.get('revision_forecast_rows', '')}`.",
        f"- Failure rows: `{coverage_map.get('failure_rows', '')}`.",
        f"- GDP release calendar rows: `{coverage_map.get('calendar_rows', '')}`.",
        "- Bias convention: `forecast_error = forecast_value - realized_value`; all bias values are mean forecast errors under this convention.",
        "- The headline 2005Q1--2024Q4 A/S/T GDP calendar is derived from ALFRED `GDPC1` vintage dates when available; fallback rows are documented in the calendar file.",
        "",
        "## Estimation Diagnostics",
        "",
        "Kalman/EM rows report `convergence_rate`, `mean_iterations`, and relative final log-likelihood improvement. Mixed-frequency rows also carry numerical guard counts in the forecast-level CSV when a finite fallback is used. For a journal version, report these diagnostics next to the headline evidence rather than treating the estimator as a black box.",
        "",
        _markdown_table(
            convergence_diagnostics.loc[
                convergence_diagnostics["model_id"].isin(
                    ["revision_dfm_kalman_em", "joint_indicator_revision_dfm_full_kalman_em"]
                )
            ].head(12),
            [
                "table",
                "model_id",
                "timing_mode",
                "checkpoint_id",
                "outcome_id",
                "convergence_rate",
                "mean_iterations",
                "median_llf_relative_last_improvement",
                "RMSE",
            ],
        ),
        "",
        "## Headline Point Forecast Winners",
        "",
        _markdown_table(
            point_winners,
            [
                "timing_mode",
                "checkpoint_id",
                "target_id",
                "best_models",
                "best_RMSE",
                "best_MAE",
                "best_bias",
                "best_n_forecasts",
            ],
        ),
        "",
        "Main reading:",
        "",
        "- The advance checkpoint should be read as a monthly-information problem: bridge/MIDAS/DFM-style monthly predictors are the relevant benchmark family before any same-quarter GDP estimate is observed.",
        "- The second and third checkpoints should be read against the no-revision benchmark. If no-revision wins, that is a substantive empirical result: official early GDP estimates are hard to improve on in point RMSE.",
        "- State-space value should be evaluated through the full evidence package: uncertainty calibration, mature-target robustness, revision-risk diagnostics, and mechanism tables, not only the winner table.",
        "",
        "## Headline Revision Forecast Winners",
        "",
        _markdown_table(
            revision_winners,
            [
                "timing_mode",
                "checkpoint_id",
                "revision_target_id",
                "best_models",
                "best_RMSE",
                "best_MAE",
                "best_bias",
                "best_sign_accuracy",
                "best_n_forecasts",
            ],
        ),
        "",
        "Revision interpretation:",
        "",
        "- No-revision is the primary benchmark for adjacent GDP revisions because many realized revisions are small and the zero-revision forecast is hard to beat.",
        "- Sign accuracy should be interpreted with thresholded diagnostics; near-zero revisions can make raw direction accuracy look weak even when magnitude forecasts are useful.",
        "",
        "## Exact Versus Pseudo Timing",
        "",
        _markdown_table(
            point_gaps,
            ["model_id", "checkpoint_id", "target_id", "exact", "pseudo", "exact_minus_pseudo_RMSE"],
        ),
        "",
        "Negative exact-minus-pseudo values mean exact event timing lowers RMSE. Positive values mean pseudo timing has lower RMSE in that cell.",
        "",
        "Revision timing gaps:",
        "",
        _markdown_table(
            revision_gaps,
            ["model_id", "checkpoint_id", "revision_target_id", "exact", "pseudo", "exact_minus_pseudo_RMSE"],
        ),
        "",
        "## Robustness Winners",
        "",
        _markdown_table(
            point_subsample_winners,
            [
                "subsample",
                "timing_mode",
                "checkpoint_id",
                "target_id",
                "best_models",
                "best_RMSE",
                "best_n_forecasts",
            ],
        ),
        "",
        "Revision robustness winners:",
        "",
        _markdown_table(
            revision_subsample_winners,
            [
                "subsample",
                "timing_mode",
                "checkpoint_id",
                "revision_target_id",
                "best_models",
                "best_RMSE",
                "best_n_forecasts",
            ],
        ),
        "",
        "## Suggested Report Claim",
        "",
        "A defensible Q1-style claim from this build is: real-time GDP nowcasting under a release ladder is best understood as a timing- and target-dependent forecast problem. Before the advance release, monthly information dominates. Before second and third releases, no-revision is a hard benchmark, so the contribution of release-ladder state-space modeling must be shown through uncertainty, revision-risk, mature-target robustness, and mechanism evidence as well as point accuracy.",
        "",
        "## Reporting Cautions",
        "",
        "- Do not mix these full state-space outputs with older frozen outputs unless the table explicitly labels the build.",
        "- If the paper claims full Kalman/EM estimation, cite the files in this package and the exact/pseudo backtest outputs, not the older factor-regression-only report.",
        "- The current generated package is traceable to forecast CSVs, but model selection should still be described as out-of-sample RMSE ranking rather than proof of universal dominance.",
        "- One S-release quarter has incomplete RTDSM target coverage in the current data, so S and DELTA_SA/DELTA_TS headline cells have 79 forecasts rather than 80.",
        "",
    ]
    if not exact_point.empty or not pseudo_point.empty:
        lines.extend(
            [
                "## Quick Narrative Anchors",
                "",
                "Exact headline winners:",
                "",
                _markdown_table(
                    exact_point,
                    ["checkpoint_id", "target_id", "best_models", "best_RMSE", "best_n_forecasts"],
                ),
                "",
                "Pseudo headline winners:",
                "",
                _markdown_table(
                    pseudo_point,
                    ["checkpoint_id", "target_id", "best_models", "best_RMSE", "best_n_forecasts"],
                ),
                "",
            ]
        )
    if figure_paths:
        lines.extend(["## Figures", ""])
        for path in figure_paths:
            lines.append(f"- `{path.relative_to(output_dir)}`")
        lines.append("")
    return "\n".join(lines)


def build_report_package(source_dir: Path, output_dir: Path) -> dict[str, Path]:
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = _load_outputs(source_dir)

    point_metrics = _sort_metrics(outputs["metrics_summary"], "target_id")
    revision_metrics = _sort_metrics(outputs["revision_metrics_summary"], "revision_target_id")
    point_winners = _winner_table(point_metrics, "target_id")
    revision_winners = _winner_table(revision_metrics, "revision_target_id")

    point_subsample_metrics = _build_subsample_metrics(outputs["forecast_results"], "target_id")
    revision_subsample_metrics = _build_subsample_metrics(outputs["revision_forecast_results"], "revision_target_id")
    point_subsample_winners = _winner_table(point_subsample_metrics, "target_id")
    revision_subsample_winners = _winner_table(revision_subsample_metrics, "revision_target_id")

    winner_stability = _winner_stability(point_subsample_winners, revision_subsample_winners)
    data_coverage = _build_data_coverage(outputs, source_dir)
    convergence_diagnostics = _build_convergence_diagnostics(point_metrics, revision_metrics)
    point_gaps = outputs["exact_pseudo_point_gaps"].copy()
    revision_gaps = outputs["exact_pseudo_revision_gaps"].copy()
    figure_paths = _write_figures(output_dir, point_metrics, revision_metrics, point_gaps)

    output_paths = {
        "headline_point_results": output_dir / "headline_point_results.csv",
        "headline_revision_results": output_dir / "headline_revision_results.csv",
        "headline_exact_vs_pseudo": output_dir / "headline_exact_vs_pseudo.csv",
        "headline_revision_exact_vs_pseudo": output_dir / "headline_revision_exact_vs_pseudo.csv",
        "point_metrics_full": output_dir / "point_metrics_full.csv",
        "revision_metrics_full": output_dir / "revision_metrics_full.csv",
        "subsample_robustness_point": output_dir / "subsample_robustness_point.csv",
        "subsample_robustness_revision": output_dir / "subsample_robustness_revision.csv",
        "subsample_robustness_point_winners": output_dir / "subsample_robustness_point_winners.csv",
        "subsample_robustness_revision_winners": output_dir / "subsample_robustness_revision_winners.csv",
        "winner_stability": output_dir / "winner_stability.csv",
        "data_coverage_summary": output_dir / "data_coverage_summary.csv",
        "convergence_diagnostics": output_dir / "convergence_diagnostics.csv",
    }
    point_winners.to_csv(output_paths["headline_point_results"], index=False)
    revision_winners.to_csv(output_paths["headline_revision_results"], index=False)
    point_gaps.to_csv(output_paths["headline_exact_vs_pseudo"], index=False)
    revision_gaps.to_csv(output_paths["headline_revision_exact_vs_pseudo"], index=False)
    point_metrics.to_csv(output_paths["point_metrics_full"], index=False)
    revision_metrics.to_csv(output_paths["revision_metrics_full"], index=False)
    point_subsample_metrics.to_csv(output_paths["subsample_robustness_point"], index=False)
    revision_subsample_metrics.to_csv(output_paths["subsample_robustness_revision"], index=False)
    point_subsample_winners.to_csv(output_paths["subsample_robustness_point_winners"], index=False)
    revision_subsample_winners.to_csv(output_paths["subsample_robustness_revision_winners"], index=False)
    winner_stability.to_csv(output_paths["winner_stability"], index=False)
    data_coverage.to_csv(output_paths["data_coverage_summary"], index=False)
    convergence_diagnostics.to_csv(output_paths["convergence_diagnostics"], index=False)

    output_paths["latex_tables"] = _write_latex_tables(output_dir, point_winners, revision_winners, point_gaps, revision_gaps)
    markdown = _generate_markdown(
        source_dir,
        output_dir,
        point_winners,
        revision_winners,
        point_gaps,
        revision_gaps,
        data_coverage,
        convergence_diagnostics,
        point_subsample_winners,
        revision_subsample_winners,
        figure_paths,
    )
    output_paths["journal_results_draft"] = output_dir / "journal_results_draft.md"
    output_paths["journal_results_draft"].write_text(markdown, encoding="utf-8")

    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "source_rows": {name: int(len(frame)) for name, frame in outputs.items()},
        "output_files": {name: str(path) for name, path in output_paths.items()},
        "figures": [str(path) for path in figure_paths],
        "bias_convention": "forecast_error = forecast_value - realized_value",
    }
    output_paths["manifest"] = output_dir / "manifest.json"
    output_paths["manifest"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return output_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build report-ready tables and figures from exact/pseudo backtest outputs.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR, help="Directory containing exact_pseudo_backtest CSVs.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for report-ready artifacts.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_paths = build_report_package(args.source_dir, args.output_dir)
    print(f"Wrote report package to {args.output_dir.resolve()}")
    for name, path in output_paths.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
