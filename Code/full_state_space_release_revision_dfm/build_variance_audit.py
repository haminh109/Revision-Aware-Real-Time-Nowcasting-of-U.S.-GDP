from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_SOURCE_DIR = Path("outputs/full_state_space_release_revision_dfm/exact_pseudo_backtest")
DEFAULT_OUTPUT_DIR = Path("outputs/full_state_space_release_revision_dfm/variance_audit")
MIN_VARIANCE = 1e-10


def _safe_read(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 1:
        return pd.DataFrame()
    return pd.read_csv(path)


def _forecast_audit(frame: pd.DataFrame, outcome_col: str) -> pd.DataFrame:
    if frame.empty or "forecast_variance" not in frame.columns:
        return pd.DataFrame()
    work = frame.copy()
    work["forecast_variance"] = pd.to_numeric(work["forecast_variance"], errors="coerce")
    work["forecast_sd"] = pd.to_numeric(work["forecast_sd"], errors="coerce")
    work["forecast_error"] = pd.to_numeric(work["forecast_error"], errors="coerce")
    work["forecast_value"] = pd.to_numeric(work["forecast_value"], errors="coerce")
    work["realized_value"] = pd.to_numeric(work["realized_value"], errors="coerce")
    z_68 = 1.0
    z_90 = 1.6448536269514722
    work["positive_variance"] = work["forecast_variance"] > MIN_VARIANCE
    work["finite_sd"] = np.isfinite(work["forecast_sd"])
    work["covered_68"] = (
        work["finite_sd"]
        & (work["realized_value"] >= work["forecast_value"] - z_68 * work["forecast_sd"])
        & (work["realized_value"] <= work["forecast_value"] + z_68 * work["forecast_sd"])
    )
    work["covered_90"] = (
        work["finite_sd"]
        & (work["realized_value"] >= work["forecast_value"] - z_90 * work["forecast_sd"])
        & (work["realized_value"] <= work["forecast_value"] + z_90 * work["forecast_sd"])
    )
    rows: list[dict[str, object]] = []
    group_cols = ["model_id", "timing_mode", "checkpoint_id", outcome_col]
    for keys, group in work.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        errors = group["forecast_error"].dropna()
        sd_values = group.loc[group["finite_sd"], "forecast_sd"]
        rmse = float(np.sqrt(np.mean(errors**2))) if len(errors) else np.nan
        empirical_sd = float(errors.std(ddof=1)) if len(errors) > 1 else np.nan
        mean_sd = float(sd_values.mean()) if len(sd_values) else np.nan
        rows.append(
            {
                **key_values,
                "n_forecasts": int(len(group)),
                "n_variance": int(group["forecast_variance"].notna().sum()),
                "share_positive_variance": float(group["positive_variance"].mean()),
                "share_finite_sd": float(group["finite_sd"].mean()),
                "min_forecast_variance": float(group["forecast_variance"].min(skipna=True)) if group["forecast_variance"].notna().any() else np.nan,
                "median_forecast_variance": float(group["forecast_variance"].median(skipna=True)) if group["forecast_variance"].notna().any() else np.nan,
                "max_forecast_variance": float(group["forecast_variance"].max(skipna=True)) if group["forecast_variance"].notna().any() else np.nan,
                "mean_forecast_sd": mean_sd,
                "empirical_error_sd": empirical_sd,
                "RMSE": rmse,
                "mean_sd_to_rmse": float(mean_sd / rmse) if np.isfinite(mean_sd) and np.isfinite(rmse) and rmse > 0 else np.nan,
                "mean_sd_to_empirical_error_sd": float(mean_sd / empirical_sd)
                if np.isfinite(mean_sd) and np.isfinite(empirical_sd) and empirical_sd > 0
                else np.nan,
                "coverage_68": float(group.loc[group["finite_sd"], "covered_68"].mean()) if group["finite_sd"].any() else np.nan,
                "coverage_90": float(group.loc[group["finite_sd"], "covered_90"].mean()) if group["finite_sd"].any() else np.nan,
                "model_implied_share": float(group["density_source"].astype(str).eq("model_implied_state_space").mean())
                if "density_source" in group.columns
                else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _covariance_matrix_audit(covariance_records: pd.DataFrame) -> pd.DataFrame:
    if covariance_records.empty:
        return pd.DataFrame()
    work = covariance_records.copy()
    work["matrix_min_eigenvalue"] = pd.to_numeric(work["matrix_min_eigenvalue"], errors="coerce")
    work["matrix_max_eigenvalue"] = pd.to_numeric(work["matrix_max_eigenvalue"], errors="coerce")
    work["matrix_max_asymmetry"] = pd.to_numeric(work["matrix_max_asymmetry"], errors="coerce")
    key_cols = ["model_id", "timing_mode", "forecast_origin", "checkpoint_id", "target_quarter"]
    matrix_level = work.drop_duplicates(key_cols).copy()
    matrix_level["psd_flag_numeric"] = matrix_level["matrix_psd_flag"].astype(str).str.lower().isin(["true", "1"])
    rows = []
    group_cols = ["model_id", "timing_mode", "checkpoint_id"]
    for keys, group in matrix_level.groupby(group_cols, dropna=False):
        key_values = dict(zip(group_cols, keys))
        rows.append(
            {
                **key_values,
                "n_matrices": int(len(group)),
                "share_psd": float(group["psd_flag_numeric"].mean()),
                "max_asymmetry": float(group["matrix_max_asymmetry"].max(skipna=True)),
                "min_eigenvalue_min": float(group["matrix_min_eigenvalue"].min(skipna=True)),
                "min_eigenvalue_median": float(group["matrix_min_eigenvalue"].median(skipna=True)),
                "max_eigenvalue_median": float(group["matrix_max_eigenvalue"].median(skipna=True)),
            }
        )
    return pd.DataFrame(rows)


def _markdown_table(frame: pd.DataFrame, max_rows: int = 40) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame.head(max_rows).copy()
    for col in display.columns:
        if pd.api.types.is_numeric_dtype(display[col]):
            display[col] = display[col].map(lambda value: "" if not np.isfinite(value) else f"{value:.4f}")
        else:
            display[col] = display[col].astype(str)
    header = "| " + " | ".join(display.columns) + " |"
    sep = "| " + " | ".join(["---"] * len(display.columns)) + " |"
    rows = ["| " + " | ".join(row) + " |" for row in display.astype(str).to_numpy()]
    return "\n".join([header, sep, *rows])


def _write_summary(output_dir: Path, point_audit: pd.DataFrame, revision_audit: pd.DataFrame, covariance_audit: pd.DataFrame) -> Path:
    lines = [
        "# Variance Audit",
        "",
        f"Generated UTC: `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`",
        "",
        "## Interpretation",
        "",
        "- `share_positive_variance` and `share_finite_sd` should be 1.0 for state-space density rows.",
        "- `mean_sd_to_rmse` far below 1 indicates under-dispersed predictive intervals; far above 1 indicates overly wide intervals.",
        "- `share_psd` should be 1.0 for serialized release covariance matrices.",
        "",
        "## Point Forecast Audit",
        "",
        _markdown_table(point_audit),
        "",
        "## Revision Forecast Audit",
        "",
        _markdown_table(revision_audit),
        "",
        "## Covariance Matrix Audit",
        "",
        _markdown_table(covariance_audit),
        "",
    ]
    path = output_dir / "variance_audit_summary.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def build_variance_audit(source_dir: Path, output_dir: Path) -> dict[str, Path]:
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    point = _safe_read(source_dir / "forecast_results.csv")
    revision = _safe_read(source_dir / "revision_forecast_results.csv")
    covariance_records = _safe_read(source_dir / "state_space_covariance_records.csv")
    point_audit = _forecast_audit(point, "target_id")
    revision_audit = _forecast_audit(revision, "revision_target_id")
    covariance_audit = _covariance_matrix_audit(covariance_records)
    paths = {
        "variance_point_audit": output_dir / "variance_point_audit.csv",
        "variance_revision_audit": output_dir / "variance_revision_audit.csv",
        "covariance_matrix_audit": output_dir / "covariance_matrix_audit.csv",
    }
    point_audit.to_csv(paths["variance_point_audit"], index=False)
    revision_audit.to_csv(paths["variance_revision_audit"], index=False)
    covariance_audit.to_csv(paths["covariance_matrix_audit"], index=False)
    paths["summary"] = _write_summary(output_dir, point_audit, revision_audit, covariance_audit)
    paths["manifest"] = output_dir / "manifest.json"
    paths["manifest"].write_text(
        json.dumps(
            {
                "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source_dir": str(source_dir),
                "output_dir": str(output_dir),
                "output_files": {name: str(path) for name, path in paths.items()},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit state-space predictive variances and covariance matrices.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = build_variance_audit(args.source_dir, args.output_dir)
    print(f"Wrote variance audit to {args.output_dir.resolve()}")
    for name, path in paths.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
