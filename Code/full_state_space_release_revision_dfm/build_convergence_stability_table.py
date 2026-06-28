from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_OUTPUT_ROOT = Path("outputs/full_state_space_release_revision_dfm")
STATE_SPACE_MODELS = {
    "monthly_mixed_frequency_kalman_em",
    "revision_dfm_kalman_em",
    "indicator_revision_only_dfm_kalman_em",
    "joint_indicator_revision_dfm_full_kalman_em",
}


def _infer_run_label(path: Path) -> str:
    return path.name


def _infer_max_iter(path: Path, metrics: pd.DataFrame) -> int | float:
    match = re.search(r"max_iter(\d+)", path.name)
    if match:
        return int(match.group(1))
    state_rows = metrics.loc[metrics["model_id"].isin(STATE_SPACE_MODELS)]
    if not state_rows.empty and "mean_iterations" in state_rows.columns:
        value = pd.to_numeric(state_rows["mean_iterations"], errors="coerce").max()
        return float(value) if np.isfinite(value) else np.nan
    return np.nan


def _load_metrics(run_dir: Path, filename: str) -> pd.DataFrame:
    path = run_dir / filename
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    frame["run_label"] = _infer_run_label(run_dir)
    frame["source_dir"] = str(run_dir)
    frame["inferred_max_iter"] = _infer_max_iter(run_dir, frame)
    return frame


def _collect_runs(output_root: Path, run_names: set[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    point_frames: list[pd.DataFrame] = []
    revision_frames: list[pd.DataFrame] = []
    for run_dir in sorted(output_root.glob("*")):
        if not run_dir.is_dir():
            continue
        if run_names is not None and run_dir.name not in run_names:
            continue
        point = _load_metrics(run_dir, "metrics_summary.csv")
        revision = _load_metrics(run_dir, "revision_metrics_summary.csv")
        if not point.empty:
            point_frames.append(point)
        if not revision.empty:
            revision_frames.append(revision)
    point_all = pd.concat(point_frames, ignore_index=True) if point_frames else pd.DataFrame()
    revision_all = pd.concat(revision_frames, ignore_index=True) if revision_frames else pd.DataFrame()
    return point_all, revision_all


def _stability_summary(metrics: pd.DataFrame, id_col: str) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    frame = metrics.loc[metrics["model_id"].isin(STATE_SPACE_MODELS)].copy()
    if frame.empty:
        return frame
    group_cols = ["model_id", "timing_mode", "checkpoint_id", id_col]
    rows = []
    for keys, group in frame.groupby(group_cols, dropna=False):
        row = dict(zip(group_cols, keys))
        ordered = group.sort_values("inferred_max_iter")
        rmse = pd.to_numeric(ordered["RMSE"], errors="coerce")
        convergence = pd.to_numeric(ordered.get("convergence_rate", np.nan), errors="coerce")
        rel_llf = pd.to_numeric(ordered.get("median_llf_relative_last_improvement", np.nan), errors="coerce")
        row.update(
            {
                "n_runs": int(len(ordered)),
                "run_labels": ";".join(ordered["run_label"].astype(str).tolist()),
                "max_iters": ";".join(ordered["inferred_max_iter"].astype(str).tolist()),
                "rmse_min": float(rmse.min()),
                "rmse_max": float(rmse.max()),
                "rmse_range": float(rmse.max() - rmse.min()),
                "last_run_rmse": float(rmse.iloc[-1]) if len(rmse) else np.nan,
                "last_run_convergence_rate": float(convergence.iloc[-1]) if len(convergence) else np.nan,
                "last_run_median_relative_llf_improvement": float(rel_llf.iloc[-1]) if len(rel_llf) else np.nan,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def build_convergence_stability_table(output_root: Path, output_dir: Path, run_names: list[str] | None = None) -> dict[str, Path]:
    output_root = output_root.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    point, revision = _collect_runs(output_root, set(run_names) if run_names else None)
    point_summary = _stability_summary(point, "target_id")
    revision_summary = _stability_summary(revision, "revision_target_id")
    paths = {
        "convergence_point_runs": output_dir / "convergence_point_runs.csv",
        "convergence_revision_runs": output_dir / "convergence_revision_runs.csv",
        "convergence_point_stability": output_dir / "convergence_point_stability.csv",
        "convergence_revision_stability": output_dir / "convergence_revision_stability.csv",
    }
    point.to_csv(paths["convergence_point_runs"], index=False)
    revision.to_csv(paths["convergence_revision_runs"], index=False)
    point_summary.to_csv(paths["convergence_point_stability"], index=False)
    revision_summary.to_csv(paths["convergence_revision_stability"], index=False)
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect convergence and RMSE stability diagnostics across backtest runs.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT / "convergence_stability")
    parser.add_argument("--run-names", nargs="*", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = build_convergence_stability_table(args.output_root, args.output_dir, args.run_names)
    print(f"Wrote convergence stability tables to {args.output_dir.resolve()}")
    for name, path in paths.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
