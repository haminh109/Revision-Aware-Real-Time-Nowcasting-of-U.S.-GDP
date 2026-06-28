from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.exact_pseudo_backtest import (
    DEFAULT_SERIES,
    ExactPseudoBacktestConfig,
    run_exact_pseudo_backtest,
    write_outputs,
)


DEFAULT_OUTPUT_ROOT = Path("outputs/full_state_space_release_revision_dfm/initialization_audit")
STATE_SPACE_MODELS = {
    "monthly_mixed_frequency_kalman_em",
    "revision_dfm_kalman_em",
    "indicator_revision_only_dfm_kalman_em",
    "joint_indicator_revision_dfm_full_kalman_em",
}


def _load_metrics(run_dir: Path, filename: str, seed: int) -> pd.DataFrame:
    path = run_dir / filename
    if not path.exists() or path.stat().st_size <= 1:
        return pd.DataFrame()
    frame = pd.read_csv(path)
    frame["initialization_seed"] = seed
    frame["source_dir"] = str(run_dir)
    return frame


def _collect_metrics(output_root: Path, seeds: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    point_frames: list[pd.DataFrame] = []
    revision_frames: list[pd.DataFrame] = []
    for seed in seeds:
        run_dir = output_root / f"seed_{seed}"
        point = _load_metrics(run_dir, "metrics_summary.csv", seed)
        revision = _load_metrics(run_dir, "revision_metrics_summary.csv", seed)
        if not point.empty:
            point_frames.append(point)
        if not revision.empty:
            revision_frames.append(revision)
    point_all = pd.concat(point_frames, ignore_index=True) if point_frames else pd.DataFrame()
    revision_all = pd.concat(revision_frames, ignore_index=True) if revision_frames else pd.DataFrame()
    return point_all, revision_all


def _stability(metrics: pd.DataFrame, id_col: str) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    frame = metrics.loc[metrics["model_id"].isin(STATE_SPACE_MODELS)].copy()
    if frame.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in frame.groupby(["model_id", "timing_mode", "checkpoint_id", id_col], dropna=False):
        key_values = dict(zip(["model_id", "timing_mode", "checkpoint_id", id_col], keys))
        rmse = pd.to_numeric(group["RMSE"], errors="coerce")
        convergence = pd.to_numeric(group.get("convergence_rate", np.nan), errors="coerce")
        llf_improvement = pd.to_numeric(group.get("median_llf_relative_last_improvement", np.nan), errors="coerce")
        rows.append(
            {
                **key_values,
                "n_seeds": int(group["initialization_seed"].nunique()),
                "seeds": ";".join(map(str, sorted(group["initialization_seed"].unique()))),
                "rmse_min": float(rmse.min(skipna=True)),
                "rmse_median": float(rmse.median(skipna=True)),
                "rmse_max": float(rmse.max(skipna=True)),
                "rmse_range": float(rmse.max(skipna=True) - rmse.min(skipna=True)),
                "convergence_rate_min": float(convergence.min(skipna=True)),
                "convergence_rate_median": float(convergence.median(skipna=True)),
                "convergence_rate_max": float(convergence.max(skipna=True)),
                "median_relative_llf_improvement_max": float(llf_improvement.max(skipna=True)),
            }
        )
    return pd.DataFrame(rows)


def run_initialization_audit(
    repo_root: Path,
    output_root: Path,
    seeds: list[int],
    max_iter: int,
    max_origins: int,
    jitter: float,
    eval_start: str,
    eval_end: str,
    tolerance: float,
    gdp_release_targets: Path | None,
    parallel_jobs: int,
) -> dict[str, Path]:
    repo_root = repo_root.resolve()
    output_root = (repo_root / output_root).resolve() if not output_root.is_absolute() else output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    for seed in seeds:
        run_dir = output_root / f"seed_{seed}"
        config = ExactPseudoBacktestConfig(
            repo_root=repo_root,
            output_dir=run_dir,
            series_ids=tuple(DEFAULT_SERIES),
            eval_start=eval_start,
            eval_end=eval_end,
            max_origins=max_origins,
            max_iter=max_iter,
            tolerance=tolerance,
            initialization_seed=seed,
            initialization_jitter=jitter,
            gdp_release_targets_path=gdp_release_targets,
            parallel_jobs=max(int(parallel_jobs), 1),
        )
        outputs = run_exact_pseudo_backtest(config)
        write_outputs(outputs, config)
    point, revision = _collect_metrics(output_root, seeds)
    point_stability = _stability(point, "target_id")
    revision_stability = _stability(revision, "revision_target_id")
    paths = {
        "initialization_point_runs": output_root / "initialization_point_runs.csv",
        "initialization_revision_runs": output_root / "initialization_revision_runs.csv",
        "initialization_point_stability": output_root / "initialization_point_stability.csv",
        "initialization_revision_stability": output_root / "initialization_revision_stability.csv",
    }
    point.to_csv(paths["initialization_point_runs"], index=False)
    revision.to_csv(paths["initialization_revision_runs"], index=False)
    point_stability.to_csv(paths["initialization_point_stability"], index=False)
    revision_stability.to_csv(paths["initialization_revision_stability"], index=False)
    paths["manifest"] = output_root / "manifest.json"
    paths["manifest"].write_text(
        json.dumps(
            {
                "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "repo_root": str(repo_root),
                "output_root": str(output_root),
                "seeds": seeds,
                "max_iter": max_iter,
                "max_origins": max_origins,
                "initialization_jitter": jitter,
                "eval_start": eval_start,
                "eval_end": eval_end,
                "gdp_release_targets": str(gdp_release_targets) if gdp_release_targets else "",
                "parallel_jobs": int(parallel_jobs),
                "output_files": {name: str(path) for name, path in paths.items()},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run multi-initialization stability diagnostics for Kalman/EM backtests.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--seeds", nargs="+", type=int, default=[1, 2, 3, 4, 5])
    parser.add_argument("--max-iter", type=int, default=50)
    parser.add_argument("--max-origins", type=int, default=8, help="Use 0 for the full sample.")
    parser.add_argument("--initialization-jitter", type=float, default=0.02)
    parser.add_argument("--eval-start", default="2005:Q1")
    parser.add_argument("--eval-end", default="2024:Q4")
    parser.add_argument("--tolerance", type=float, default=1e-4)
    parser.add_argument("--gdp-release-targets", type=Path, default=None)
    parser.add_argument("--parallel-jobs", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = run_initialization_audit(
        repo_root=args.repo_root,
        output_root=args.output_root,
        seeds=args.seeds,
        max_iter=args.max_iter,
        max_origins=args.max_origins,
        jitter=args.initialization_jitter,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        tolerance=args.tolerance,
        gdp_release_targets=args.gdp_release_targets,
        parallel_jobs=args.parallel_jobs,
    )
    print(f"Wrote initialization audit to {args.output_root.resolve()}")
    for name, path in paths.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
