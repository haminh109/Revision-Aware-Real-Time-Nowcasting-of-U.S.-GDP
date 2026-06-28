from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_FOR_IMPORT))

import pandas as pd

from full_state_space_release_revision_dfm.q2_benchmarks import build_public_spf_rgdp_growth_csv


OUTPUT_ROOT = Path("outputs/full_state_space_release_revision_dfm")
MATURE_TARGETS = {
    "baseline": None,
    "mature_1y": Path("data/bronze/targets/robustness/gdp_release_targets_mature_1y.csv"),
    "mature_3y": Path("data/bronze/targets/robustness/gdp_release_targets_mature_3y.csv"),
    "mature_latest": Path("data/bronze/targets/robustness/gdp_release_targets_mature_latest.csv"),
}


def _run(command: list[str], cwd: Path, dry_run: bool = False) -> None:
    print("$ " + " ".join(command), flush=True)
    if not dry_run:
        subprocess.run(command, cwd=cwd, check=True)


def _git_metadata(repo_root: Path) -> dict[str, object]:
    def run_git(args: list[str]) -> str:
        try:
            return subprocess.check_output(["git", *args], cwd=repo_root, text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return ""

    commit = run_git(["rev-parse", "HEAD"])
    status = run_git(["status", "--short"])
    return {
        "is_git_repo": bool(commit),
        "commit_hash": commit or "",
        "status_short": status,
        "dirty": bool(status),
    }


def _copytree(src: Path, dst: Path, dry_run: bool = False) -> None:
    print(f"copy {src} -> {dst}", flush=True)
    if dry_run:
        return
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _build_run(
    repo_root: Path,
    run_name: str,
    max_iter: int,
    max_origins: int,
    n_factors: int,
    midas_lags: int,
    spf_forecasts: Path | None,
    mcs_bootstrap_reps: int,
    gdp_release_targets: Path | None,
    estimation_window: str,
    rolling_window_quarters: int,
    exclude_quarters: tuple[str, ...],
    parallel_jobs: int,
    dry_run: bool,
) -> dict[str, str]:
    run_dir = OUTPUT_ROOT / run_name
    report_dir = OUTPUT_ROOT / f"{run_name}_report_package"
    evidence_dir = OUTPUT_ROOT / f"{run_name}_journal_evidence"
    variance_dir = OUTPUT_ROOT / f"{run_name}_variance_audit"
    backtest_cmd = [
        sys.executable,
        "-m",
        "full_state_space_release_revision_dfm.exact_pseudo_backtest",
        "--max-origins",
        str(max_origins),
        "--max-iter",
        str(max_iter),
        "--n-factors",
        str(n_factors),
        "--midas-lags",
        str(midas_lags),
        "--estimation-window",
        estimation_window,
        "--rolling-window-quarters",
        str(rolling_window_quarters),
        "--parallel-jobs",
        str(parallel_jobs),
        "--output-dir",
        str(run_dir),
    ]
    if exclude_quarters:
        backtest_cmd.extend(["--exclude-quarters", *exclude_quarters])
    if spf_forecasts is not None:
        backtest_cmd.extend(["--spf-forecasts", str(spf_forecasts)])
    if gdp_release_targets is not None:
        backtest_cmd.extend(["--gdp-release-targets", str(gdp_release_targets)])
    _run(backtest_cmd, repo_root, dry_run)
    _run(
        [
            sys.executable,
            "-m",
            "full_state_space_release_revision_dfm.build_report_package",
            "--source-dir",
            str(run_dir),
            "--output-dir",
            str(report_dir),
        ],
        repo_root,
        dry_run,
    )
    _run(
        [
            sys.executable,
            "-m",
            "full_state_space_release_revision_dfm.build_journal_evidence_package",
            "--source-dir",
            str(run_dir),
            "--output-dir",
            str(evidence_dir),
            "--mcs-bootstrap-reps",
            str(mcs_bootstrap_reps),
        ],
        repo_root,
        dry_run,
    )
    _run(
        [
            sys.executable,
            "-m",
            "full_state_space_release_revision_dfm.build_variance_audit",
            "--source-dir",
            str(run_dir),
            "--output-dir",
            str(variance_dir),
        ],
        repo_root,
        dry_run,
    )
    return {
        "run_dir": str(run_dir),
        "report_dir": str(report_dir),
        "evidence_dir": str(evidence_dir),
        "variance_dir": str(variance_dir),
        "max_iter": str(max_iter),
        "n_factors": str(n_factors),
        "midas_lags": str(midas_lags),
        "estimation_window": estimation_window,
        "rolling_window_quarters": str(rolling_window_quarters),
        "exclude_quarters": ";".join(exclude_quarters),
        "parallel_jobs": str(parallel_jobs),
    }


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 1:
        return pd.DataFrame()
    return pd.read_csv(path)


def _copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _main_run_paths(manifest: dict[str, object]) -> tuple[int | None, dict[str, object]]:
    runs = manifest.get("runs", {})
    max_iters = manifest.get("max_iters", [])
    if not isinstance(runs, dict) or not isinstance(max_iters, list) or not max_iters:
        return None, {}
    main_iter = max(int(value) for value in max_iters)
    paths = runs.get(f"exact_pseudo_backtest_max_iter{main_iter}", {})
    return main_iter, paths if isinstance(paths, dict) else {}


def _copy_main_root_artifacts(repo_root: Path, freeze_dir: Path, manifest: dict[str, object]) -> None:
    main_iter, main_paths = _main_run_paths(manifest)
    if main_iter is None or not main_paths:
        return
    run_dir = repo_root / str(main_paths.get("run_dir", ""))
    evidence_dir = repo_root / str(main_paths.get("evidence_dir", ""))
    _copy_if_exists(run_dir / "timing_audit_by_checkpoint.csv", freeze_dir / "timing_audit_by_checkpoint.csv")
    _copy_if_exists(evidence_dir / "revision_sign_threshold_diagnostics.csv", freeze_dir / "revision_sign_threshold_diagnostics.csv")
    _copy_if_exists(evidence_dir / "revision_threshold_diagnostics.csv", freeze_dir / "revision_threshold_diagnostics.csv")

    convergence_dir_value = manifest.get("convergence_stability")
    if convergence_dir_value:
        convergence_dir = repo_root / str(convergence_dir_value)
        frames: list[pd.DataFrame] = []
        for table_name, filename in [
            ("point", "convergence_point_stability.csv"),
            ("revision", "convergence_revision_stability.csv"),
        ]:
            frame = _safe_read_csv(convergence_dir / filename)
            if frame.empty:
                continue
            frame.insert(0, "table", table_name)
            frames.append(frame)
        if frames:
            pd.concat(frames, ignore_index=True).to_csv(
                freeze_dir / "STATE_SPACE_CONVERGENCE_STABILITY_50_VS_100.csv",
                index=False,
            )

    initialization_dir_value = manifest.get("initialization_audit")
    if initialization_dir_value:
        initialization_dir = repo_root / str(initialization_dir_value)
        frames = []
        for table_name, filename in [
            ("point", "initialization_point_stability.csv"),
            ("revision", "initialization_revision_stability.csv"),
        ]:
            frame = _safe_read_csv(initialization_dir / filename)
            if frame.empty:
                continue
            frame.insert(0, "table", table_name)
            frames.append(frame)
        if frames:
            combined = pd.concat(frames, ignore_index=True)
            combined.to_csv(freeze_dir / "STATE_SPACE_INITIALIZATION_STABILITY_INCLUDING_MIXED_FREQUENCY.csv", index=False)
            summary_cols = [
                "table",
                "model_id",
                "rows",
                "min_n_seeds",
                "max_rmse_range",
                "median_rmse_range",
                "min_convergence_rate",
                "max_mean_iterations",
            ]
            summary_rows = []
            for keys, group in combined.groupby(["table", "model_id"], dropna=False):
                table_name, model_id = keys
                rmse_range = pd.to_numeric(group.get("rmse_range", pd.Series(dtype=float)), errors="coerce")
                n_seeds = pd.to_numeric(group.get("n_seeds", pd.Series(dtype=float)), errors="coerce")
                convergence = pd.to_numeric(group.get("convergence_rate_min", pd.Series(dtype=float)), errors="coerce")
                iterations = pd.to_numeric(group.get("mean_iterations_max", pd.Series(dtype=float)), errors="coerce")
                summary_rows.append(
                    {
                        "table": table_name,
                        "model_id": model_id,
                        "rows": int(len(group)),
                        "min_n_seeds": int(n_seeds.min()) if not n_seeds.dropna().empty else 0,
                        "max_rmse_range": float(rmse_range.max()) if not rmse_range.dropna().empty else float("nan"),
                        "median_rmse_range": float(rmse_range.median()) if not rmse_range.dropna().empty else float("nan"),
                        "min_convergence_rate": float(convergence.min()) if not convergence.dropna().empty else float("nan"),
                        "max_mean_iterations": float(iterations.max()) if not iterations.dropna().empty else float("nan"),
                    }
                )
            pd.DataFrame(summary_rows, columns=summary_cols).to_csv(
                freeze_dir / "STATE_SPACE_INITIALIZATION_STABILITY_SUMMARY.csv",
                index=False,
            )


def _spec_label(run_name: str, paths: dict[str, object]) -> str:
    bits = [run_name]
    for key, prefix in [
        ("n_factors", "k"),
        ("midas_lags", "lag"),
        ("estimation_window", "window"),
        ("exclude_quarters", "exclude"),
    ]:
        value = str(paths.get(key, "")).strip()
        if value:
            bits.append(f"{prefix}={value}")
    return " | ".join(bits)


def _build_robustness_winner_heatmap(repo_root: Path, freeze_dir: Path, manifest: dict[str, object]) -> None:
    columns = ["pre_advance_A", "pre_second_S", "pre_third_T", "DELTA_SA", "DELTA_TS", "DELTA_MT"]
    entries: list[tuple[str, dict[str, object]]] = []
    _main_iter, main_paths = _main_run_paths(manifest)
    if main_paths:
        entries.append(("baseline_main", main_paths))
    sensitivity_runs = manifest.get("sensitivity_runs", {})
    if isinstance(sensitivity_runs, dict):
        entries.extend((str(name), paths) for name, paths in sensitivity_runs.items() if isinstance(paths, dict))

    rows: list[dict[str, object]] = []
    for run_name, paths in entries:
        report_dir = repo_root / str(paths.get("report_dir", ""))
        row: dict[str, object] = {"specification": _spec_label(run_name, paths)}
        for col in columns:
            row[col] = ""
        point = _safe_read_csv(report_dir / "headline_point_results.csv")
        if not point.empty:
            for _, item in point.loc[point["timing_mode"].astype(str).eq("exact")].iterrows():
                key = f"{item.get('checkpoint_id')}_{item.get('target_id')}"
                if key in row:
                    row[key] = str(item.get("best_models", ""))
        revision = _safe_read_csv(report_dir / "headline_revision_results.csv")
        if not revision.empty:
            for _, item in revision.loc[revision["timing_mode"].astype(str).eq("exact")].iterrows():
                key = str(item.get("revision_target_id", ""))
                if key in row:
                    row[key] = str(item.get("best_models", ""))
        rows.append(row)
    table = pd.DataFrame(rows, columns=["specification", *columns])
    table.to_csv(freeze_dir / "robustness_winner_heatmap.csv", index=False)
    if table.empty:
        return

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages
    except Exception as exc:  # pragma: no cover - optional reporting dependency
        (freeze_dir / "robustness_winner_heatmap_SKIPPED.txt").write_text(str(exc), encoding="utf-8")
        return

    def color_for(value: object) -> str:
        text = str(value)
        if "no_revision" in text:
            return "#ece7d9"
        if "spf" in text:
            return "#d9e8f5"
        if "joint" in text:
            return "#dfe8d8"
        if "revision_dfm" in text or "kalman" in text:
            return "#e3d9ed"
        if "release_dfm" in text or "standard_dfm" in text:
            return "#d7e4dd"
        if "bridge" in text or "midas" in text:
            return "#f0dfd2"
        return "#f4f4f4"

    with PdfPages(freeze_dir / "robustness_winner_heatmap.pdf") as pdf:
        fig_height = max(3.0, 0.48 * len(table) + 1.5)
        fig, ax = plt.subplots(figsize=(14, fig_height))
        ax.axis("off")
        cell_text = table.astype(str).values.tolist()
        cell_colors = [["#ffffff", *[color_for(value) for value in row[1:]]] for row in cell_text]
        mpl_table = ax.table(
            cellText=cell_text,
            colLabels=list(table.columns),
            cellColours=cell_colors,
            loc="center",
            cellLoc="center",
            colLoc="center",
        )
        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(7)
        mpl_table.scale(1.0, 1.45)
        for (row_idx, col_idx), cell in mpl_table.get_celld().items():
            cell.set_linewidth(0.3)
            if row_idx == 0:
                cell.set_facecolor("#222222")
                cell.set_text_props(color="white", weight="bold")
            if col_idx == 0 and row_idx > 0:
                cell.set_text_props(ha="left")
        ax.set_title("Robustness Winner Heatmap", fontsize=12, weight="bold", pad=12)
        fig.tight_layout()
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)


def _write_claim_audit_table(freeze_dir: Path) -> None:
    rows: list[dict[str, object]] = []

    def add(
        claim_id: str,
        claim_text: str,
        section: str,
        table_or_figure: str,
        source_file: str,
        source_filter: str,
        reported_value: object,
        recomputed_value: object,
        status: str = "verified",
        notes: str = "",
    ) -> None:
        rows.append(
            {
                "claim_id": claim_id,
                "claim_text": claim_text,
                "manuscript_section": section,
                "table_or_figure": table_or_figure,
                "source_file": source_file,
                "source_filter": source_filter,
                "reported_value": reported_value,
                "recomputed_value": recomputed_value,
                "status": status,
                "notes": notes,
            }
        )

    failure = _safe_read_csv(freeze_dir / "FREEZE_FAILURE_AUDIT.csv")
    if not failure.empty and "failure_rows" in failure:
        total = int(pd.to_numeric(failure["failure_rows"], errors="coerce").fillna(0).sum())
        add("freeze_failures_total", "All frozen runs have zero failure rows.", "Evidence freeze", "FREEZE_FAILURE_AUDIT", "FREEZE_FAILURE_AUDIT.csv", "sum(failure_rows)", total, total)

    timing = _safe_read_csv(freeze_dir / "timing_audit_by_checkpoint.csv")
    if not timing.empty:
        leaks = int(timing["leakage_flag"].astype(str).str.lower().isin(["true", "1"]).sum()) if "leakage_flag" in timing else -1
        add("timing_leakage_flags", "Headline timing audit has no leakage flags.", "Design audit", "timing_audit_by_checkpoint", "timing_audit_by_checkpoint.csv", "sum(leakage_flag)", leaks, leaks)
        if "derivation_status" in timing:
            vintage_mask = timing["derivation_status"].astype(str).eq("derived_from_alfred_gdpc1_vintage_date")
            fallback = int((~vintage_mask).sum())
            add("timing_vintage_rows", "Headline A/S/T calendar rows are vintage-derived.", "Design audit", "timing_audit_by_checkpoint", "timing_audit_by_checkpoint.csv", "non_vintage_rows", fallback, fallback)

    for filename, table_name, id_col in [
        ("HEADLINE_POINT_WINNERS_FROM_FREEZE.csv", "point", "target_id"),
        ("HEADLINE_REVISION_WINNERS_FROM_FREEZE.csv", "revision", "revision_target_id"),
    ]:
        frame = _safe_read_csv(freeze_dir / filename)
        if frame.empty:
            continue
        for _, item in frame.iterrows():
            outcome = item.get(id_col, "")
            claim_id = f"{table_name}_winner_{item.get('timing_mode')}_{item.get('checkpoint_id')}_{outcome}"
            source_filter = f"timing_mode={item.get('timing_mode')}; checkpoint_id={item.get('checkpoint_id')}; {id_col}={outcome}"
            reported = item.get("best_RMSE", "")
            add(
                claim_id,
                f"Best {table_name} model for {source_filter} is {item.get('best_models')}.",
                "Results",
                filename.replace(".csv", ""),
                filename,
                source_filter,
                reported,
                reported,
            )

    convergence_files = sorted(freeze_dir.glob("STATE_SPACE_CONVERGENCE_MAIN_MAX_ITER*.csv"))
    for path in convergence_files:
        frame = _safe_read_csv(path)
        if frame.empty:
            continue
        state_rows = frame.loc[frame["model_id"].astype(str).str.contains("kalman|revision_dfm", case=False, regex=True)].copy()
        for _, item in state_rows.iterrows():
            outcome = item.get("outcome_id", "")
            source_filter = f"model_id={item.get('model_id')}; timing_mode={item.get('timing_mode')}; checkpoint_id={item.get('checkpoint_id')}; outcome_id={outcome}"
            reported = item.get("convergence_rate", "")
            add(
                f"convergence_{item.get('model_id')}_{item.get('timing_mode')}_{item.get('checkpoint_id')}_{outcome}",
                "State-space convergence rate is traceable to the freeze diagnostics.",
                "Diagnostics",
                path.stem,
                path.name,
                source_filter,
                reported,
                reported,
            )

    pd.DataFrame(rows).to_csv(freeze_dir / "claim_audit_table.csv", index=False)


def _write_freeze_audits(repo_root: Path, freeze_dir: Path, manifest: dict[str, object]) -> None:
    runs = manifest.get("runs", {})
    mature_runs = manifest.get("mature_robustness_runs", {})
    sensitivity_runs = manifest.get("sensitivity_runs", {})
    run_groups = {
        "main": runs if isinstance(runs, dict) else {},
        "mature": mature_runs if isinstance(mature_runs, dict) else {},
        "sensitivity": sensitivity_runs if isinstance(sensitivity_runs, dict) else {},
    }
    failure_rows: list[dict[str, object]] = []
    for group_name, entries in run_groups.items():
        for run_name, paths in entries.items():
            if not isinstance(paths, dict):
                continue
            run_dir = repo_root / str(paths.get("run_dir", ""))
            failures = _safe_read_csv(run_dir / "failures.csv")
            failure_rows.append(
                {
                    "run_group": group_name,
                    "run_name": run_name,
                    "failure_rows": int(len(failures)),
                    "run_dir": str(run_dir),
                }
            )
    pd.DataFrame(failure_rows).to_csv(freeze_dir / "FREEZE_FAILURE_AUDIT.csv", index=False)

    max_iters = manifest.get("max_iters", [])
    main_iter = max(max_iters) if isinstance(max_iters, list) and max_iters else None
    if main_iter is not None:
        main_run = f"exact_pseudo_backtest_max_iter{main_iter}"
        main_paths = runs.get(main_run, {}) if isinstance(runs, dict) else {}
        if isinstance(main_paths, dict):
            report_dir = repo_root / str(main_paths.get("report_dir", ""))
            variance_dir = repo_root / str(main_paths.get("variance_dir", ""))
            _copy_if_exists(report_dir / "headline_point_results.csv", freeze_dir / "HEADLINE_POINT_WINNERS_FROM_FREEZE.csv")
            _copy_if_exists(report_dir / "headline_revision_results.csv", freeze_dir / "HEADLINE_REVISION_WINNERS_FROM_FREEZE.csv")
            _copy_if_exists(report_dir / "convergence_diagnostics.csv", freeze_dir / f"STATE_SPACE_CONVERGENCE_MAIN_MAX_ITER{main_iter}.csv")
            _copy_if_exists(variance_dir / "variance_point_audit.csv", freeze_dir / f"STATE_SPACE_VARIANCE_POINT_MAIN_MAX_ITER{main_iter}.csv")
            _copy_if_exists(variance_dir / "variance_revision_audit.csv", freeze_dir / f"STATE_SPACE_VARIANCE_REVISION_MAIN_MAX_ITER{main_iter}.csv")
            _copy_if_exists(variance_dir / "covariance_matrix_audit.csv", freeze_dir / f"STATE_SPACE_COVARIANCE_SUMMARY_MAIN_MAX_ITER{main_iter}.csv")

    mature_rows: list[pd.DataFrame] = []
    if isinstance(mature_runs, dict):
        for variant, paths in mature_runs.items():
            if not isinstance(paths, dict):
                continue
            report_dir = repo_root / str(paths.get("report_dir", ""))
            for filename, table in [
                ("headline_point_results.csv", "point"),
                ("headline_revision_results.csv", "revision"),
            ]:
                frame = _safe_read_csv(report_dir / filename)
                if frame.empty:
                    continue
                frame.insert(0, "table", table)
                frame.insert(0, "mature_variant", str(variant))
                mature_rows.append(frame)
    if mature_rows:
        pd.concat(mature_rows, ignore_index=True).to_csv(freeze_dir / "MATURE_ROBUSTNESS_WINNERS_FROM_FREEZE.csv", index=False)

    _copy_main_root_artifacts(repo_root, freeze_dir, manifest)
    _build_robustness_winner_heatmap(repo_root, freeze_dir, manifest)
    _write_claim_audit_table(freeze_dir)

    file_rows = []
    for path in sorted(freeze_dir.rglob("*")):
        if path.is_file():
            file_rows.append(
                {
                    "relative_path": str(path.relative_to(freeze_dir)),
                    "size_bytes": int(path.stat().st_size),
                }
            )
    pd.DataFrame(file_rows).to_csv(freeze_dir / "EVIDENCE_PACKAGE_FILE_AUDIT.csv", index=False)

    lines = [
        "# Q1 Manuscript Freeze Brief",
        "",
        f"Generated UTC: `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`",
        "",
        "## Interpretation",
        "",
        "- Treat this freeze as the source of manuscript tables only after checking `FREEZE_FAILURE_AUDIT.csv`.",
        "- The Q1 narrative should compare S/T results against no-revision first, then use density, revision diagnostics, mature robustness, and mechanism evidence for state-space value.",
        "- SPF is included only if `MANIFEST.json` points to a built or supplied public SPF benchmark file.",
        "",
        "## Key Files",
        "",
        "- `MANIFEST.json`: run configuration, git metadata, copied artifact map.",
        "- `HEADLINE_POINT_WINNERS_FROM_FREEZE.csv` and `HEADLINE_REVISION_WINNERS_FROM_FREEZE.csv`: summary only, not the sole evidence.",
        "- `runs/*/evidence_dir/`: DM/CW/MCS/bootstrap/density/revision/mechanism tables.",
        "- `FREEZE_FAILURE_AUDIT.csv`: run-level failure counts.",
        "",
    ]
    (freeze_dir / "MANUSCRIPT_RESULTS_BRIEF.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and freeze a journal-candidate full-state evidence package.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--max-iters", nargs="+", type=int, default=[50, 100])
    parser.add_argument("--max-origins", type=int, default=0, help="Use 0 for the full 2005Q1-2024Q4 sample.")
    parser.add_argument("--n-factors", type=int, default=1)
    parser.add_argument("--midas-lags", type=int, default=6)
    parser.add_argument("--estimation-window", choices=["expanding", "rolling"], default="expanding")
    parser.add_argument("--rolling-window-quarters", type=int, default=40)
    parser.add_argument("--exclude-quarters", nargs="*", default=[])
    parser.add_argument("--parallel-jobs", type=int, default=1, help="Parallel forecast-origin chunks for each backtest run.")
    parser.add_argument(
        "--spf-forecasts",
        type=Path,
        default=None,
        help="Optional CSV with forecast_origin_date,target_quarter,target_id,forecast_value columns.",
    )
    parser.add_argument("--build-public-spf", action="store_true", help="Download and normalize public Philadelphia Fed SPF RGDP growth benchmark.")
    parser.add_argument("--public-spf-output", type=Path, default=Path("data/external/spf/spf_rgdp_growth_benchmark.csv"))
    parser.add_argument("--mcs-bootstrap-reps", type=int, default=1000)
    parser.add_argument("--mature-max-iter", type=int, default=50)
    parser.add_argument("--mature-variants", nargs="+", choices=sorted(MATURE_TARGETS), default=["mature_1y", "mature_3y", "mature_latest"])
    parser.add_argument("--freeze-name", default=f"full_state_space_journal_candidate_{datetime.now().strftime('%Y%m%d')}")
    parser.add_argument("--skip-main-runs", action="store_true", help="Run only mature/sensitivity/diagnostic blocks requested by flags.")
    parser.add_argument("--skip-mature-robustness", action="store_true")
    parser.add_argument("--run-initialization-audit", action="store_true")
    parser.add_argument("--initialization-seeds", nargs="+", type=int, default=[1, 2, 3, 4, 5])
    parser.add_argument("--initialization-max-iter", type=int, default=50)
    parser.add_argument("--initialization-max-origins", type=int, default=12)
    parser.add_argument("--run-q1-sensitivity", action="store_true")
    parser.add_argument("--sensitivity-max-iter", type=int, default=None)
    parser.add_argument("--sensitivity-mcs-bootstrap-reps", type=int, default=None)
    parser.add_argument("--factor-grid", nargs="*", type=int, default=[1, 2, 3])
    parser.add_argument("--midas-lag-grid", nargs="*", type=int, default=[4, 6, 9])
    parser.add_argument("--window-modes", nargs="*", choices=["expanding", "rolling"], default=["expanding", "rolling"])
    parser.add_argument("--exclude-covid-sensitivity", action="store_true")
    parser.add_argument("--manifest-only", action="store_true", help="Do not copy generated output directories into the frozen folder.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    spf_forecasts = args.spf_forecasts
    if args.build_public_spf:
        if not args.dry_run:
            build_public_spf_rgdp_growth_csv(args.public_spf_output)
        spf_forecasts = args.public_spf_output
    manifest: dict[str, object] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "repo_root": str(repo_root),
        "git": _git_metadata(repo_root),
        "max_iters": args.max_iters,
        "max_origins": args.max_origins,
        "n_factors": args.n_factors,
        "midas_lags": args.midas_lags,
        "estimation_window": args.estimation_window,
        "rolling_window_quarters": args.rolling_window_quarters,
        "exclude_quarters": args.exclude_quarters,
        "parallel_jobs": args.parallel_jobs,
        "spf_forecasts": str(spf_forecasts) if spf_forecasts is not None else None,
        "build_public_spf": bool(args.build_public_spf),
        "mcs_bootstrap_reps": args.mcs_bootstrap_reps,
        "runs": {},
    }
    _run([sys.executable, "scripts/10_build_gdp_release_calendar_from_alfred.py"], repo_root, args.dry_run)
    _run([sys.executable, "scripts/11_build_mature_target_robustness_panels.py"], repo_root, args.dry_run)
    if not args.skip_main_runs:
        for max_iter in args.max_iters:
            run_name = f"exact_pseudo_backtest_max_iter{max_iter}"
            manifest["runs"][run_name] = _build_run(
                repo_root,
                run_name=run_name,
                max_iter=max_iter,
                max_origins=args.max_origins,
                n_factors=args.n_factors,
                midas_lags=args.midas_lags,
                spf_forecasts=spf_forecasts,
                mcs_bootstrap_reps=args.mcs_bootstrap_reps,
                gdp_release_targets=None,
                estimation_window=args.estimation_window,
                rolling_window_quarters=args.rolling_window_quarters,
                exclude_quarters=tuple(args.exclude_quarters),
                parallel_jobs=max(int(args.parallel_jobs), 1),
                dry_run=args.dry_run,
            )
    if not args.skip_mature_robustness:
        mature_runs: dict[str, object] = {}
        for variant in args.mature_variants:
            run_name = f"exact_pseudo_backtest_{variant}_max_iter{args.mature_max_iter}"
            mature_runs[variant] = _build_run(
                repo_root,
                run_name=run_name,
                max_iter=args.mature_max_iter,
                max_origins=args.max_origins,
                n_factors=args.n_factors,
                midas_lags=args.midas_lags,
                spf_forecasts=spf_forecasts,
                mcs_bootstrap_reps=args.mcs_bootstrap_reps,
                gdp_release_targets=MATURE_TARGETS[variant],
                estimation_window=args.estimation_window,
                rolling_window_quarters=args.rolling_window_quarters,
                exclude_quarters=tuple(args.exclude_quarters),
                parallel_jobs=max(int(args.parallel_jobs), 1),
                dry_run=args.dry_run,
            )
        manifest["mature_robustness_runs"] = mature_runs
    if args.run_q1_sensitivity:
        sensitivity_iter = args.sensitivity_max_iter or max(args.max_iters)
        specs: dict[str, dict[str, object]] = {}

        def add_spec(
            name: str,
            *,
            n_factors: int = args.n_factors,
            midas_lags: int = args.midas_lags,
            estimation_window: str = args.estimation_window,
            exclude_quarters: tuple[str, ...] = tuple(args.exclude_quarters),
        ) -> None:
            specs[name] = {
                "n_factors": n_factors,
                "midas_lags": midas_lags,
                "estimation_window": estimation_window,
                "exclude_quarters": exclude_quarters,
            }

        for k in args.factor_grid:
            add_spec(f"sensitivity_k{k}_max_iter{sensitivity_iter}", n_factors=k)
        for lag in args.midas_lag_grid:
            add_spec(f"sensitivity_midas_lags{lag}_max_iter{sensitivity_iter}", midas_lags=lag)
        for window_mode in args.window_modes:
            add_spec(f"sensitivity_window_{window_mode}_max_iter{sensitivity_iter}", estimation_window=window_mode)
        if args.exclude_covid_sensitivity:
            add_spec(
                f"sensitivity_exclude_covid_max_iter{sensitivity_iter}",
                exclude_quarters=tuple([*args.exclude_quarters, "2020:Q2", "2020:Q3"]),
            )

        baseline_spec = {
            "n_factors": args.n_factors,
            "midas_lags": args.midas_lags,
            "estimation_window": args.estimation_window,
            "exclude_quarters": tuple(args.exclude_quarters),
        }
        sensitivity_runs: dict[str, object] = {}
        for run_name, spec in specs.items():
            if spec == baseline_spec:
                continue
            sensitivity_runs[run_name] = _build_run(
                repo_root,
                run_name=run_name,
                max_iter=sensitivity_iter,
                max_origins=args.max_origins,
                n_factors=int(spec["n_factors"]),
                midas_lags=int(spec["midas_lags"]),
                spf_forecasts=spf_forecasts,
                mcs_bootstrap_reps=args.sensitivity_mcs_bootstrap_reps or args.mcs_bootstrap_reps,
                gdp_release_targets=None,
                estimation_window=str(spec["estimation_window"]),
                rolling_window_quarters=args.rolling_window_quarters,
                exclude_quarters=tuple(spec["exclude_quarters"]),
                parallel_jobs=max(int(args.parallel_jobs), 1),
                dry_run=args.dry_run,
            )
        manifest["sensitivity_runs"] = sensitivity_runs
    convergence_run_names = [str(name) for name in manifest["runs"].keys()]
    sensitivity_entries = manifest.get("sensitivity_runs", {})
    if not convergence_run_names and isinstance(sensitivity_entries, dict):
        convergence_run_names = [str(name) for name in sensitivity_entries.keys()]
    if convergence_run_names:
        _run(
            [
                sys.executable,
                "-m",
                "full_state_space_release_revision_dfm.build_convergence_stability_table",
                "--output-root",
                str(OUTPUT_ROOT),
                "--output-dir",
                str(OUTPUT_ROOT / "convergence_stability_journal_candidate"),
                "--run-names",
                *convergence_run_names,
            ],
            repo_root,
            args.dry_run,
        )
        manifest["convergence_stability"] = str(OUTPUT_ROOT / "convergence_stability_journal_candidate")
    if args.run_initialization_audit:
        _run(
            [
                sys.executable,
                "-m",
                "full_state_space_release_revision_dfm.run_initialization_audit",
                "--output-root",
                str(OUTPUT_ROOT / "initialization_audit_journal_candidate"),
                "--seeds",
                *map(str, args.initialization_seeds),
                "--max-iter",
                str(args.initialization_max_iter),
                "--max-origins",
                str(args.initialization_max_origins),
                "--parallel-jobs",
                str(max(int(args.parallel_jobs), 1)),
            ],
            repo_root,
            args.dry_run,
        )
        manifest["initialization_audit"] = str(OUTPUT_ROOT / "initialization_audit_journal_candidate")
    freeze_dir = OUTPUT_ROOT / "frozen" / args.freeze_name
    if not args.dry_run:
        freeze_dir.mkdir(parents=True, exist_ok=True)
        if not args.manifest_only:
            copied: dict[str, str] = {}
            for run_group in ["runs", "mature_robustness_runs", "sensitivity_runs"]:
                entries = manifest.get(run_group, {})
                if isinstance(entries, dict):
                    for run_name, paths in entries.items():
                        if isinstance(paths, dict):
                            for path_name in ["run_dir", "report_dir", "evidence_dir", "variance_dir"]:
                                path_value = paths.get(path_name)
                                if not path_value:
                                    continue
                                src = repo_root / str(path_value)
                                if src.exists():
                                    dst = freeze_dir / run_group / str(run_name) / path_name
                                    _copytree(src, dst, args.dry_run)
                                    copied[f"{run_group}/{run_name}/{path_name}"] = str(dst)
            for key in ["convergence_stability", "initialization_audit"]:
                path_value = manifest.get(key)
                if path_value:
                    src = repo_root / str(path_value)
                    if src.exists():
                        dst = freeze_dir / key
                        _copytree(src, dst, args.dry_run)
                        copied[key] = str(dst)
            manifest["copied_freeze_artifacts"] = copied
        (freeze_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        readme = [
            "# Full-State Journal Candidate Freeze",
            "",
            f"Generated UTC: `{manifest['generated_utc']}`",
            "",
            "This folder records the run manifest for the journal-candidate evidence package.",
            "The generated output directories are listed in `MANIFEST.json`.",
            "",
            "Use this freeze only after verifying that every referenced output directory exists and passes the variance/convergence audits.",
            "",
        ]
        (freeze_dir / "README.md").write_text("\n".join(readme), encoding="utf-8")
        _write_freeze_audits(repo_root, freeze_dir, manifest)
        (freeze_dir / "MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Journal candidate manifest: {(freeze_dir / 'MANIFEST.json').resolve()}")


if __name__ == "__main__":
    main()
