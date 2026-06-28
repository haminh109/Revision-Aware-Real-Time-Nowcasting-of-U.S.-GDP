from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "Applied Economics Submission Package"
FIG_DIR = PKG / "figures"

HIGHLIGHT = "#2f6f8f"
SSM = "#6f8797"
NEUTRAL = "#9aa8b2"
BENCHMARK = "#6b7280"
BLACK = "#111111"


def _model_color(model: str, is_benchmark: bool = False) -> str:
    if model == "Indicator-revision SSM":
        return HIGHLIGHT
    if "SSM" in model or model == "Release DFM":
        return SSM
    if is_benchmark:
        return BENCHMARK
    return NEUTRAL


def _finish_barh(ax, xlabel: str) -> None:
    ax.axvline(0.0, color=BLACK, linewidth=0.8)
    ax.set_xlabel(xlabel)
    ax.grid(axis="x", color="#d9dee2", linewidth=0.7, alpha=0.9)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#777777")
    ax.spines["bottom"].set_color("#777777")
    ax.tick_params(axis="both", labelsize=8)


def figure_1() -> None:
    fig, ax = plt.subplots(figsize=(10.2, 3.15))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 3)
    ax.axis("off")

    xs = [0.9, 3.1, 5.25, 7.25, 9.05]
    labels = [
        "Pre-advance\ncheckpoint",
        "Advance\nestimate",
        "Second\nestimate",
        "Third\nestimate",
        "Mature\nrelease",
    ]
    sub = [
        "No current-quarter\nGDP estimate",
        r"$y_q^{A}$ observed",
        r"$y_q^{S}$ observed",
        r"$y_q^{T}$ observed",
        r"$y_q^{M}$ used for\nrobustness",
    ]
    for i, (x, label, note) in enumerate(zip(xs, labels, sub)):
        ax.scatter([x], [1.7], s=90, color=BLACK, zorder=3)
        ax.text(x, 2.22, label, ha="center", va="bottom", fontsize=10, fontweight="bold", color=BLACK)
        ax.text(x, 0.77, note, ha="center", va="top", fontsize=8.8, color=BLACK)
        if i < len(xs) - 1:
            ax.annotate(
                "",
                xy=(xs[i + 1] - 0.23, 1.7),
                xytext=(x + 0.23, 1.7),
                arrowprops={"arrowstyle": "->", "color": BLACK, "lw": 1.6},
            )
    ax.text(
        5,
        0.18,
        "Operational target is the next official release; mature releases are retained for later-revision robustness.",
        ha="center",
        va="bottom",
        fontsize=9.3,
        color=BLACK,
    )
    fig.tight_layout(pad=0.4)
    fig.savefig(FIG_DIR / "release_ladder_timeline.pdf", bbox_inches="tight")
    plt.close(fig)


def figure_2() -> None:
    data = {
        "Pre-advance": [
            ("SPF", 2.225),
            ("Release DFM", 3.066),
            ("GDP-revision SSM", 3.383),
            ("Indicator-revision SSM", 3.566),
            ("Joint-revision SSM", 3.610),
            ("Monthly MF-SSM", 3.725),
            ("Bridge", 4.874),
        ],
        "Pre-second": [
            ("No-revision", 0.570),
            ("Indicator-revision SSM", 0.591),
            ("Monthly MF-SSM", 0.595),
            ("Joint-revision SSM", 0.632),
            ("Release DFM", 0.691),
            ("GDP-revision SSM", 0.695),
            ("SPF", 2.337),
        ],
        "Pre-third": [
            ("No-revision", 0.362),
            ("Monthly MF-SSM", 0.369),
            ("Indicator-revision SSM", 0.371),
            ("Joint-revision SSM", 0.372),
            ("GDP-revision SSM", 0.373),
            ("Release DFM", 0.445),
            ("SPF", 2.380),
        ],
    }
    fig, axes = plt.subplots(1, 3, figsize=(12.5, 5.0), sharex=False)
    for ax, (checkpoint, rows) in zip(axes, data.items()):
        best = min(v for _, v in rows)
        labels = [m for m, _ in rows]
        gaps = [v - best for _, v in rows]
        colors = [_model_color(m, abs(v - best) < 1e-12) for m, v in rows]
        y = np.arange(len(labels))
        ax.barh(y, gaps, color=colors, edgecolor="white", linewidth=0.6)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.invert_yaxis()
        ax.set_title(checkpoint, fontsize=10.5, fontweight="bold")
        _finish_barh(ax, "RMSE gap")
    fig.suptitle("Exact-timing point RMSE gaps relative to the release-stage winner", fontsize=12, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(FIG_DIR / "point_rmse_gap_to_benchmark.pdf", bbox_inches="tight")
    plt.close(fig)


def figure_3() -> None:
    data = {
        r"$y^A$ density": [
            ("SPF", 1.066),
            ("Release DFM", 1.371),
            ("GDP-revision SSM", 1.435),
        ],
        r"$y^S$ density": [
            ("No-revision", 0.300),
            ("Monthly MF-SSM", 0.316),
            ("Indicator-revision SSM", 0.326),
        ],
        r"$y^T$ density": [
            ("Indicator-revision SSM", 0.187),
            ("Joint-revision SSM", 0.188),
            ("GDP-revision SSM", 0.188),
            ("No-revision", 0.198),
        ],
    }
    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.4), sharex=False)
    for ax, (target, rows) in zip(axes, data.items()):
        if "y^A" in target:
            benchmark = 1.066
        elif "y^S" in target:
            benchmark = 0.300
        else:
            benchmark = 0.198
        labels = [m for m, _ in rows]
        gaps = [v - benchmark for _, v in rows]
        colors = [_model_color(m, abs(v - benchmark) < 1e-12) for m, v in rows]
        y = np.arange(len(labels))
        ax.barh(y, gaps, color=colors, edgecolor="white", linewidth=0.6)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.invert_yaxis()
        ax.set_title(target, fontsize=10.5, fontweight="bold")
        _finish_barh(ax, "CRPS gap")
    fig.suptitle("Point-density CRPS gaps relative to the release-stage benchmark", fontsize=12, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(FIG_DIR / "density_crps_gap_to_benchmark.pdf", bbox_inches="tight")
    plt.close(fig)


def figure_4() -> None:
    data = {
        r"$\Delta^{S,A}$": [
            ("No-revision", 0.300),
            ("Indicator-revision SSM", 0.308),
            ("Monthly MF-SSM", 0.327),
        ],
        r"$\Delta^{T,S}$": [
            ("GDP-revision SSM", 0.184),
            ("Joint-revision SSM", 0.184),
            ("Indicator-revision SSM", 0.184),
            ("No-revision", 0.197),
        ],
        r"$\Delta^{M,T}$": [
            ("No-revision", 0.752),
            ("Release DFM", 0.760),
            ("Indicator-revision SSM", 0.803),
        ],
    }
    benchmarks = {r"$\Delta^{S,A}$": 0.300, r"$\Delta^{T,S}$": 0.197, r"$\Delta^{M,T}$": 0.752}
    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.4), sharex=False)
    for ax, (target, rows) in zip(axes, data.items()):
        benchmark = benchmarks[target]
        labels = [m for m, _ in rows]
        gaps = [v - benchmark for _, v in rows]
        colors = [_model_color(m, abs(v - benchmark) < 1e-12) for m, v in rows]
        y = np.arange(len(labels))
        ax.barh(y, gaps, color=colors, edgecolor="white", linewidth=0.6)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.invert_yaxis()
        ax.set_title(target, fontsize=10.5, fontweight="bold")
        _finish_barh(ax, "CRPS gap")
    fig.suptitle("Adjacent-revision density CRPS gaps relative to no-revision", fontsize=12, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(FIG_DIR / "revision_density_crps_gap_to_no_revision.pdf", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "axes.titlesize": 10.5,
            "axes.labelsize": 9,
        }
    )
    figure_1()
    figure_2()
    figure_3()
    figure_4()


if __name__ == "__main__":
    main()
