from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_TARGETS = REPO_ROOT / "data/bronze/targets/gdp_release_targets.csv"
COMPLETE_VINTAGES = REPO_ROOT / "data/bronze/targets/gdp_complete_vintages_long.csv"
OUTPUT_DIR = REPO_ROOT / "data/bronze/targets/robustness"


def _quarter_to_period(label: str) -> pd.Period:
    year, quarter = str(label).split(":Q")
    return pd.Period(year=int(year), quarter=int(quarter), freq="Q")


def _period_to_label(period: pd.Period) -> str:
    return f"{period.year}:Q{period.quarter}"


def _load_release_base() -> pd.DataFrame:
    raw = pd.read_csv(RELEASE_TARGETS)
    base = raw.loc[raw["release_stage"].isin(["first", "second", "third"])].copy()
    return base


def _complete_vintage_growth() -> pd.DataFrame:
    raw = pd.read_csv(COMPLETE_VINTAGES)
    required = {"target_quarter", "vintage_period", "value"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(f"Complete-vintage file is missing columns: {sorted(missing)}")
    frame = raw.loc[:, ["target_quarter", "vintage_period", "value"]].copy()
    frame["target_period"] = frame["target_quarter"].map(_quarter_to_period)
    frame["vintage_period_obj"] = frame["vintage_period"].map(_quarter_to_period)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.loc[np.isfinite(frame["value"]) & (frame["value"] > 0)].copy()
    rows: list[pd.DataFrame] = []
    for vintage_period, group in frame.groupby("vintage_period_obj", dropna=False):
        ordered = group.sort_values("target_period").copy()
        ordered["previous_value"] = ordered["value"].shift(1)
        ordered["growth_value"] = 400.0 * (np.log(ordered["value"]) - np.log(ordered["previous_value"]))
        ordered = ordered.loc[np.isfinite(ordered["growth_value"])].copy()
        ordered["vintage_period_obj"] = vintage_period
        rows.append(ordered)
    if not rows:
        return pd.DataFrame(columns=["target_quarter", "target_period", "vintage_period", "vintage_period_obj", "growth_value"])
    return pd.concat(rows, ignore_index=True)


def _select_mature_rows(growth: pd.DataFrame, maturity_rule: str, lag_quarters: int | None) -> pd.DataFrame:
    selected_rows: list[dict[str, object]] = []
    for target_period, group in growth.groupby("target_period", dropna=False):
        ordered = group.sort_values("vintage_period_obj")
        if maturity_rule == "latest":
            chosen = ordered.iloc[-1:] if not ordered.empty else ordered
        else:
            if lag_quarters is None:
                raise ValueError("lag_quarters is required unless maturity_rule='latest'")
            cutoff = target_period + lag_quarters
            chosen = ordered.loc[ordered["vintage_period_obj"] >= cutoff].head(1)
        if chosen.empty:
            continue
        row = chosen.iloc[0]
        selected_rows.append(
            {
                "source_family": "RTDSM",
                "source_dataset": f"ROUTPUTQvQd_{maturity_rule}",
                "source_file": str(COMPLETE_VINTAGES.relative_to(REPO_ROOT)),
                "source_sheet": "ROUTPUT",
                "target_variable_id": "ROUTPUT",
                "target_description": f"Real GNP/GDP mature target robustness ({maturity_rule})",
                "source_measure": "qoq_annualized_percent_from_complete_vintage_levels",
                "source_measure_label": "Q/Q Growth (Annual Rate, Percentage Points)",
                "source_last_updated": "",
                "target_quarter": _period_to_label(target_period),
                "target_year": int(target_period.year),
                "target_quarter_number": int(target_period.quarter),
                "release_stage": "mature",
                "release_stage_order": 4,
                "release_date": "",
                "release_date_status": "quarter_vintage_period_only",
                "value": float(row["growth_value"]),
                "notes": (
                    f"Mature robustness target selected using rule={maturity_rule}; "
                    f"selected vintage_period={_period_to_label(row['vintage_period_obj'])}. "
                    "Complete-vintage workbook exposes quarter-coded vintage periods, not exact daily release dates."
                ),
            }
        )
    return pd.DataFrame(selected_rows)


def _write_panel(base: pd.DataFrame, mature: pd.DataFrame, name: str) -> Path:
    output = pd.concat([base, mature], ignore_index=True, sort=False)
    output["release_stage_order"] = pd.to_numeric(output["release_stage_order"], errors="coerce")
    output["_target_period"] = output["target_quarter"].map(_quarter_to_period)
    output = output.sort_values(["_target_period", "release_stage_order"]).drop(columns=["_target_period"])
    path = OUTPUT_DIR / f"gdp_release_targets_{name}.csv"
    output.to_csv(path, index=False)
    return path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    base = _load_release_base()
    growth = _complete_vintage_growth()
    rules = {
        "mature_1y": ("mature_1y", 4),
        "mature_3y": ("mature_3y", 12),
        "mature_latest": ("latest", None),
    }
    outputs = {}
    for output_name, (rule_name, lag) in rules.items():
        mature = _select_mature_rows(growth, rule_name, lag)
        outputs[output_name] = str(_write_panel(base, mature, output_name))
    metadata = {
        "created_by": "scripts/11_build_mature_target_robustness_panels.py",
        "release_targets_source": str(RELEASE_TARGETS),
        "complete_vintages_source": str(COMPLETE_VINTAGES),
        "outputs": outputs,
        "rules": {
            "mature_1y": "first complete-vintage GDP growth estimate at least 4 quarters after target quarter",
            "mature_3y": "first complete-vintage GDP growth estimate at least 12 quarters after target quarter",
            "mature_latest": "latest complete-vintage GDP growth estimate available in the complete-vintage workbook",
        },
        "important_note": (
            "These robustness panels derive mature targets from complete-vintage level-like RTDSM values. "
            "They should be used as mature-target sensitivity checks, not as replacements for official A/S/T release-stage targets."
        ),
    }
    metadata_path = OUTPUT_DIR / "mature_target_robustness_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"Wrote mature-target robustness panels to {OUTPUT_DIR}")
    for name, path in outputs.items():
        print(f"- {name}: {path}")
    print(f"- metadata: {metadata_path}")


if __name__ == "__main__":
    main()
