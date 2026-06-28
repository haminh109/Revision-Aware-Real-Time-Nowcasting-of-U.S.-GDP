from __future__ import annotations

from pathlib import Path

import pandas as pd


RELEASE_STAGE_MAP = {
    "first": "A",
    "advance": "A",
    "second": "S",
    "third": "T",
    "most_recent": "M",
    "mature": "M",
}


def load_gdp_release_panel(
    path: str | Path,
    *,
    include_most_recent_as_m: bool = True,
) -> pd.DataFrame:
    """Load RTDSM release-stage targets as a quarterly A/S/T/M panel."""

    raw = pd.read_csv(path)
    required = {"target_quarter", "release_stage", "value"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(f"GDP release file is missing columns: {sorted(missing)}")
    frame = raw.copy()
    frame["release_stage"] = frame["release_stage"].astype(str).str.lower()
    frame["release_id"] = frame["release_stage"].map(RELEASE_STAGE_MAP)
    if not include_most_recent_as_m:
        frame = frame[frame["release_id"] != "M"]
    frame = frame[frame["release_id"].isin(["A", "S", "T", "M"])]
    panel = frame.pivot_table(
        index="target_quarter",
        columns="release_id",
        values="value",
        aggfunc="last",
    ).sort_index()
    for col in ["A", "S", "T", "M"]:
        if col not in panel:
            panel[col] = pd.NA
    return panel[["A", "S", "T", "M"]].astype(float)


def load_alfred_first_mature_monthly_panels(
    path: str | Path,
    *,
    series_ids: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load ALFRED first-vintage and latest-vintage monthly indicator panels.

    This function constructs observation-level first and mature panels from all
    vintages in `alfred_monthly_long.csv`. It is useful for estimating the joint
    indicator-revision model. It is not a replacement for origin-specific
    real-time snapshots in a forecast backtest.
    """

    raw = pd.read_csv(path, parse_dates=["observation_date", "realtime_start"], low_memory=False)
    required = {"series_id", "series_frequency", "observation_date", "realtime_start", "value_numeric"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(f"ALFRED file is missing columns: {sorted(missing)}")
    frame = raw.loc[raw["series_frequency"].eq("monthly")].copy()
    frame = frame[pd.to_numeric(frame["value_numeric"], errors="coerce").notna()]
    frame["value_numeric"] = pd.to_numeric(frame["value_numeric"], errors="coerce")
    if series_ids is not None:
        frame = frame[frame["series_id"].isin(series_ids)]
    if start is not None:
        frame = frame[frame["observation_date"] >= pd.Timestamp(start)]
    if end is not None:
        frame = frame[frame["observation_date"] <= pd.Timestamp(end)]
    frame = frame.sort_values(["series_id", "observation_date", "realtime_start"])
    first = frame.groupby(["series_id", "observation_date"], as_index=False).first()
    mature = frame.groupby(["series_id", "observation_date"], as_index=False).last()
    first_panel = first.pivot(index="observation_date", columns="series_id", values="value_numeric").sort_index()
    mature_panel = mature.pivot(index="observation_date", columns="series_id", values="value_numeric").sort_index()
    common_columns = sorted(set(first_panel.columns).intersection(mature_panel.columns))
    return first_panel[common_columns].astype(float), mature_panel[common_columns].astype(float)


def quarterly_average_monthly_panel(panel: pd.DataFrame) -> pd.DataFrame:
    """Convert a monthly panel to a quarterly average panel."""

    if not isinstance(panel.index, pd.DatetimeIndex):
        panel = panel.copy()
        panel.index = pd.to_datetime(panel.index)
    quarterly = panel.resample("QE").mean()
    quarterly.index = quarterly.index.to_period("Q").map(lambda p: f"{p.year}:Q{p.quarter}")
    return quarterly


def align_quarterly_model_panels(
    first_monthly: pd.DataFrame,
    mature_monthly: pd.DataFrame,
    release_panel: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Quarterly-average indicators and align them to the GDP release panel."""

    first_q = quarterly_average_monthly_panel(first_monthly)
    mature_q = quarterly_average_monthly_panel(mature_monthly)
    common_index = first_q.index.intersection(mature_q.index).intersection(release_panel.index)
    common_columns = first_q.columns.intersection(mature_q.columns)
    return first_q.loc[common_index, common_columns], mature_q.loc[common_index, common_columns], release_panel.loc[common_index]
