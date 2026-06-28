from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
ALFRED_LONG = REPO_ROOT / "data/bronze/indicators/alfred_monthly_long.csv"
GDP_TARGETS = REPO_ROOT / "data/bronze/targets/gdp_release_targets.csv"
OUTPUT = REPO_ROOT / "data/silver/calendars/gdp_release_calendar_alfred.csv"
METADATA_OUTPUT = REPO_ROOT / "data/silver/calendars/gdp_release_calendar_alfred_metadata.json"
SOURCE_SERIES_ID = "GDPC1"
SOURCE_PROVENANCE = "data/bronze/indicators/alfred_monthly_long.csv"
DEFAULT_RELEASE_TIME_ET = "08:30"
STAGE_TO_ROUND = {
    "first": "A",
    "advance": "A",
    "second": "S",
    "preliminary": "S",
    "third": "T",
    "final": "T",
    "most_recent": "M",
    "mature": "M",
}
ROUND_TO_STAGE = {
    "A": "first",
    "S": "second",
    "T": "third",
    "M": "mature_12_quarters",
}
ROUND_DAY_WINDOWS = {
    "A": (1, 180),
    "S": (1, 180),
    "T": (1, 180),
}


@dataclass(slots=True)
class CalendarCoverage:
    n_quarters: int
    n_rows: int
    n_vintage_derived: int
    n_approximate: int
    min_target_quarter: str
    max_target_quarter: str


def _quarter_to_period(label: str) -> pd.Period:
    year, quarter = str(label).split(":Q")
    return pd.Period(year=int(year), quarter=int(quarter), freq="Q")


def _period_to_label(period: pd.Period) -> str:
    return f"{period.year}:Q{period.quarter}"


def _quarter_start(label: str) -> pd.Timestamp:
    return _quarter_to_period(label).to_timestamp(how="start").normalize()


def _quarter_end(label: str) -> pd.Timestamp:
    return _quarter_to_period(label).to_timestamp(how="end").normalize()


def _fallback_release_date(target_quarter: str, release_round: str) -> pd.Timestamp:
    quarter_end = _quarter_end(target_quarter)
    month_offset = {"A": 1, "S": 2, "T": 3, "M": 39}[release_round]
    return (quarter_end + pd.offsets.MonthEnd(month_offset)).normalize()


def _valid_release_date(target_quarter: str, release_round: str, release_date: pd.Timestamp | None) -> bool:
    if release_date is None:
        return False
    if release_round not in ROUND_DAY_WINDOWS:
        return True
    q_end = _quarter_end(target_quarter)
    lower, upper = ROUND_DAY_WINDOWS[release_round]
    days_after = int((release_date - q_end).days)
    return lower <= days_after <= upper


def _load_target_quarters() -> list[str]:
    targets = pd.read_csv(GDP_TARGETS, usecols=["target_quarter", "release_stage"])
    targets["release_round"] = targets["release_stage"].astype(str).str.lower().map(STAGE_TO_ROUND)
    targets = targets[targets["release_round"].isin(RELEASE_ROUNDS)]
    quarters = targets["target_quarter"].dropna().drop_duplicates().tolist()
    return sorted(quarters, key=_quarter_to_period)


def _load_gdpc1_vintages() -> pd.DataFrame:
    raw = pd.read_csv(
        ALFRED_LONG,
        usecols=["series_id", "series_frequency", "observation_date", "realtime_start", "value_numeric"],
        parse_dates=["observation_date", "realtime_start"],
        low_memory=False,
    )
    frame = raw.loc[raw["series_id"].eq(SOURCE_SERIES_ID)].copy()
    if frame.empty:
        raise ValueError(f"{SOURCE_SERIES_ID} not found in {ALFRED_LONG}")
    frame["value_numeric"] = pd.to_numeric(frame["value_numeric"], errors="coerce")
    frame = frame[np.isfinite(frame["value_numeric"])]
    return frame.sort_values(["observation_date", "realtime_start"])


RELEASE_ROUNDS = ("A", "S", "T", "M")


def build_calendar() -> tuple[pd.DataFrame, CalendarCoverage]:
    quarters = _load_target_quarters()
    vintages = _load_gdpc1_vintages()
    vintage_dates_by_obs = {
        obs_date: group["realtime_start"].drop_duplicates().sort_values().tolist()
        for obs_date, group in vintages.groupby("observation_date")
    }
    rows: list[dict[str, object]] = []

    for quarter in quarters:
        period = _quarter_to_period(quarter)
        obs_date = _quarter_start(quarter)
        q_end = _quarter_end(quarter)
        candidates = [
            pd.Timestamp(date).normalize()
            for date in vintage_dates_by_obs.get(obs_date, [])
            if pd.Timestamp(date).normalize() > q_end
        ]
        first_three = candidates[:3]
        mature_cutoff = (_quarter_to_period(quarter) + 12).to_timestamp(how="end").normalize()
        mature_candidates = [date for date in candidates if date >= mature_cutoff]

        release_dates = {
            "A": first_three[0] if len(first_three) >= 1 else None,
            "S": first_three[1] if len(first_three) >= 2 else None,
            "T": first_three[2] if len(first_three) >= 3 else None,
            "M": mature_candidates[0] if mature_candidates else None,
        }
        for release_round in RELEASE_ROUNDS:
            release_date = release_dates[release_round]
            if not _valid_release_date(quarter, release_round, release_date):
                release_date = _fallback_release_date(quarter, release_round)
                derivation_status = "fallback_deterministic_month_end"
            else:
                derivation_status = "derived_from_alfred_gdpc1_vintage_date"
            rows.append(
                {
                    "target_quarter": quarter,
                    "target_year": period.year,
                    "target_quarter_number": period.quarter,
                    "release_round": release_round,
                    "release_stage": ROUND_TO_STAGE[release_round],
                    "public_release_date": release_date.date().isoformat(),
                    "public_release_time_et": DEFAULT_RELEASE_TIME_ET,
                    "public_release_timestamp_et": f"{release_date.date().isoformat()} {DEFAULT_RELEASE_TIME_ET}",
                    "source_series_id": SOURCE_SERIES_ID,
                    "source_observation_date": obs_date.date().isoformat(),
                    "derivation_status": derivation_status,
                    "days_after_quarter_end": int((release_date - q_end).days),
                    "provenance_file": SOURCE_PROVENANCE,
                    "notes": (
                        "GDP release date is derived from the first GDPC1 ALFRED realtime_start dates "
                        "after the target quarter end. Time is the BEA 08:30 ET default convention."
                    ),
                }
            )

    calendar = pd.DataFrame(rows)
    coverage = CalendarCoverage(
        n_quarters=len(quarters),
        n_rows=len(calendar),
        n_vintage_derived=int(calendar["derivation_status"].eq("derived_from_alfred_gdpc1_vintage_date").sum()),
        n_approximate=int(calendar["derivation_status"].eq("fallback_deterministic_month_end").sum()),
        min_target_quarter=min(quarters),
        max_target_quarter=max(quarters),
    )
    return calendar, coverage


def main() -> None:
    calendar, coverage = build_calendar()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    calendar.to_csv(OUTPUT, index=False)
    METADATA_OUTPUT.write_text(
        pd.Series(
            {
                "source_series_id": SOURCE_SERIES_ID,
                "source_file": SOURCE_PROVENANCE,
                "output_file": str(OUTPUT.relative_to(REPO_ROOT)),
                "n_quarters": coverage.n_quarters,
                "n_rows": coverage.n_rows,
                "n_vintage_derived": coverage.n_vintage_derived,
                "n_approximate": coverage.n_approximate,
                "min_target_quarter": coverage.min_target_quarter,
                "max_target_quarter": coverage.max_target_quarter,
                "release_time_convention_et": DEFAULT_RELEASE_TIME_ET,
            }
        ).to_json(indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {OUTPUT}")
    print(f"rows={coverage.n_rows} vintage_derived={coverage.n_vintage_derived} approximate={coverage.n_approximate}")


if __name__ == "__main__":
    main()
