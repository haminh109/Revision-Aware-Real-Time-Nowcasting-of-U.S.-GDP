import json
import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALFRED_SERIES_DIR = PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations"
CENSUS_CALENDAR_DIR = PROJECT_ROOT / "data" / "raw" / "calendars" / "census"

SOURCE = "ALFRED"
PROXY_METHOD = "realtime_start_availability_proxy"
SERIES_NOTES = (
    "Proxy availability event inferred from ALFRED vintage dates; not an official Census release timestamp."
)
BLOCK_NOTES = (
    "Block-level proxy built from union of ALFRED availability dates across included series; not an official Census release calendar."
)

RELEASE_BLOCKS = {
    "retail_sales": ["RSAFS", "RSXFS"],
    "housing": ["HOUST", "PERMIT"],
    "durable_goods": ["DGORDER", "NEWORDER"],
    "inventories": ["BUSINV", "ISRATIO"],
    "trade": ["BOPGSTB", "BOPTEXP", "BOPTIMP"],
}

SERIES_TO_BLOCK = {
    series_id: release_block
    for release_block, series_ids in RELEASE_BLOCKS.items()
    for series_id in series_ids
}

SERIES_OUTPUT_PATH = CENSUS_CALENDAR_DIR / "census_proxy_release_events.csv"
BLOCK_OUTPUT_PATH = CENSUS_CALENDAR_DIR / "census_proxy_release_calendar.csv"
METADATA_OUTPUT_PATH = CENSUS_CALENDAR_DIR / "census_proxy_calendar_metadata.json"


def normalize_release_dates(raw_dates, series_id):
    cleaned = pd.Series(raw_dates, dtype="string").dropna().str.strip()
    cleaned = cleaned[cleaned != ""].drop_duplicates()
    parsed = pd.to_datetime(cleaned, errors="coerce")

    invalid = cleaned[parsed.isna()].tolist()
    if invalid:
        raise ValueError(
            f"Found invalid availability dates for {series_id}: {invalid[:5]}"
        )

    return parsed.dt.strftime("%Y-%m-%d").sort_values().drop_duplicates().tolist()


def extract_realtime_start_dates(series_id, csv_path):
    header = pd.read_csv(csv_path, nrows=0)
    columns = header.columns.tolist()

    if "realtime_start" in columns:
        realtime_starts = pd.read_csv(csv_path, usecols=["realtime_start"])["realtime_start"]
        return normalize_release_dates(realtime_starts, series_id)

    vintage_pattern = re.compile(rf"^{re.escape(series_id)}_(\d{{8}})$")
    inferred_dates = []
    for column in columns:
        match = vintage_pattern.match(column)
        if match:
            inferred_dates.append(match.group(1))

    if not inferred_dates:
        raise ValueError(
            f"Could not infer ALFRED availability dates from {csv_path} for {series_id}."
        )

    return normalize_release_dates(inferred_dates, series_id)


def build_series_events():
    rows = []

    for series_id in sorted(SERIES_TO_BLOCK):
        csv_path = ALFRED_SERIES_DIR / f"{series_id}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing ALFRED observations file: {csv_path}")

        provenance_file = csv_path.relative_to(PROJECT_ROOT).as_posix()
        release_block = SERIES_TO_BLOCK[series_id]
        release_dates = extract_realtime_start_dates(series_id, csv_path)

        for release_date in release_dates:
            rows.append(
                {
                    "source": SOURCE,
                    "series_id": series_id,
                    "release_block": release_block,
                    "release_date": release_date,
                    "release_time_et": pd.NA,
                    "proxy_method": PROXY_METHOD,
                    "provenance_file": provenance_file,
                    "notes": SERIES_NOTES,
                }
            )

    events = pd.DataFrame(rows).drop_duplicates()
    events = events.sort_values(
        ["release_date", "release_block", "series_id"],
        kind="stable",
    ).reset_index(drop=True)
    return events


def build_block_calendar(events):
    grouped = (
        events.groupby(["release_block", "release_date"], as_index=False)
        .agg(
            included_series=(
                "series_id",
                lambda values: ";".join(sorted(set(values))),
            )
        )
        .sort_values(["release_date", "release_block"], kind="stable")
        .reset_index(drop=True)
    )

    grouped["source"] = SOURCE
    grouped["release_time_et"] = pd.NA
    grouped["proxy_method"] = PROXY_METHOD
    grouped["notes"] = BLOCK_NOTES

    return grouped[
        [
            "source",
            "release_block",
            "release_date",
            "release_time_et",
            "included_series",
            "proxy_method",
            "notes",
        ]
    ]


def build_metadata():
    return {
        "title": "Census proxy release calendar",
        "status": "proxy_not_official",
        "source": SOURCE,
        "proxy_method": PROXY_METHOD,
        "summary": (
            "Proxy release calendar for Census-related indicators derived from "
            "ALFRED vintage availability dates for the series already used in this repository."
        ),
        "important_notes": [
            "This is NOT an official Census release calendar.",
            "It is a proxy calendar derived from ALFRED vintage availability dates.",
            "It preserves daily availability logic via ALFRED release-day vintage dates.",
            "It does not claim exact official intraday release timestamps.",
            "release_time_et is intentionally left blank for Census proxy events.",
            "Census indicator values remain the canonical ALFRED/FRED values already used elsewhere in the repo.",
        ],
        "release_blocks": RELEASE_BLOCKS,
        "input_pattern": "data/raw/alfred/series_observations/{SERIES_ID}.csv",
        "artifacts": {
            "series_level_events": SERIES_OUTPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
            "block_level_calendar": BLOCK_OUTPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
        },
    }


def write_outputs(events, calendar):
    CENSUS_CALENDAR_DIR.mkdir(parents=True, exist_ok=True)
    events.to_csv(SERIES_OUTPUT_PATH, index=False)
    calendar.to_csv(BLOCK_OUTPUT_PATH, index=False)

    with open(METADATA_OUTPUT_PATH, "w", encoding="utf-8") as file_obj:
        json.dump(build_metadata(), file_obj, ensure_ascii=False, indent=2)


def main():
    events = build_series_events()
    calendar = build_block_calendar(events)
    write_outputs(events, calendar)

    print(f"[OK] Wrote {len(events)} series-level proxy events -> {SERIES_OUTPUT_PATH}")
    print(f"[OK] Wrote {len(calendar)} block-level proxy rows -> {BLOCK_OUTPUT_PATH}")
    print(f"[OK] Wrote metadata -> {METADATA_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
