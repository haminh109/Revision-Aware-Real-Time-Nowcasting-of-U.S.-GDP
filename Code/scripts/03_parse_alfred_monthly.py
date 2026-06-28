import json
import re
from datetime import timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGE0_MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage0_manifest.json"
OBSERVATIONS_DIR = PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations"
VINTAGE_DATES_DIR = PROJECT_ROOT / "data" / "raw" / "alfred" / "vintage_dates"
OUTPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "indicators" / "alfred_monthly_long.csv"

QUARTERLY_SERIES = {
    "GDPC1",
    "A191RL1Q225SBEA",
}

MISSING_VALUE_SENTINELS = {
    ".",
}


def load_stage0_manifest():
    with open(STAGE0_MANIFEST_PATH, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_iso_date(value, context: str) -> str:
    text = normalize_text(value)
    if not text:
        raise ValueError(f"Missing date value for {context}.")
    return pd.to_datetime(text, errors="raise").strftime("%Y-%m-%d")


def build_series_list():
    manifest = load_stage0_manifest()
    series_list = list(manifest.get("required_alfred_series", []))
    for series_id in manifest.get("optional_alfred_series", []):
        observation_path = OBSERVATIONS_DIR / f"{series_id}.csv"
        vintage_dates_path = VINTAGE_DATES_DIR / f"{series_id}.csv"
        if observation_path.exists() and vintage_dates_path.exists():
            series_list.append(series_id)
    return sorted(series_list)


def load_vintage_dates(series_id: str):
    vintage_dates_path = VINTAGE_DATES_DIR / f"{series_id}.csv"
    vintage_dates = pd.read_csv(vintage_dates_path, dtype="string")
    if "vintage_date" not in vintage_dates.columns:
        raise ValueError(f"{vintage_dates_path} is missing the vintage_date column.")

    cleaned = vintage_dates["vintage_date"].dropna().map(lambda value: normalize_iso_date(value, f"{series_id} vintage_date"))
    cleaned = cleaned[cleaned != ""].drop_duplicates().tolist()
    if not cleaned:
        raise ValueError(f"{vintage_dates_path} contains no usable vintage dates.")

    end_map = {}
    for index, realtime_start in enumerate(cleaned):
        if index + 1 < len(cleaned):
            next_start = pd.to_datetime(cleaned[index + 1], errors="raise")
            end_map[realtime_start] = (next_start - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            end_map[realtime_start] = "9999-12-31"

    return cleaned, end_map


def infer_series_frequency(series_id: str) -> str:
    return "quarterly" if series_id in QUARTERLY_SERIES else "monthly"


def finalize_output_frame(
    series_id: str,
    frame: pd.DataFrame,
    raw_schema: str,
    realtime_end_source: str,
):
    frame["series_id"] = series_id
    frame["series_frequency"] = infer_series_frequency(series_id)
    frame["source_family"] = "ALFRED"
    frame["raw_schema"] = raw_schema
    frame["realtime_end_source"] = realtime_end_source
    frame["provenance_observations_file"] = (OBSERVATIONS_DIR / f"{series_id}.csv").relative_to(PROJECT_ROOT).as_posix()
    frame["provenance_vintage_dates_file"] = (VINTAGE_DATES_DIR / f"{series_id}.csv").relative_to(PROJECT_ROOT).as_posix()

    frame = frame[
        [
            "source_family",
            "series_id",
            "series_frequency",
            "raw_schema",
            "observation_date",
            "realtime_start",
            "realtime_end",
            "realtime_end_source",
            "value_raw",
            "value_numeric",
            "is_missing_value",
            "provenance_observations_file",
            "provenance_vintage_dates_file",
        ]
    ]
    frame = frame.sort_values(["observation_date", "realtime_start"], kind="stable").reset_index(drop=True)
    return frame


def parse_wide_schema(series_id: str):
    observation_path = OBSERVATIONS_DIR / f"{series_id}.csv"
    vintage_dates, realtime_end_map = load_vintage_dates(series_id)
    vintage_date_set = set(vintage_dates)

    frame = pd.read_csv(observation_path, dtype="string")
    columns = [normalize_text(col) for col in frame.columns]
    frame.columns = columns

    if not columns or columns[0] != "date":
        raise ValueError(f"{observation_path} must begin with a date column.")

    pattern = re.compile(rf"^{re.escape(series_id)}_(?P<yyyymmdd>\d{{8}})$")
    vintage_columns = []
    for column_name in columns[1:]:
        match = pattern.match(column_name)
        if match is None:
            continue
        realtime_start = pd.to_datetime(match.group("yyyymmdd"), format="%Y%m%d", errors="raise").strftime("%Y-%m-%d")
        vintage_columns.append((column_name, realtime_start))

    if not vintage_columns:
        raise ValueError(f"{observation_path} does not contain any parseable vintage columns for {series_id}.")

    vintage_columns = sorted(vintage_columns, key=lambda item: item[1])
    unknown_vintage_dates = sorted({realtime_start for _, realtime_start in vintage_columns if realtime_start not in vintage_date_set})
    if unknown_vintage_dates:
        raise ValueError(
            f"{observation_path} contains vintage columns not present in the vintage_dates file for {series_id}: "
            f"{unknown_vintage_dates[:5]}"
        )

    selected_columns = ["date"] + [column_name for column_name, _ in vintage_columns]
    frame = frame[selected_columns]
    long_frame = frame.melt(
        id_vars=["date"],
        value_vars=[column_name for column_name, _ in vintage_columns],
        var_name="vintage_column",
        value_name="value_raw",
    )

    long_frame["value_raw"] = long_frame["value_raw"].fillna("").map(normalize_text)
    long_frame = long_frame[long_frame["value_raw"] != ""].reset_index(drop=True)

    vintage_map = {column_name: realtime_start for column_name, realtime_start in vintage_columns}
    long_frame["observation_date"] = long_frame["date"].map(lambda value: normalize_iso_date(value, f"{series_id} observation_date"))
    long_frame["realtime_start"] = long_frame["vintage_column"].map(vintage_map)
    long_frame["realtime_end"] = long_frame["realtime_start"].map(realtime_end_map)

    long_frame["value_numeric"] = pd.to_numeric(long_frame["value_raw"], errors="coerce")
    unexpected_non_numeric = (
        long_frame.loc[
            long_frame["value_numeric"].isna() & ~long_frame["value_raw"].isin(MISSING_VALUE_SENTINELS),
            "value_raw",
        ]
        .drop_duplicates()
        .tolist()
    )
    if unexpected_non_numeric:
        raise ValueError(
            f"{observation_path} contains unexpected non-numeric values for {series_id}: {unexpected_non_numeric[:5]}"
        )

    long_frame["is_missing_value"] = long_frame["value_raw"].isin(MISSING_VALUE_SENTINELS)
    return finalize_output_frame(
        series_id=series_id,
        frame=long_frame.rename(columns={"date": "date_unused"}),
        raw_schema="wide_vintage_matrix",
        realtime_end_source="derived_from_vintage_dates",
    )


def parse_long_schema(series_id: str):
    observation_path = OBSERVATIONS_DIR / f"{series_id}.csv"
    vintage_dates, realtime_end_map = load_vintage_dates(series_id)
    vintage_date_set = set(vintage_dates)

    frame = pd.read_csv(observation_path, dtype="string")
    columns = {normalize_text(col).lower(): col for col in frame.columns}
    required_columns = {"realtime_start", "date", "value"}
    missing_columns = sorted(required_columns.difference(columns))
    if missing_columns:
        raise ValueError(f"{observation_path} is missing required long-schema columns: {missing_columns}")

    long_frame = pd.DataFrame(
        {
            "observation_date": frame[columns["date"]].map(
                lambda value: normalize_iso_date(value, f"{series_id} observation_date")
            ),
            "realtime_start": frame[columns["realtime_start"]].map(
                lambda value: normalize_iso_date(value, f"{series_id} realtime_start")
            ),
            "value_raw": frame[columns["value"]].fillna("").map(normalize_text),
        }
    )
    long_frame = long_frame[long_frame["value_raw"] != ""].reset_index(drop=True)

    if "realtime_end" in columns:
        long_frame["realtime_end"] = frame[columns["realtime_end"]].map(
            lambda value: normalize_iso_date(value, f"{series_id} realtime_end")
        )
        realtime_end_source = "raw_observations_file"
    else:
        unknown_starts = sorted(set(long_frame["realtime_start"]) - vintage_date_set)
        if unknown_starts:
            raise ValueError(
                f"{observation_path} contains realtime_start values not present in the vintage_dates file for {series_id}: "
                f"{unknown_starts[:5]}"
            )
        long_frame["realtime_end"] = long_frame["realtime_start"].map(realtime_end_map)
        realtime_end_source = "derived_from_vintage_dates"

    long_frame["value_numeric"] = pd.to_numeric(long_frame["value_raw"], errors="coerce")
    unexpected_non_numeric = (
        long_frame.loc[
            long_frame["value_numeric"].isna() & ~long_frame["value_raw"].isin(MISSING_VALUE_SENTINELS),
            "value_raw",
        ]
        .drop_duplicates()
        .tolist()
    )
    if unexpected_non_numeric:
        raise ValueError(
            f"{observation_path} contains unexpected non-numeric values for {series_id}: {unexpected_non_numeric[:5]}"
        )

    long_frame["is_missing_value"] = long_frame["value_raw"].isin(MISSING_VALUE_SENTINELS)
    return finalize_output_frame(
        series_id=series_id,
        frame=long_frame,
        raw_schema="long_observation_rows",
        realtime_end_source=realtime_end_source,
    )


def parse_series(series_id: str):
    observation_path = OBSERVATIONS_DIR / f"{series_id}.csv"
    header = pd.read_csv(observation_path, nrows=0)
    columns = [normalize_text(col) for col in header.columns]
    if "realtime_start" in {col.lower() for col in columns}:
        return parse_long_schema(series_id)
    return parse_wide_schema(series_id)


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    total_rows = 0
    first_write = True
    for series_id in build_series_list():
        output = parse_series(series_id)
        output.to_csv(OUTPUT_PATH, mode="w" if first_write else "a", index=False, header=first_write)
        total_rows += len(output)
        first_write = False
        print(f"[OK] Parsed {series_id}: {len(output)} rows")

    print(f"[OK] Wrote {total_rows} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
