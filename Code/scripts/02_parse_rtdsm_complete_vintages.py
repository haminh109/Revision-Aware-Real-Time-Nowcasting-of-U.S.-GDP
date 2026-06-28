import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "rtdsm" / "routput" / "ROUTPUTQvQd.xlsx"
OUTPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "targets" / "gdp_complete_vintages_long.csv"

TARGET_QUARTER_PATTERN = re.compile(r"^(?P<year>\d{4}):Q(?P<quarter>[1-4])$")
VINTAGE_COLUMN_PATTERN = re.compile(r"^ROUTPUT(?P<yy>\d{2})Q(?P<quarter>[1-4])$")

GENERAL_NOTE = (
    "RTDSM complete-vintage workbook exposes quarter-coded vintage labels rather than exact daily release dates, "
    "so bronze preserves vintage_period and does not fabricate a vintage_date. The workbook does not include an "
    "explicit unit note; raw numeric values appear level-like and are intentionally left unharmonized here."
)


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def parse_target_quarter(label: str):
    match = TARGET_QUARTER_PATTERN.match(label)
    if match is None:
        raise ValueError(f"Invalid RTDSM target-quarter label: {label}")
    return int(match.group("year")), int(match.group("quarter"))


def expand_two_digit_year(two_digit_year: str) -> int:
    year = int(two_digit_year)
    return 1900 + year if year >= 50 else 2000 + year


def parse_vintage_column(column_name: str):
    match = VINTAGE_COLUMN_PATTERN.match(column_name)
    if match is None:
        raise ValueError(f"Invalid RTDSM vintage column name: {column_name}")
    year = expand_two_digit_year(match.group("yy"))
    quarter = int(match.group("quarter"))
    return year, quarter, f"{year}:Q{quarter}"


def build_output():
    frame = pd.read_excel(INPUT_PATH, sheet_name="ROUTPUT")
    frame.columns = [normalize_text(col) for col in frame.columns]

    if len(frame.columns) == 0 or frame.columns[0] != "DATE":
        raise ValueError("Expected the first RTDSM complete-vintage column to be DATE.")

    target_info = frame["DATE"].map(normalize_text).map(parse_target_quarter)
    frame["target_quarter"] = frame["DATE"].map(normalize_text)
    frame["target_year"] = [item[0] for item in target_info]
    frame["target_quarter_number"] = [item[1] for item in target_info]

    vintage_columns = frame.columns[1:-3]
    vintage_metadata = {}
    for column_name in vintage_columns:
        vintage_year, vintage_quarter, vintage_period = parse_vintage_column(column_name)
        vintage_metadata[column_name] = {
            "vintage_year": vintage_year,
            "vintage_quarter_number": vintage_quarter,
            "vintage_period": vintage_period,
        }

    long_frame = frame.melt(
        id_vars=["target_quarter", "target_year", "target_quarter_number"],
        value_vars=vintage_columns,
        var_name="vintage_label_raw",
        value_name="value",
    )
    long_frame["value"] = pd.to_numeric(long_frame["value"], errors="coerce")
    long_frame = long_frame.dropna(subset=["value"]).reset_index(drop=True)

    long_frame["vintage_period"] = long_frame["vintage_label_raw"].map(
        lambda value: vintage_metadata[value]["vintage_period"]
    )
    long_frame["vintage_year"] = long_frame["vintage_label_raw"].map(
        lambda value: vintage_metadata[value]["vintage_year"]
    )
    long_frame["vintage_quarter_number"] = long_frame["vintage_label_raw"].map(
        lambda value: vintage_metadata[value]["vintage_quarter_number"]
    )

    output = pd.DataFrame(
        {
            "source_family": "RTDSM",
            "source_dataset": "ROUTPUTQvQd",
            "source_file": INPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
            "source_sheet": "ROUTPUT",
            "target_variable_id": "ROUTPUT",
            "source_measure": "raw_numeric_unit_not_explicit_in_workbook",
            "target_quarter": long_frame["target_quarter"],
            "target_year": long_frame["target_year"],
            "target_quarter_number": long_frame["target_quarter_number"],
            "vintage_period": long_frame["vintage_period"],
            "vintage_year": long_frame["vintage_year"],
            "vintage_quarter_number": long_frame["vintage_quarter_number"],
            "vintage_label_raw": long_frame["vintage_label_raw"],
            "value": long_frame["value"],
            "notes": GENERAL_NOTE,
        }
    )

    output = output.sort_values(
        ["target_year", "target_quarter_number", "vintage_year", "vintage_quarter_number"],
        kind="stable",
    ).reset_index(drop=True)
    return output


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    output = build_output()
    output.to_csv(OUTPUT_PATH, index=False)
    print(f"[OK] Wrote {len(output)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
