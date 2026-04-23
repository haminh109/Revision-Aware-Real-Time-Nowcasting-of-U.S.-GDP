import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "rtdsm" / "routput" / "routput_first_second_third.xlsx"
OUTPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "targets" / "gdp_release_targets.csv"

HEADER_TOKENS = {"date", "first", "second", "third", "most_recent"}
QUARTER_PATTERN = re.compile(r"^(?P<year>\d{4}):Q(?P<quarter>[1-4])$")

STAGE_ORDER = {
    "first": 1,
    "second": 2,
    "third": 3,
    "most_recent": 4,
}

GENERAL_NOTE = (
    "RTDSM release-stage workbook provides first/second/third release values plus a raw Most_Recent "
    "column. The workbook does not expose exact publication dates for each row, so release_date is left blank. "
    "Most_Recent is preserved as a raw source column and should not be silently treated as the paper's ex-ante mature target."
)


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_header_row(frame: pd.DataFrame):
    for row_index in range(min(len(frame), 12)):
        observed = {normalize_text(value).lower().replace(" ", "_") for value in frame.iloc[row_index]}
        if HEADER_TOKENS.issubset(observed):
            return row_index
    raise ValueError("Could not locate the RTDSM release-stage header row in the DATA sheet.")


def parse_quarter_label(label: str):
    match = QUARTER_PATTERN.match(label)
    if match is None:
        raise ValueError(f"Invalid RTDSM target-quarter label: {label}")
    return int(match.group("year")), int(match.group("quarter"))


def parse_notes_metadata():
    notes = pd.read_excel(INPUT_PATH, sheet_name="NOTES", header=None)
    note_lines = [normalize_text(value) for value in notes.iloc[:, 0].tolist() if normalize_text(value)]

    metadata = {
        "target_description": "Real GNP/GDP",
        "source_measure": "qoq_annualized_percent",
        "source_measure_label": "",
        "source_last_updated": "",
    }

    for line in note_lines:
        if line.lower().startswith("unit of measurement:"):
            metadata["source_measure_label"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("last updated on:"):
            raw_date = line.split(":", 1)[1].strip()
            metadata["source_last_updated"] = pd.to_datetime(raw_date, errors="raise").strftime("%Y-%m-%d")

    return metadata


def load_release_stage_table():
    preview = pd.read_excel(INPUT_PATH, sheet_name="DATA", header=None, nrows=12)
    header_row = find_header_row(preview)

    data = pd.read_excel(INPUT_PATH, sheet_name="DATA", header=header_row)
    data.columns = [normalize_text(col) for col in data.columns]
    required_columns = ["Date", "First", "Second", "Third", "Most_Recent"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing expected RTDSM release-stage columns: {missing_columns}")

    frame = data[required_columns].dropna(how="all")
    frame["Date"] = frame["Date"].map(normalize_text)
    frame = frame[frame["Date"] != ""].reset_index(drop=True)
    return frame


def build_output():
    metadata = parse_notes_metadata()
    frame = load_release_stage_table()

    long_frame = frame.melt(
        id_vars=["Date"],
        value_vars=["First", "Second", "Third", "Most_Recent"],
        var_name="release_stage_raw",
        value_name="value",
    )
    long_frame["release_stage"] = long_frame["release_stage_raw"].str.lower()
    long_frame["value"] = pd.to_numeric(long_frame["value"], errors="coerce")
    long_frame = long_frame.dropna(subset=["value"]).reset_index(drop=True)

    years = []
    quarters = []
    for label in long_frame["Date"]:
        year, quarter = parse_quarter_label(label)
        years.append(year)
        quarters.append(quarter)

    long_frame["target_year"] = years
    long_frame["target_quarter_number"] = quarters
    long_frame["release_stage_order"] = long_frame["release_stage"].map(STAGE_ORDER)

    output = pd.DataFrame(
        {
            "source_family": "RTDSM",
            "source_dataset": "routput_first_second_third",
            "source_file": INPUT_PATH.relative_to(PROJECT_ROOT).as_posix(),
            "source_sheet": "DATA",
            "target_variable_id": "ROUTPUT",
            "target_description": metadata["target_description"],
            "source_measure": metadata["source_measure"],
            "source_measure_label": metadata["source_measure_label"],
            "source_last_updated": metadata["source_last_updated"],
            "target_quarter": long_frame["Date"],
            "target_year": long_frame["target_year"],
            "target_quarter_number": long_frame["target_quarter_number"],
            "release_stage": long_frame["release_stage"],
            "release_stage_order": long_frame["release_stage_order"],
            "release_date": pd.NA,
            "release_date_status": "not_provided_in_raw_file",
            "value": long_frame["value"],
            "notes": GENERAL_NOTE,
        }
    )

    output = output.sort_values(
        ["target_year", "target_quarter_number", "release_stage_order"],
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
