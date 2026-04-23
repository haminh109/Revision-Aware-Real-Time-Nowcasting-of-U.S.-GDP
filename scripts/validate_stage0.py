import json
import re
import sys
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage0_manifest.json"
REPORT_PATH = PROJECT_ROOT / "data" / "metadata" / "stage0_validation_report.json"

REQUIRED_STAGE0_SCRIPTS = [
    "scripts/download_alfred_and_calendars.py",
    "scripts/download_bea.py",
    "scripts/build_census_proxy_calendar.py",
    "scripts/validate_stage0.py",
]

REQUIRED_REQUIREMENT_TOKENS = [
    "pandas",
    "requests",
    "python-dotenv",
    "beautifulsoup4",
    "lxml",
    "openpyxl",
]

REQUIRED_ENV_KEYS = [
    "FRED_API_KEY",
    "BEA_API_KEY",
]

QUARTER_LABEL_PATTERN = re.compile(r"^\d{4}:Q[1-4]$")


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def relative_path(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def parse_iso_date(value: str):
    return pd.to_datetime(value, errors="raise").strftime("%Y-%m-%d")


def is_blank(value) -> bool:
    return normalize_text(value) == ""


def check_paths(paths):
    present = []
    missing = []
    for rel_path in paths:
        if (PROJECT_ROOT / rel_path).exists():
            present.append(rel_path)
        else:
            missing.append(rel_path)
    return {"present": present, "missing": missing}


def load_manifest():
    return load_json(MANIFEST_PATH)


def check_required_scripts():
    return check_paths(REQUIRED_STAGE0_SCRIPTS)


def check_requirements_file():
    requirements_path = PROJECT_ROOT / "requirements.txt"
    if not requirements_path.exists():
        return {
            "status": "FAIL",
            "detail": "requirements.txt is missing.",
            "missing_tokens": REQUIRED_REQUIREMENT_TOKENS,
        }

    text = requirements_path.read_text(encoding="utf-8")
    missing_tokens = [token for token in REQUIRED_REQUIREMENT_TOKENS if token not in text]
    status = "PASS" if not missing_tokens else "FAIL"
    return {
        "status": status,
        "detail": "requirements.txt contains the baseline Stage 0 dependencies."
        if status == "PASS"
        else "requirements.txt is missing one or more expected Stage 0 dependency tokens.",
        "missing_tokens": missing_tokens,
    }


def check_env_template():
    env_path = PROJECT_ROOT / ".env.example"
    if not env_path.exists():
        return {
            "status": "FAIL",
            "detail": ".env.example is missing.",
            "missing_keys": REQUIRED_ENV_KEYS,
        }

    text = env_path.read_text(encoding="utf-8")
    missing_keys = [key for key in REQUIRED_ENV_KEYS if key not in text]
    status = "PASS" if not missing_keys else "FAIL"
    return {
        "status": status,
        "detail": ".env.example contains the expected API-key placeholders."
        if status == "PASS"
        else ".env.example is missing one or more expected API-key placeholders.",
        "missing_keys": missing_keys,
    }


def check_gitignore():
    gitignore_path = PROJECT_ROOT / ".gitignore"
    if not gitignore_path.exists():
        return {
            "status": "FAIL",
            "detail": ".gitignore is missing.",
            "missing_tokens": [".env", ".venv/"],
        }

    text = gitignore_path.read_text(encoding="utf-8")
    expected_tokens = [".env", ".venv/", "__pycache__/"]
    missing_tokens = [token for token in expected_tokens if token not in text]
    status = "PASS" if not missing_tokens else "FAIL"
    return {
        "status": status,
        "detail": ".gitignore covers local environment files and caches."
        if status == "PASS"
        else ".gitignore is missing one or more expected local ignore patterns.",
        "missing_tokens": missing_tokens,
    }


def find_header_row(frame: pd.DataFrame, required_tokens):
    for row_index in range(min(len(frame), 12)):
        observed = {normalize_text(value).lower().replace(" ", "_") for value in frame.iloc[row_index]}
        if required_tokens.issubset(observed):
            return row_index
    return None


def inspect_rtdsm_release_specific(path: Path):
    workbook = pd.ExcelFile(path)
    if "DATA" not in workbook.sheet_names:
        return {
            "status": "FAIL",
            "detail": "Expected DATA sheet is missing from routput_first_second_third.xlsx.",
        }

    preview = pd.read_excel(path, sheet_name="DATA", header=None, nrows=12)
    header_row = find_header_row(preview, {"date", "first", "second", "third", "most_recent"})
    if header_row is None:
        return {
            "status": "FAIL",
            "detail": "Could not find the RTDSM release-stage header row in the DATA sheet.",
        }

    data = pd.read_excel(path, sheet_name="DATA", header=header_row)
    columns = [normalize_text(col) for col in data.columns]
    data.columns = columns
    required_columns = ["Date", "First", "Second", "Third", "Most_Recent"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        return {
            "status": "FAIL",
            "detail": "The RTDSM release-stage workbook is missing one or more expected columns.",
            "missing_columns": missing_columns,
        }

    cleaned = data[required_columns].dropna(how="all")
    cleaned["Date"] = cleaned["Date"].map(normalize_text)
    cleaned = cleaned[cleaned["Date"] != ""]
    invalid_quarters = cleaned.loc[
        ~cleaned["Date"].str.match(QUARTER_LABEL_PATTERN),
        "Date",
    ].head(5).tolist()
    if invalid_quarters:
        return {
            "status": "FAIL",
            "detail": "The RTDSM release-stage workbook contains invalid target-quarter labels.",
            "sample_invalid_quarters": invalid_quarters,
        }

    value_cells = (
        cleaned[["First", "Second", "Third", "Most_Recent"]]
        .apply(pd.to_numeric, errors="coerce")
        .stack()
    )
    if value_cells.empty:
        return {
            "status": "FAIL",
            "detail": "The RTDSM release-stage workbook has no numeric release values.",
        }

    notes = pd.read_excel(path, sheet_name="NOTES", header=None)
    note_lines = [normalize_text(value) for value in notes.iloc[:, 0].tolist() if normalize_text(value)]
    unit_line = next((line for line in note_lines if line.lower().startswith("unit of measurement:")), "")

    return {
        "status": "PASS",
        "detail": "The RTDSM release-stage workbook has the expected sheets, columns, and numeric content.",
        "row_count": int(len(cleaned)),
        "unit_line": unit_line,
        "sheet_names": workbook.sheet_names,
    }


def inspect_rtdsm_complete_vintages(path: Path):
    workbook = pd.ExcelFile(path)
    if "ROUTPUT" not in workbook.sheet_names:
        return {
            "status": "FAIL",
            "detail": "Expected ROUTPUT sheet is missing from ROUTPUTQvQd.xlsx.",
        }

    data = pd.read_excel(path, sheet_name="ROUTPUT")
    if data.empty:
        return {
            "status": "FAIL",
            "detail": "The RTDSM complete-vintage workbook is empty.",
        }

    columns = [normalize_text(col) for col in data.columns]
    data.columns = columns
    if not columns or columns[0] != "DATE":
        return {
            "status": "FAIL",
            "detail": "The RTDSM complete-vintage workbook does not begin with a DATE column.",
        }

    vintage_columns = columns[1:]
    pattern = re.compile(r"^ROUTPUT\d{2}Q[1-4]$")
    invalid_vintage_columns = [col for col in vintage_columns if not pattern.match(col)]
    if invalid_vintage_columns:
        return {
            "status": "FAIL",
            "detail": "The RTDSM complete-vintage workbook contains unexpected vintage column names.",
            "sample_invalid_columns": invalid_vintage_columns[:5],
        }

    target_quarters = data["DATE"].map(normalize_text)
    invalid_quarters = target_quarters.loc[~target_quarters.str.match(QUARTER_LABEL_PATTERN)].head(5).tolist()
    if invalid_quarters:
        return {
            "status": "FAIL",
            "detail": "The RTDSM complete-vintage workbook contains invalid target-quarter labels.",
            "sample_invalid_quarters": invalid_quarters,
        }

    numeric_values = data[vintage_columns].apply(pd.to_numeric, errors="coerce").stack()
    if numeric_values.empty:
        return {
            "status": "FAIL",
            "detail": "The RTDSM complete-vintage workbook has no numeric vintage values.",
        }

    return {
        "status": "PASS",
        "detail": "The RTDSM complete-vintage workbook has the expected quarterly vintage-matrix schema.",
        "row_count": int(len(data)),
        "vintage_column_count": int(len(vintage_columns)),
        "sample_vintage_columns": vintage_columns[:5],
    }


def inspect_rtdsm_optional_monthly(path: Path):
    workbook = pd.ExcelFile(path)
    if "routput" not in workbook.sheet_names:
        return {
            "status": "FAIL",
            "detail": "Expected routput sheet is missing from routputMvQd.xlsx.",
        }

    data = pd.read_excel(path, sheet_name="routput", nrows=5)
    columns = [normalize_text(col) for col in data.columns]
    pattern = re.compile(r"^ROUTPUT\d{2}M\d{1,2}$")
    vintage_columns = columns[1:]
    invalid_columns = [col for col in vintage_columns if not pattern.match(col)]

    return {
        "status": "PASS" if not invalid_columns else "FAIL",
        "detail": "The optional RTDSM monthly workbook has the expected monthly vintage-matrix schema."
        if not invalid_columns
        else "The optional RTDSM monthly workbook contains unexpected vintage column names.",
        "sample_invalid_columns": invalid_columns[:5],
        "vintage_column_count": int(len(vintage_columns)),
    }


def inspect_alfred_series(series_id: str):
    vintage_csv_path = PROJECT_ROOT / "data" / "raw" / "alfred" / "vintage_dates" / f"{series_id}.csv"
    vintage_json_path = PROJECT_ROOT / "data" / "raw" / "alfred" / "vintage_dates" / f"{series_id}.json"
    observations_csv_path = PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations" / f"{series_id}.csv"
    observations_json_path = PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations" / f"{series_id}.json"

    required_paths = [
        vintage_csv_path,
        vintage_json_path,
        observations_csv_path,
        observations_json_path,
    ]
    missing_paths = [relative_path(path) for path in required_paths if not path.exists()]
    if missing_paths:
        return {
            "status": "FAIL",
            "detail": "One or more required ALFRED files are missing.",
            "missing_paths": missing_paths,
        }

    vintage_dates = pd.read_csv(vintage_csv_path, dtype="string")
    if "vintage_date" not in vintage_dates.columns:
        return {
            "status": "FAIL",
            "detail": "ALFRED vintage_dates CSV is missing the vintage_date column.",
        }

    cleaned_vintage_dates = vintage_dates["vintage_date"].dropna().map(normalize_text)
    cleaned_vintage_dates = cleaned_vintage_dates[cleaned_vintage_dates != ""]
    if cleaned_vintage_dates.empty:
        return {
            "status": "FAIL",
            "detail": "ALFRED vintage_dates CSV contains no vintage dates.",
        }

    invalid_vintage_dates = []
    for raw_date in cleaned_vintage_dates.head(25):
        try:
            parse_iso_date(raw_date)
        except Exception:
            invalid_vintage_dates.append(raw_date)
    if invalid_vintage_dates:
        return {
            "status": "FAIL",
            "detail": "ALFRED vintage_dates CSV contains invalid date values.",
            "sample_invalid_dates": invalid_vintage_dates[:5],
        }

    observations_header = pd.read_csv(observations_csv_path, nrows=0)
    observation_columns = [normalize_text(col) for col in observations_header.columns]

    if "realtime_start" in observation_columns:
        schema = "long_observation_rows"
        required_columns = {"realtime_start", "date", "value"}
        missing_columns = sorted(required_columns.difference({col.lower() for col in observation_columns}))
        if missing_columns:
            return {
                "status": "FAIL",
                "detail": "ALFRED observations CSV has a long schema but is missing required columns.",
                "missing_columns": missing_columns,
            }
    else:
        schema = "wide_vintage_matrix"
        if not observation_columns or observation_columns[0] != "date":
            return {
                "status": "FAIL",
                "detail": "ALFRED observations CSV with a wide schema must begin with a date column.",
            }

        pattern = re.compile(rf"^{re.escape(series_id)}_\d{{8}}$")
        vintage_columns = [col for col in observation_columns[1:] if pattern.match(col)]
        if not vintage_columns:
            return {
                "status": "FAIL",
                "detail": "ALFRED observations CSV does not expose any parseable vintage columns.",
            }

    sample_rows = pd.read_csv(observations_csv_path, dtype="string", nrows=5)
    if sample_rows.empty:
        return {
            "status": "FAIL",
            "detail": "ALFRED observations CSV contains no data rows.",
        }

    return {
        "status": "PASS",
        "detail": "ALFRED raw files exist and the observations CSV is parseable.",
        "schema": schema,
        "vintage_date_count": int(cleaned_vintage_dates.nunique()),
        "observation_column_count": int(len(observation_columns)),
    }


def inspect_bea_file(path: Path):
    payload = load_json(path)
    data_rows = payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
    request_params = payload.get("BEAAPI", {}).get("Request", {}).get("RequestParam", [])
    user_id_values = [
        str(item.get("ParameterValue", ""))
        for item in request_params
        if str(item.get("ParameterName", "")).upper() == "USERID"
    ]

    if not data_rows:
        return {
            "status": "FAIL",
            "detail": "BEA API payload contains no data rows.",
        }

    exposed_values = [value for value in user_id_values if value and value != "REDACTED"]
    if exposed_values:
        return {
            "status": "FAIL",
            "detail": "BEA raw payload still exposes the API UserID and should be scrubbed.",
            "sample_exposed_value": exposed_values[0],
        }

    return {
        "status": "PASS",
        "detail": "BEA API payload is populated and the request UserID is scrubbed.",
        "row_count": int(len(data_rows)),
        "utc_production_time": payload.get("BEAAPI", {}).get("Results", {}).get("UTCProductionTime"),
    }


def inspect_bea_calendar(path: Path):
    tables = pd.read_html(path)
    if not tables:
        return {
            "status": "FAIL",
            "detail": "Could not parse any tables from the BEA release schedule HTML.",
        }

    table = tables[0]
    release_columns = [normalize_text(col) for col in table.columns]
    if not any("Release" in col for col in release_columns):
        return {
            "status": "FAIL",
            "detail": "The BEA release schedule table does not expose a Release column.",
        }

    non_empty_rows = int(table.dropna(how="all").shape[0])
    year_match = re.search(r"(19|20)\d{2}", release_columns[0]) if release_columns else None
    return {
        "status": "PASS",
        "detail": "The BEA release schedule HTML contains a parseable release table.",
        "row_count": non_empty_rows,
        "declared_scope_year": year_match.group(0) if year_match else None,
    }


def inspect_bls_current_year_calendar(path: Path):
    tables = pd.read_html(path)
    release_like_tables = []
    for table in tables:
        columns = [normalize_text(col) for col in table.columns]
        if {"Date", "Time", "Release"}.issubset(columns):
            release_like_tables.append(table)

    if not release_like_tables:
        return {
            "status": "FAIL",
            "detail": "Could not parse a Date/Time/Release table from the BLS current-year calendar HTML.",
        }

    parsed_rows = 0
    for table in release_like_tables:
        frame = table.copy()
        frame.columns = [normalize_text(col) for col in frame.columns]
        frame = frame[["Date", "Time", "Release"]]
        frame = frame[
            ~(frame["Date"].map(normalize_text) == "Date")
            & ~(frame["Release"].map(normalize_text) == "Release")
        ]
        frame = frame[frame["Date"].map(normalize_text) != ""]
        frame = frame[frame["Time"].map(normalize_text) != ""]
        parsed_rows += int(len(frame))

    if parsed_rows == 0:
        return {
            "status": "FAIL",
            "detail": "BLS current-year calendar tables were found but yielded no usable release rows.",
        }

    return {
        "status": "PASS",
        "detail": "The BLS current-year calendar HTML contains parseable dated release rows.",
        "row_count": parsed_rows,
    }


def inspect_bls_employment_calendar(path: Path):
    tables = pd.read_html(path)
    for table in tables:
        columns = [normalize_text(col) for col in table.columns]
        if {"Reference Month", "Release Date", "Release Time"}.issubset(columns):
            frame = table.copy()
            frame.columns = columns
            frame = frame[["Reference Month", "Release Date", "Release Time"]]
            frame = frame.dropna(how="all")
            if not frame.empty:
                return {
                    "status": "PASS",
                    "detail": "The Employment Situation calendar HTML contains a parseable schedule table.",
                    "row_count": int(len(frame)),
                }

    return {
        "status": "FAIL",
        "detail": "Could not parse the Employment Situation schedule table from the BLS HTML.",
    }


def inspect_fed_g17_calendar(path: Path):
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    pre_block = soup.find("pre")
    if pre_block is None:
        return {
            "status": "FAIL",
            "detail": "The Federal Reserve G.17 HTML does not contain the expected preformatted release-date block.",
        }

    parseable_rows = 0
    invalid_rows = []
    pattern = re.compile(r"^(?P<reference>[A-Za-z]+ \d{4})\s+(?P<release_dates>.+)$")
    for line in pre_block.get_text("\n").splitlines():
        cleaned = normalize_text(line)
        if not cleaned:
            continue
        match = pattern.match(cleaned)
        if match is None:
            invalid_rows.append(cleaned)
            continue
        parseable_rows += 1

    if parseable_rows == 0:
        return {
            "status": "FAIL",
            "detail": "The Federal Reserve G.17 HTML contains the pre block but no parseable release-date rows.",
            "sample_invalid_rows": invalid_rows[:5],
        }

    return {
        "status": "PASS",
        "detail": "The Federal Reserve G.17 HTML contains parseable historical and scheduled release-date rows.",
        "row_count": parseable_rows,
        "sample_invalid_rows": invalid_rows[:5],
    }


def inspect_census_proxy_artifacts():
    events_path = PROJECT_ROOT / "data" / "raw" / "calendars" / "census" / "census_proxy_release_events.csv"
    calendar_path = PROJECT_ROOT / "data" / "raw" / "calendars" / "census" / "census_proxy_release_calendar.csv"
    metadata_path = PROJECT_ROOT / "data" / "raw" / "calendars" / "census" / "census_proxy_calendar_metadata.json"

    missing = [
        relative_path(path)
        for path in [events_path, calendar_path, metadata_path]
        if not path.exists()
    ]
    if missing:
        return {
            "status": "FAIL",
            "detail": "One or more Census proxy artifacts are missing.",
            "missing_paths": missing,
        }

    events = pd.read_csv(events_path, dtype="string")
    calendar = pd.read_csv(calendar_path, dtype="string")
    metadata = load_json(metadata_path)

    if events.empty or calendar.empty:
        return {
            "status": "FAIL",
            "detail": "The Census proxy artifacts exist but contain no rows.",
        }

    if metadata.get("status") != "proxy_not_official":
        return {
            "status": "FAIL",
            "detail": "The Census proxy metadata does not explicitly label the calendar as a proxy.",
        }

    event_times_blank = events["release_time_et"].fillna("").map(normalize_text).eq("").all()
    calendar_times_blank = calendar["release_time_et"].fillna("").map(normalize_text).eq("").all()
    if not (event_times_blank and calendar_times_blank):
        return {
            "status": "FAIL",
            "detail": "Census proxy artifacts must leave release_time_et blank to avoid fake intraday timestamps.",
        }

    notes_text = " ".join(metadata.get("important_notes", []))
    if "NOT an official Census release calendar" not in notes_text:
        return {
            "status": "FAIL",
            "detail": "The Census proxy metadata does not clearly state that the calendar is not official.",
        }

    return {
        "status": "PASS",
        "detail": "Census proxy artifacts are present and preserve the proxy-not-official distinction honestly.",
        "series_event_rows": int(len(events)),
        "block_calendar_rows": int(len(calendar)),
    }


def build_results():
    manifest = load_manifest()

    structural_checks = {
        "required_root_files": check_paths(manifest.get("required_root_files", [])),
        "required_stage0_scripts": check_required_scripts(),
        "required_directories": check_paths(manifest.get("required_directories", [])),
        "required_rtdsm_files": check_paths(manifest.get("required_rtdsm_files", [])),
        "optional_rtdsm_files": check_paths(manifest.get("optional_rtdsm_files", [])),
        "required_bea_files": check_paths(manifest.get("required_bea_files", [])),
        "required_calendar_files": check_paths(manifest.get("required_calendar_files", [])),
        "optional_calendar_files": check_paths(manifest.get("optional_calendar_files", [])),
        "recommended_calendar_files": check_paths(manifest.get("recommended_calendar_files", [])),
        "requirements_file": check_requirements_file(),
        "env_template": check_env_template(),
        "gitignore": check_gitignore(),
    }

    semantic_checks = {
        "rtdsm_release_specific": inspect_rtdsm_release_specific(
            PROJECT_ROOT / "data" / "raw" / "rtdsm" / "routput" / "routput_first_second_third.xlsx"
        ),
        "rtdsm_complete_vintages": inspect_rtdsm_complete_vintages(
            PROJECT_ROOT / "data" / "raw" / "rtdsm" / "routput" / "ROUTPUTQvQd.xlsx"
        ),
        "rtdsm_optional_monthly_vintages": inspect_rtdsm_optional_monthly(
            PROJECT_ROOT / "data" / "raw" / "rtdsm" / "routput" / "routputMvQd.xlsx"
        ),
        "bea_calendar": inspect_bea_calendar(
            PROJECT_ROOT / "data" / "raw" / "calendars" / "bea" / "full_release_schedule.html"
        ),
        "bls_current_year_calendar": inspect_bls_current_year_calendar(
            PROJECT_ROOT / "data" / "raw" / "calendars" / "bls" / "current_year.html"
        ),
        "bls_employment_calendar": inspect_bls_employment_calendar(
            PROJECT_ROOT / "data" / "raw" / "calendars" / "bls" / "employment_situation.html"
        ),
        "fed_g17_calendar": inspect_fed_g17_calendar(
            PROJECT_ROOT / "data" / "raw" / "calendars" / "fed_g17" / "release_dates.html"
        ),
        "census_proxy": inspect_census_proxy_artifacts(),
        "bea_api_tables": {
            filename: inspect_bea_file(PROJECT_ROOT / filename)
            for filename in manifest.get("required_bea_files", [])
        },
        "required_alfred_series": {
            series_id: inspect_alfred_series(series_id)
            for series_id in manifest.get("required_alfred_series", [])
        },
        "optional_alfred_series": {
            series_id: inspect_alfred_series(series_id)
            for series_id in manifest.get("optional_alfred_series", [])
            if (PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations" / f"{series_id}.csv").exists()
        },
    }

    return structural_checks, semantic_checks


def collect_failures(structural_checks, semantic_checks):
    failures = []
    required_path_groups = {
        "required_root_files",
        "required_stage0_scripts",
        "required_directories",
        "required_rtdsm_files",
        "required_bea_files",
        "required_calendar_files",
    }

    for check_name, result in structural_checks.items():
        if check_name in required_path_groups and isinstance(result, dict) and "missing" in result and result["missing"]:
            failures.append(f"{check_name}: missing {len(result['missing'])} required path(s).")
        elif result.get("status") == "FAIL":
            failures.append(f"{check_name}: {result.get('detail', 'failed')}")

    for check_name, result in semantic_checks.items():
        if check_name in {"bea_api_tables", "required_alfred_series", "optional_alfred_series"}:
            for nested_name, nested_result in result.items():
                if nested_result.get("status") == "FAIL":
                    failures.append(f"{check_name}.{nested_name}: {nested_result.get('detail', 'failed')}")
        elif result.get("status") == "FAIL":
            failures.append(f"{check_name}: {result.get('detail', 'failed')}")

    return failures


def collect_warnings(structural_checks, semantic_checks):
    warnings = []

    optional_calendar_missing = structural_checks["optional_calendar_files"]["missing"]
    if optional_calendar_missing:
        warnings.append(
            "Official Census HTML is optional and currently unavailable; the ALFRED-based proxy calendar remains the timing source."
        )

    optional_alfred_missing = {"SP500", "NAPM"} - set(semantic_checks["optional_alfred_series"].keys())
    if optional_alfred_missing:
        warnings.append(
            "Optional ALFRED series are not present; this does not block Stage 0 or Stage 1."
        )

    release_unit_line = semantic_checks["rtdsm_release_specific"].get("unit_line", "")
    if "Growth" in release_unit_line:
        warnings.append(
            "RTDSM release-specific targets are explicitly q/q annualized growth rates, while the complete-vintage RTDSM matrix is a separate level-like numeric history; downstream stages must not silently treat them as identical measures."
        )

    bea_scope_year = semantic_checks["bea_calendar"].get("declared_scope_year")
    if bea_scope_year:
        warnings.append(
            f"BEA official calendar raw coverage currently appears to be a current-year snapshot ({bea_scope_year}), not a full historical archive."
        )

    if semantic_checks["bls_current_year_calendar"].get("status") == "PASS":
        warnings.append(
            "BLS official calendar raw coverage currently reflects current-year and scheduled pages, not a completed historical archive."
        )

    if semantic_checks["fed_g17_calendar"].get("status") == "PASS":
        warnings.append(
            "Federal Reserve G.17 release dates are official, but the raw page does not publish intraday release times; no time should be fabricated downstream."
        )

    return warnings


def main():
    structural_checks, semantic_checks = build_results()
    failures = collect_failures(structural_checks, semantic_checks)
    warnings = collect_warnings(structural_checks, semantic_checks)

    status = "PASS" if not failures else "FAIL"

    report = {
        "stage": "stage_0",
        "status": status,
        "project_root": str(PROJECT_ROOT),
        "structural_checks": structural_checks,
        "semantic_checks": semantic_checks,
        "warnings": warnings,
        "hard_failures": failures,
    }
    write_json(REPORT_PATH, report)

    print(f"Stage 0 validation status: {status}")

    if failures:
        print("\nHard failures:")
        for item in failures:
            print(f"  - {item}")

    if warnings:
        print("\nWarnings:")
        for item in warnings:
            print(f"  - {item}")

    print(f"\nValidation report written to: {REPORT_PATH}")

    if status != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
