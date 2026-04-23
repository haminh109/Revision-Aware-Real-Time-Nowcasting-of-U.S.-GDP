import csv
import json
import re
import sys
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGE0_MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage0_manifest.json"
STAGE1_MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage1_manifest.json"
REPORT_PATH = PROJECT_ROOT / "data" / "metadata" / "stage1_validation_report.json"

QUARTER_LABEL_PATTERN = re.compile(r"^\d{4}:Q[1-4]$")
TIME_PATTERN = re.compile(r"^\d{2}:\d{2}$")


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def is_blank(value) -> bool:
    return normalize_text(value) == ""


def load_manifest():
    return load_json(STAGE1_MANIFEST_PATH)


def load_stage0_manifest():
    return load_json(STAGE0_MANIFEST_PATH)


@lru_cache(maxsize=50000)
def parse_iso_date_cached(value: str):
    return date.fromisoformat(value)


def is_dataframe_sorted(frame: pd.DataFrame, sort_keys):
    previous = None
    for row in frame[sort_keys].itertuples(index=False, name=None):
        current = tuple("" if item is None else str(item) for item in row)
        if previous is not None and current < previous:
            return False
        previous = current
    return True


def check_required_columns(columns, required_columns):
    missing = [column for column in required_columns if column not in columns]
    return missing


def validate_small_artifact(name: str, spec):
    path = PROJECT_ROOT / spec["path"]
    result = {
        "path": spec["path"],
        "status": "PASS",
        "row_count": 0,
        "issues": [],
    }

    if not path.exists():
        result["status"] = "FAIL"
        result["issues"].append("Artifact file is missing.")
        return result

    frame = pd.read_csv(path, dtype="string", keep_default_na=False)
    result["row_count"] = int(len(frame))
    if frame.empty:
        result["status"] = "FAIL"
        result["issues"].append("Artifact contains zero rows.")
        return result

    missing_columns = check_required_columns(frame.columns.tolist(), spec["required_columns"])
    if missing_columns:
        result["status"] = "FAIL"
        result["issues"].append(f"Missing required columns: {missing_columns}")
        return result

    duplicate_rows = int(frame.duplicated().sum())
    if duplicate_rows:
        result["status"] = "FAIL"
        result["issues"].append(f"Found {duplicate_rows} duplicate full rows.")

    duplicate_key_rows = int(frame.duplicated(spec["unique_key"]).sum())
    if duplicate_key_rows:
        result["status"] = "FAIL"
        result["issues"].append(
            f"Found {duplicate_key_rows} duplicate rows at the intended grain {spec['unique_key']}."
        )

    for column in spec.get("non_null_columns", []):
        blank_count = int(frame[column].map(is_blank).sum())
        if blank_count:
            result["status"] = "FAIL"
            result["issues"].append(f"Column {column} has {blank_count} blank values but is required to be non-null.")

    for column in spec.get("numeric_columns", []):
        invalid_mask = frame[column].map(is_blank) | pd.to_numeric(frame[column], errors="coerce").notna()
        invalid_count = int((~invalid_mask).sum())
        if invalid_count:
            result["status"] = "FAIL"
            result["issues"].append(f"Column {column} has {invalid_count} non-numeric values.")

    for column in spec.get("date_columns", []):
        non_blank = frame.loc[~frame[column].map(is_blank), column]
        invalid_count = 0
        for value in non_blank:
            try:
                parse_iso_date_cached(str(value))
            except Exception:
                invalid_count += 1
        if invalid_count:
            result["status"] = "FAIL"
            result["issues"].append(f"Column {column} has {invalid_count} invalid ISO date values.")

    for column in spec.get("quarter_label_columns", []):
        invalid_count = int(frame.loc[~frame[column].str.match(QUARTER_LABEL_PATTERN), column].shape[0])
        if invalid_count:
            result["status"] = "FAIL"
            result["issues"].append(f"Column {column} has {invalid_count} invalid quarter labels.")

    if not is_dataframe_sorted(frame, spec["sort_keys"]):
        result["status"] = "FAIL"
        result["issues"].append(f"Artifact is not deterministically sorted by {spec['sort_keys']}.")

    if name == "gdp_release_targets":
        allowed_stages = {"first", "second", "third", "most_recent"}
        invalid_stage_count = int((~frame["release_stage"].isin(allowed_stages)).sum())
        if invalid_stage_count:
            result["status"] = "FAIL"
            result["issues"].append(f"release_stage has {invalid_stage_count} unexpected values.")

        non_blank_release_dates = int((~frame["release_date"].map(is_blank)).sum())
        if non_blank_release_dates:
            result["status"] = "FAIL"
            result["issues"].append("release_date should remain blank in the RTDSM release-stage bronze file.")

        invalid_status_count = int((frame["release_date_status"] != "not_provided_in_raw_file").sum())
        if invalid_status_count:
            result["status"] = "FAIL"
            result["issues"].append("release_date_status must be not_provided_in_raw_file for all rows.")

    if name == "gdp_complete_vintages_long":
        invalid_vintage_label_count = int(
            frame.loc[~frame["vintage_label_raw"].str.match(r"^ROUTPUT\d{2}Q[1-4]$"), "vintage_label_raw"].shape[0]
        )
        if invalid_vintage_label_count:
            result["status"] = "FAIL"
            result["issues"].append("vintage_label_raw contains unexpected RTDSM vintage labels.")

    if name == "release_calendar_master":
        allowed_source_types = {"official", "proxy"}
        invalid_source_types = int((~frame["source_type"].isin(allowed_source_types)).sum())
        if invalid_source_types:
            result["status"] = "FAIL"
            result["issues"].append(f"source_type contains {invalid_source_types} unexpected values.")

        allowed_time_statuses = {"official_published", "official_not_published", "proxy_not_official"}
        invalid_time_statuses = int((~frame["release_time_status"].isin(allowed_time_statuses)).sum())
        if invalid_time_statuses:
            result["status"] = "FAIL"
            result["issues"].append(f"release_time_status contains {invalid_time_statuses} unexpected values.")

        invalid_time_formats = int(
            frame.loc[
                ~frame["release_time_et"].map(is_blank) & ~frame["release_time_et"].str.match(TIME_PATTERN),
                "release_time_et",
            ].shape[0]
        )
        if invalid_time_formats:
            result["status"] = "FAIL"
            result["issues"].append(f"release_time_et has {invalid_time_formats} invalid HH:MM values.")

        proxy_rows = frame["source_type"] == "proxy"
        proxy_time_count = int((~frame.loc[proxy_rows, "release_time_et"].map(is_blank)).sum())
        proxy_status_count = int((frame.loc[proxy_rows, "release_time_status"] != "proxy_not_official").sum())
        proxy_method_blank_count = int(frame.loc[proxy_rows, "proxy_method"].map(is_blank).sum())
        if proxy_time_count:
            result["status"] = "FAIL"
            result["issues"].append("Proxy calendar rows must keep release_time_et blank.")
        if proxy_status_count:
            result["status"] = "FAIL"
            result["issues"].append("Proxy calendar rows must use release_time_status = proxy_not_official.")
        if proxy_method_blank_count:
            result["status"] = "FAIL"
            result["issues"].append("Proxy calendar rows must retain proxy_method provenance.")

        official_proxy_status_count = int(
            ((frame["source_type"] == "official") & (frame["release_time_status"] == "proxy_not_official")).sum()
        )
        if official_proxy_status_count:
            result["status"] = "FAIL"
            result["issues"].append("Official rows cannot use the proxy_not_official release_time_status.")

    return result


def validate_alfred_monthly(spec, required_series):
    path = PROJECT_ROOT / spec["path"]
    result = {
        "path": spec["path"],
        "status": "PASS",
        "row_count": 0,
        "issues": [],
        "series_seen": [],
    }

    if not path.exists():
        result["status"] = "FAIL"
        result["issues"].append("Artifact file is missing.")
        return result

    with open(path, "r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        header = reader.fieldnames or []
        missing_columns = check_required_columns(header, spec["required_columns"])
        if missing_columns:
            result["status"] = "FAIL"
            result["issues"].append(f"Missing required columns: {missing_columns}")
            return result

        previous_key = None
        seen_series = set()
        duplicate_key_count = 0
        sort_violation_count = 0
        non_null_failures = {column: 0 for column in spec["non_null_columns"]}
        invalid_date_counts = {column: 0 for column in spec["date_columns"]}
        invalid_realtime_range_count = 0
        invalid_series_frequency_count = 0
        invalid_raw_schema_count = 0
        invalid_realtime_end_source_count = 0
        invalid_source_family_count = 0
        invalid_missing_flag_count = 0
        missing_value_logic_count = 0
        invalid_numeric_count = 0

        for row in reader:
            result["row_count"] += 1

            for column in spec["non_null_columns"]:
                if is_blank(row[column]):
                    non_null_failures[column] += 1

            key = (
                row["series_id"],
                row["observation_date"],
                row["realtime_start"],
            )
            if previous_key is not None:
                if key < previous_key:
                    sort_violation_count += 1
                if key == previous_key:
                    duplicate_key_count += 1
            previous_key = key
            seen_series.add(row["series_id"])

            parsed_dates = {}
            for column in spec["date_columns"]:
                value = row[column]
                try:
                    parsed_dates[column] = parse_iso_date_cached(value)
                except Exception:
                    invalid_date_counts[column] += 1

            if {"realtime_start", "realtime_end"}.issubset(parsed_dates):
                if parsed_dates["realtime_end"] < parsed_dates["realtime_start"]:
                    invalid_realtime_range_count += 1

            if row["source_family"] != "ALFRED":
                invalid_source_family_count += 1

            if row["series_frequency"] not in {"monthly", "quarterly"}:
                invalid_series_frequency_count += 1

            if row["raw_schema"] not in {"wide_vintage_matrix", "long_observation_rows"}:
                invalid_raw_schema_count += 1

            if row["realtime_end_source"] not in {"derived_from_vintage_dates", "raw_observations_file"}:
                invalid_realtime_end_source_count += 1

            missing_flag = row["is_missing_value"].strip().lower()
            if missing_flag not in {"true", "false"}:
                invalid_missing_flag_count += 1
                continue

            is_missing_value = missing_flag == "true"
            value_raw = row["value_raw"].strip()
            value_numeric = row["value_numeric"].strip()

            if is_missing_value:
                if value_numeric:
                    missing_value_logic_count += 1
            else:
                if not value_numeric:
                    missing_value_logic_count += 1
                else:
                    try:
                        float(value_numeric)
                    except Exception:
                        invalid_numeric_count += 1

            if not is_missing_value and value_raw == ".":
                missing_value_logic_count += 1

        if result["row_count"] == 0:
            result["status"] = "FAIL"
            result["issues"].append("Artifact contains zero rows.")
            return result

        missing_required_series = sorted(required_series - seen_series)
        if missing_required_series:
            result["status"] = "FAIL"
            result["issues"].append(f"Missing required ALFRED series in bronze output: {missing_required_series}")

        if duplicate_key_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {duplicate_key_count} duplicate rows at the intended grain {spec['unique_key']}."
            )
        if sort_violation_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {sort_violation_count} sort-order violations against {spec['sort_keys']}."
            )

        for column, count in non_null_failures.items():
            if count:
                result["status"] = "FAIL"
                result["issues"].append(f"Column {column} has {count} blank values but is required to be non-null.")

        for column, count in invalid_date_counts.items():
            if count:
                result["status"] = "FAIL"
                result["issues"].append(f"Column {column} has {count} invalid ISO date values.")

        if invalid_realtime_range_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {invalid_realtime_range_count} rows where realtime_end is earlier than realtime_start."
            )
        if invalid_series_frequency_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {invalid_series_frequency_count} rows with unexpected series_frequency values."
            )
        if invalid_raw_schema_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {invalid_raw_schema_count} rows with unexpected raw_schema values."
            )
        if invalid_realtime_end_source_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {invalid_realtime_end_source_count} rows with unexpected realtime_end_source values."
            )
        if invalid_source_family_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {invalid_source_family_count} rows with source_family different from ALFRED."
            )
        if invalid_missing_flag_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {invalid_missing_flag_count} rows with invalid is_missing_value flags."
            )
        if missing_value_logic_count:
            result["status"] = "FAIL"
            result["issues"].append(
                f"Found {missing_value_logic_count} rows violating missing-value/value_numeric consistency."
            )
        if invalid_numeric_count:
            result["status"] = "FAIL"
            result["issues"].append(f"Found {invalid_numeric_count} rows with invalid numeric values.")

        result["series_seen"] = sorted(seen_series)

    return result


def validate_required_scripts(script_paths):
    present = []
    missing = []
    for rel_path in script_paths:
        path = PROJECT_ROOT / rel_path
        if path.exists():
            present.append(rel_path)
        else:
            missing.append(rel_path)
    return {
        "present": present,
        "missing": missing,
        "status": "PASS" if not missing else "FAIL",
    }


def main():
    manifest = load_manifest()
    stage0_manifest = load_stage0_manifest()
    required_series = set(stage0_manifest.get("required_alfred_series", []))

    script_results = validate_required_scripts(manifest.get("required_scripts", []))

    artifact_results = {}
    for name, spec in manifest["artifacts"].items():
        if name == "alfred_monthly_long":
            artifact_results[name] = validate_alfred_monthly(spec, required_series)
        else:
            artifact_results[name] = validate_small_artifact(name, spec)

    failures = []
    if script_results["status"] == "FAIL":
        failures.append(f"Missing required Stage 1 scripts: {script_results['missing']}")

    for name, result in artifact_results.items():
        if result["status"] == "FAIL":
            failures.append(f"{name}: {' | '.join(result['issues'])}")

    status = "PASS" if not failures else "FAIL"
    report = {
        "stage": "stage_1",
        "status": status,
        "project_root": str(PROJECT_ROOT),
        "script_checks": script_results,
        "artifact_checks": artifact_results,
        "hard_failures": failures,
    }
    write_json(REPORT_PATH, report)

    print(f"Stage 1 validation status: {status}")
    if failures:
        print("\nHard failures:")
        for item in failures:
            print(f"  - {item}")
    print(f"\nValidation report written to: {REPORT_PATH}")

    if status != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
