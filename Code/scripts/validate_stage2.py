import json
import sys
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGE0_MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage0_manifest.json"
STAGE2_MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage2_manifest.json"
REPORT_PATH = PROJECT_ROOT / "data" / "metadata" / "stage2_validation_report.json"

CORE_LIMITATION_IDS = {
    "targets_release_stage_and_complete_vintage_not_interchangeable",
    "targets_release_stage_missing_row_level_release_dates",
    "targets_complete_vintage_has_quarter_coded_not_daily_vintages",
    "calendars_census_proxy_not_official",
    "calendars_bea_snapshot_not_complete_archive",
    "calendars_bls_snapshot_not_complete_archive",
    "calendars_fed_g17_has_date_but_no_official_intraday_time",
    "indicators_currently_unmapped_release_calendar_support",
}

EXPECTED_REQUIRED_SERIES = {
    "GDPC1",
    "A191RL1Q225SBEA",
    "PAYEMS",
    "UNRATE",
    "INDPRO",
    "TCU",
    "AWHMAN",
    "W875RX1",
    "DSPIC96",
    "PCECC96",
    "RSAFS",
    "RSXFS",
    "UMCSENT",
    "HOUST",
    "PERMIT",
    "DGORDER",
    "NEWORDER",
    "BUSINV",
    "ISRATIO",
    "BOPGSTB",
    "BOPTEXP",
    "BOPTIMP",
    "FEDFUNDS",
    "TB3MS",
    "GS10",
    "T10Y3MM",
}

QUARTER_LABEL_PATTERN = r"^\d{4}:Q[1-4]$"
TIME_PATTERN = r"^\d{2}:\d{2}$"


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


def check_required_scripts(script_paths):
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


def validate_artifact(name: str, spec):
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

    missing_columns = [column for column in spec["required_columns"] if column not in frame.columns]
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
            f"Found {duplicate_key_rows} duplicate rows at intended grain {spec['unique_key']}."
        )

    for column in spec.get("non_null_columns", []):
        blank_count = int(frame[column].map(is_blank).sum())
        if blank_count:
            result["status"] = "FAIL"
            result["issues"].append(f"Column {column} has {blank_count} blank values.")

    if not is_dataframe_sorted(frame, spec["sort_keys"]):
        result["status"] = "FAIL"
        result["issues"].append(f"Artifact is not deterministically sorted by {spec['sort_keys']}.")

    return result


def run_custom_checks(artifact_results):
    issues = []

    target_defs = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "targets" / "target_definition_table.csv",
        dtype="string",
        keep_default_na=False,
    )
    release_stage = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "targets" / "gdp_release_stage_silver.csv",
        dtype="string",
        keep_default_na=False,
    )
    complete_vintages = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "targets" / "gdp_complete_vintages_silver.csv",
        dtype="string",
        keep_default_na=False,
    )
    indicator_metadata = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_metadata.csv",
        dtype="string",
        keep_default_na=False,
    )
    indicator_release_map = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_release_map.csv",
        dtype="string",
        keep_default_na=False,
    )
    release_block_taxonomy = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "calendars" / "release_block_taxonomy.csv",
        dtype="string",
        keep_default_na=False,
    )
    release_calendar = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "calendars" / "release_calendar_silver.csv",
        dtype="string",
        keep_default_na=False,
    )
    coverage = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "calendars" / "calendar_coverage_metadata.csv",
        dtype="string",
        keep_default_na=False,
    )
    limitations = pd.read_csv(
        PROJECT_ROOT / "data" / "silver" / "governance" / "source_limitations_registry.csv",
        dtype="string",
        keep_default_na=False,
    )

    expected_target_datasets = {"routput_first_second_third", "ROUTPUTQvQd"}
    if set(target_defs["source_dataset"]) != expected_target_datasets:
        issues.append("target_definition_table does not contain exactly the two expected RTDSM target objects.")

    if len(set(target_defs["canonical_target_id"])) != 2:
        issues.append("target_definition_table does not separate the target objects into distinct canonical_target_id values.")

    if len(set(target_defs["target_object_type"])) != 2:
        issues.append("target_definition_table does not separate the target objects into distinct target_object_type values.")

    if len(set(target_defs["comparability_group"])) != 2:
        issues.append("target_definition_table does not separate the target objects into distinct comparability_group values.")

    if int((target_defs["is_real_time_release_target"] == "True").sum()) != 1:
        issues.append("target_definition_table should contain exactly one real-time release target object.")

    if int((target_defs["is_revision_history_target"] == "True").sum()) != 1:
        issues.append("target_definition_table should contain exactly one revision-history target object.")

    valid_target_ids = set(target_defs["canonical_target_id"])
    invalid_release_stage_target_ids = sorted(set(release_stage["canonical_target_id"]).difference(valid_target_ids))
    invalid_complete_target_ids = sorted(set(complete_vintages["canonical_target_id"]).difference(valid_target_ids))
    if invalid_release_stage_target_ids:
        issues.append(f"gdp_release_stage_silver contains unknown canonical_target_id values: {invalid_release_stage_target_ids}")
    if invalid_complete_target_ids:
        issues.append(f"gdp_complete_vintages_silver contains unknown canonical_target_id values: {invalid_complete_target_ids}")

    if (~release_stage["target_quarter"].str.match(QUARTER_LABEL_PATTERN)).any():
        issues.append("gdp_release_stage_silver contains invalid target_quarter labels.")

    if (~complete_vintages["target_quarter"].str.match(QUARTER_LABEL_PATTERN)).any():
        issues.append("gdp_complete_vintages_silver contains invalid target_quarter labels.")

    if (~complete_vintages["vintage_period"].str.match(QUARTER_LABEL_PATTERN)).any():
        issues.append("gdp_complete_vintages_silver contains invalid vintage_period labels.")

    if (~release_stage["release_date"].map(is_blank)).any():
        issues.append("gdp_release_stage_silver should keep release_date blank.")

    if (release_stage["release_date_status"] != "not_provided_in_raw_file").any():
        issues.append("gdp_release_stage_silver must keep release_date_status = not_provided_in_raw_file.")

    missing_required_series = sorted(EXPECTED_REQUIRED_SERIES.difference(set(indicator_metadata["series_id"])))
    if missing_required_series:
        issues.append(f"indicator_metadata is missing required Stage 1 series: {missing_required_series}")

    if len(set(indicator_metadata["series_id"])) != len(indicator_metadata):
        issues.append("indicator_metadata should contain exactly one row per series.")

    if set(indicator_release_map["series_id"]) != set(indicator_metadata["series_id"]):
        issues.append("indicator_release_map and indicator_metadata do not cover the same set of series.")

    allowed_mapping_statuses = {
        "mapped_official_with_coverage_limits",
        "mapped_official_date_only",
        "mapped_proxy",
        "currently_unmapped",
    }
    invalid_mapping_statuses = sorted(set(indicator_metadata["release_block_mapping_status"]).difference(allowed_mapping_statuses))
    if invalid_mapping_statuses:
        issues.append(f"indicator_metadata has unexpected release_block_mapping_status values: {invalid_mapping_statuses}")

    allowed_calendar_support = {
        "official_calendar_supported_with_coverage_limits",
        "official_calendar_supported_date_only",
        "proxy_supported",
        "currently_unmapped",
    }
    invalid_calendar_support = sorted(set(indicator_metadata["calendar_support_type"]).difference(allowed_calendar_support))
    if invalid_calendar_support:
        issues.append(f"indicator_metadata has unexpected calendar_support_type values: {invalid_calendar_support}")

    allowed_map_source_types = {"official", "proxy", "unmapped"}
    invalid_map_source_types = sorted(set(indicator_release_map["source_type"]).difference(allowed_map_source_types))
    if invalid_map_source_types:
        issues.append(f"indicator_release_map has unexpected source_type values: {invalid_map_source_types}")

    observed_or_placeholder_blocks = set(release_block_taxonomy["release_block"])
    missing_release_blocks = sorted(set(indicator_release_map["release_block"]).difference(observed_or_placeholder_blocks))
    if missing_release_blocks:
        issues.append(f"indicator_release_map references release_block values absent from release_block_taxonomy: {missing_release_blocks}")

    if "currently_unmapped" not in observed_or_placeholder_blocks:
        issues.append("release_block_taxonomy must contain the currently_unmapped placeholder block.")

    proxy_rows = release_calendar["source_type"] == "proxy"
    if (~release_calendar.loc[proxy_rows, "release_time_et"].map(is_blank)).any():
        issues.append("release_calendar_silver proxy rows must keep release_time_et blank.")

    if (release_calendar.loc[proxy_rows, "release_time_status"] != "proxy_not_official").any():
        issues.append("release_calendar_silver proxy rows must use release_time_status = proxy_not_official.")

    official_rows = release_calendar["source_type"] == "official"
    if (release_calendar.loc[official_rows, "release_time_status"] == "proxy_not_official").any():
        issues.append("release_calendar_silver official rows cannot be labeled as proxy_not_official.")

    invalid_time_formats = release_calendar.loc[
        ~release_calendar["release_time_et"].map(is_blank) & ~release_calendar["release_time_et"].str.match(TIME_PATTERN),
        "release_time_et",
    ]
    if not invalid_time_formats.empty:
        issues.append("release_calendar_silver contains invalid HH:MM release_time_et values.")

    invalid_release_dates = 0
    for value in release_calendar["release_date"]:
        try:
            parse_iso_date_cached(value)
        except Exception:
            invalid_release_dates += 1
    if invalid_release_dates:
        issues.append(f"release_calendar_silver contains {invalid_release_dates} invalid ISO release_date values.")

    bool_columns = [
        "is_historical_archive_complete",
        "is_current_snapshot_only",
        "has_official_release_time",
        "has_only_release_date",
        "is_proxy",
    ]
    for column in bool_columns:
        invalid_values = sorted(
            {
                value
                for value in coverage[column]
                if value not in {"True", "False"}
            }
        )
        if invalid_values:
            issues.append(f"calendar_coverage_metadata column {column} contains non-boolean values: {invalid_values}")

    bea_rows = coverage[coverage["source_family"] == "BEA"]
    if bea_rows.empty or (bea_rows["is_historical_archive_complete"] == "True").any():
        issues.append("calendar_coverage_metadata must record BEA coverage as not historically complete.")

    bls_rows = coverage[coverage["source_family"] == "BLS"]
    if bls_rows.empty or (bls_rows["is_historical_archive_complete"] == "True").any():
        issues.append("calendar_coverage_metadata must record BLS coverage as not historically complete.")

    census_rows = coverage[coverage["source_family"] == "CENSUS_PROXY"]
    if census_rows.empty:
        issues.append("calendar_coverage_metadata is missing the CENSUS_PROXY row.")
    else:
        if (census_rows["is_proxy"] != "True").any():
            issues.append("calendar_coverage_metadata must record CENSUS_PROXY as proxy.")
        if (census_rows["has_official_release_time"] != "False").any():
            issues.append("calendar_coverage_metadata must record CENSUS_PROXY as lacking official release times.")

    missing_core_limitations = sorted(CORE_LIMITATION_IDS.difference(set(limitations["limitation_id"])))
    if missing_core_limitations:
        issues.append(f"source_limitations_registry is missing core limitation ids: {missing_core_limitations}")

    allowed_severity = {"high", "medium", "low"}
    invalid_severity = sorted(set(limitations["severity"]).difference(allowed_severity))
    if invalid_severity:
        issues.append(f"source_limitations_registry has unexpected severity values: {invalid_severity}")

    allowed_status = {"active_documented"}
    invalid_status = sorted(set(limitations["status"]).difference(allowed_status))
    if invalid_status:
        issues.append(f"source_limitations_registry has unexpected status values: {invalid_status}")

    return issues


def main():
    manifest = load_json(STAGE2_MANIFEST_PATH)
    script_checks = check_required_scripts(manifest["required_scripts"])

    artifact_checks = {}
    for name, spec in manifest["artifacts"].items():
        artifact_checks[name] = validate_artifact(name, spec)

    failures = []
    if script_checks["status"] == "FAIL":
        failures.append(f"Missing required Stage 2 scripts: {script_checks['missing']}")

    for name, result in artifact_checks.items():
        if result["status"] == "FAIL":
            failures.append(f"{name}: {' | '.join(result['issues'])}")

    if not failures:
        custom_issues = run_custom_checks(artifact_checks)
        failures.extend(custom_issues)

    status = "PASS" if not failures else "FAIL"
    report = {
        "stage": "stage_2",
        "status": status,
        "project_root": str(PROJECT_ROOT),
        "script_checks": script_checks,
        "artifact_checks": artifact_checks,
        "hard_failures": failures,
    }
    write_json(REPORT_PATH, report)

    print(f"Stage 2 validation status: {status}")
    if failures:
        print("\nHard failures:")
        for item in failures:
            print(f"  - {item}")
    print(f"\nValidation report written to: {REPORT_PATH}")

    if status != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
