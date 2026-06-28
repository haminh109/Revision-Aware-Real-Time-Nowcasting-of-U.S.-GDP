from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

TARGET_DEFINITION_PATH = PROJECT_ROOT / "data" / "silver" / "targets" / "target_definition_table.csv"
INDICATOR_METADATA_PATH = PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_metadata.csv"
INDICATOR_RELEASE_MAP_PATH = PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_release_map.csv"
CALENDAR_COVERAGE_PATH = PROJECT_ROOT / "data" / "silver" / "calendars" / "calendar_coverage_metadata.csv"

BRONZE_RELEASE_STAGE_PATH = PROJECT_ROOT / "data" / "bronze" / "targets" / "gdp_release_targets.csv"
BRONZE_COMPLETE_VINTAGES_PATH = PROJECT_ROOT / "data" / "bronze" / "targets" / "gdp_complete_vintages_long.csv"
BRONZE_CALENDAR_PATH = PROJECT_ROOT / "data" / "bronze" / "calendars" / "release_calendar_master.csv"

RELEASE_STAGE_OUTPUT = PROJECT_ROOT / "data" / "silver" / "targets" / "gdp_release_stage_silver.csv"
COMPLETE_VINTAGES_OUTPUT = PROJECT_ROOT / "data" / "silver" / "targets" / "gdp_complete_vintages_silver.csv"
CALENDAR_SILVER_OUTPUT = PROJECT_ROOT / "data" / "silver" / "calendars" / "release_calendar_silver.csv"
LIMITATIONS_OUTPUT = PROJECT_ROOT / "data" / "silver" / "governance" / "source_limitations_registry.csv"


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def build_target_lookup():
    target_definitions = pd.read_csv(TARGET_DEFINITION_PATH, dtype="string", keep_default_na=False)
    lookup = {}
    for row in target_definitions.itertuples(index=False):
        lookup[row.source_dataset] = {
            "canonical_target_id": row.canonical_target_id,
            "target_family": row.target_family,
            "target_object_type": row.target_object_type,
            "measurement_semantics": row.measurement_semantics,
            "unit_semantics": row.unit_semantics,
            "release_structure_type": row.release_structure_type,
            "comparability_group": row.comparability_group,
            "is_real_time_release_target": row.is_real_time_release_target,
            "is_revision_history_target": row.is_revision_history_target,
        }
    return lookup


def build_release_stage_silver(target_lookup):
    bronze = pd.read_csv(BRONZE_RELEASE_STAGE_PATH, dtype="string", keep_default_na=False)
    lookup = target_lookup["routput_first_second_third"]

    frame = bronze.copy()
    frame.insert(0, "canonical_target_id", lookup["canonical_target_id"])
    frame.insert(1, "target_family", lookup["target_family"])
    frame.insert(2, "target_object_type", lookup["target_object_type"])
    frame.insert(3, "measurement_semantics", lookup["measurement_semantics"])
    frame.insert(4, "unit_semantics", lookup["unit_semantics"])
    frame.insert(5, "release_structure_type", lookup["release_structure_type"])
    frame.insert(6, "comparability_group", lookup["comparability_group"])
    frame.insert(7, "is_real_time_release_target", lookup["is_real_time_release_target"])
    frame.insert(8, "is_revision_history_target", lookup["is_revision_history_target"])

    numeric_columns = ["target_year", "target_quarter_number", "release_stage_order", "value"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise")

    frame = frame.sort_values(
        ["target_year", "target_quarter_number", "release_stage_order"],
        kind="stable",
    ).reset_index(drop=True)
    return frame


def build_complete_vintages_silver(target_lookup):
    bronze = pd.read_csv(BRONZE_COMPLETE_VINTAGES_PATH, dtype="string", keep_default_na=False)
    lookup = target_lookup["ROUTPUTQvQd"]

    frame = bronze.copy()
    frame.insert(0, "canonical_target_id", lookup["canonical_target_id"])
    frame.insert(1, "target_family", lookup["target_family"])
    frame.insert(2, "target_object_type", lookup["target_object_type"])
    frame.insert(3, "measurement_semantics", lookup["measurement_semantics"])
    frame.insert(4, "unit_semantics", lookup["unit_semantics"])
    frame.insert(5, "release_structure_type", lookup["release_structure_type"])
    frame.insert(6, "comparability_group", lookup["comparability_group"])
    frame.insert(7, "is_real_time_release_target", lookup["is_real_time_release_target"])
    frame.insert(8, "is_revision_history_target", lookup["is_revision_history_target"])

    numeric_columns = [
        "target_year",
        "target_quarter_number",
        "vintage_year",
        "vintage_quarter_number",
        "value",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="raise")

    frame = frame.sort_values(
        ["target_year", "target_quarter_number", "vintage_year", "vintage_quarter_number"],
        kind="stable",
    ).reset_index(drop=True)
    return frame


def build_release_calendar_silver():
    bronze = pd.read_csv(BRONZE_CALENDAR_PATH, dtype="string", keep_default_na=False)
    coverage = pd.read_csv(CALENDAR_COVERAGE_PATH, dtype="string", keep_default_na=False)

    merged = bronze.merge(
        coverage[
            [
                "source_family",
                "source_subsource",
                "source_type",
                "coverage_status",
            ]
        ],
        on=["source_family", "source_subsource", "source_type"],
        how="left",
        validate="many_to_one",
    )
    if merged["coverage_status"].eq("").any():
        missing_rows = merged.loc[merged["coverage_status"].eq(""), ["source_family", "source_subsource"]]
        raise ValueError(
            "Missing calendar coverage metadata for bronze calendar rows: "
            f"{missing_rows.drop_duplicates().to_dict(orient='records')}"
        )

    merged.insert(0, "canonical_event_id", merged["event_id"].map(lambda value: f"canonical__{value}"))
    merged = merged.rename(columns={"event_id": "bronze_event_id"})
    merged = merged[
        [
            "canonical_event_id",
            "bronze_event_id",
            "source_family",
            "source_type",
            "source_subsource",
            "coverage_scope",
            "coverage_status",
            "release_block",
            "release_name",
            "reference_period_label",
            "release_date",
            "release_time_et",
            "release_time_status",
            "included_series",
            "proxy_method",
            "provenance_file",
            "notes",
        ]
    ]
    merged = merged.sort_values(
        ["release_date", "release_time_et", "source_family", "release_block", "release_name", "reference_period_label"],
        kind="stable",
    ).reset_index(drop=True)
    return merged


def build_limitations_registry():
    indicator_metadata = pd.read_csv(INDICATOR_METADATA_PATH, dtype="string", keep_default_na=False)
    indicator_release_map = pd.read_csv(INDICATOR_RELEASE_MAP_PATH, dtype="string", keep_default_na=False)
    coverage = pd.read_csv(CALENDAR_COVERAGE_PATH, dtype="string", keep_default_na=False)

    unmapped_series = sorted(
        indicator_release_map.loc[
            indicator_release_map["source_type"] == "unmapped",
            "series_id",
        ].unique().tolist()
    )

    bea_note = coverage.loc[
        (coverage["source_family"] == "BEA") & (coverage["source_subsource"] == "full_release_schedule"),
        "downstream_usage_constraint",
    ].iloc[0]
    bls_constraints = coverage.loc[
        coverage["source_family"] == "BLS",
        "downstream_usage_constraint",
    ].tolist()
    fed_note = coverage.loc[
        (coverage["source_family"] == "FED_G17") & (coverage["source_subsource"] == "release_dates"),
        "downstream_usage_constraint",
    ].iloc[0]
    census_note = coverage.loc[
        (coverage["source_family"] == "CENSUS_PROXY") & (coverage["source_subsource"] == "alfred_proxy_calendar"),
        "downstream_usage_constraint",
    ].iloc[0]

    rows = [
        {
            "limitation_id": "targets_release_stage_and_complete_vintage_not_interchangeable",
            "domain": "targets",
            "affected_artifact": "data/silver/targets/target_definition_table.csv",
            "severity": "high",
            "limitation_summary": "RTDSM release-stage GDP growth targets and RTDSM complete-vintage GDP history are distinct target objects and are not directly interchangeable.",
            "downstream_constraint": "Stage 3 must keep these targets in separate comparability groups and must not merge them into one forecasting target without an explicit target-definition choice.",
            "status": "active_documented",
            "notes": "Documented in Stage 2 target definitions and propagated into both silver target tables.",
        },
        {
            "limitation_id": "targets_release_stage_missing_row_level_release_dates",
            "domain": "targets",
            "affected_artifact": "data/silver/targets/gdp_release_stage_silver.csv",
            "severity": "high",
            "limitation_summary": "The RTDSM release-stage workbook does not provide row-level release dates for each target-quarter/release-stage row.",
            "downstream_constraint": "Stage 3 must keep release_date blank for these rows and rely on separately curated calendar logic rather than fabricating target-row timestamps.",
            "status": "active_documented",
            "notes": "The silver release-stage table preserves blank release_date and the original timing limitation note from bronze.",
        },
        {
            "limitation_id": "targets_complete_vintage_has_quarter_coded_not_daily_vintages",
            "domain": "targets",
            "affected_artifact": "data/silver/targets/gdp_complete_vintages_silver.csv",
            "severity": "high",
            "limitation_summary": "The RTDSM complete-vintage history uses quarter-coded vintage labels rather than exact daily vintage dates.",
            "downstream_constraint": "Stage 3 must not fabricate daily vintage timestamps from this object; any date-level alignment must come from a separate defensible mapping layer.",
            "status": "active_documented",
            "notes": "Silver preserves vintage_period and vintage_label_raw exactly as quarter-coded identifiers.",
        },
        {
            "limitation_id": "calendars_census_proxy_not_official",
            "domain": "calendars",
            "affected_artifact": "data/silver/calendars/release_calendar_silver.csv;data/silver/calendars/calendar_coverage_metadata.csv",
            "severity": "high",
            "limitation_summary": "Census-related release timing is proxy-only and derived from ALFRED availability dates rather than official Census timestamps.",
            "downstream_constraint": census_note,
            "status": "active_documented",
            "notes": "Proxy rows remain labeled as proxy, retain blank release_time_et, and are separated from official calendar rows.",
        },
        {
            "limitation_id": "calendars_bea_snapshot_not_complete_archive",
            "domain": "calendars",
            "affected_artifact": "data/silver/calendars/calendar_coverage_metadata.csv;data/silver/calendars/release_calendar_silver.csv",
            "severity": "high",
            "limitation_summary": "The current BEA calendar artifact in the repo is a current-year snapshot, not a completed historical archive.",
            "downstream_constraint": bea_note,
            "status": "active_documented",
            "notes": "Coverage metadata records this explicitly so Stage 3 does not overclaim exact historical completeness.",
        },
        {
            "limitation_id": "calendars_bls_snapshot_not_complete_archive",
            "domain": "calendars",
            "affected_artifact": "data/silver/calendars/calendar_coverage_metadata.csv;data/silver/calendars/release_calendar_silver.csv",
            "severity": "high",
            "limitation_summary": "The current BLS calendar artifacts in the repo are snapshot/scheduled-page style and not a completed historical archive.",
            "downstream_constraint": " ".join(bls_constraints),
            "status": "active_documented",
            "notes": "Both the general current-year page and the Employment Situation schedule page are preserved as official but coverage-limited sources.",
        },
        {
            "limitation_id": "calendars_fed_g17_has_date_but_no_official_intraday_time",
            "domain": "calendars",
            "affected_artifact": "data/silver/calendars/release_calendar_silver.csv;data/silver/calendars/calendar_coverage_metadata.csv",
            "severity": "medium",
            "limitation_summary": "The Federal Reserve G.17 release page provides official release dates but not official intraday release times in the current repo artifacts.",
            "downstream_constraint": fed_note,
            "status": "active_documented",
            "notes": "Silver keeps release_time_et blank for these official date-only rows.",
        },
        {
            "limitation_id": "indicators_currently_unmapped_release_calendar_support",
            "domain": "indicators",
            "affected_artifact": "data/silver/indicators/indicator_metadata.csv;data/silver/indicators/indicator_release_map.csv",
            "severity": "medium",
            "limitation_summary": "Some Stage 1 indicators currently lack a defensible release-calendar mapping in the repo.",
            "downstream_constraint": "Stage 3 must either exclude unmapped indicators from release-event joins or add an explicit timing-source expansion before using them in exact-vintage daily information sets.",
            "status": "active_documented",
            "notes": f"Affected series: {';'.join(unmapped_series)}",
        },
    ]

    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["domain", "limitation_id"], kind="stable").reset_index(drop=True)
    return frame


def main():
    target_lookup = build_target_lookup()

    RELEASE_STAGE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    COMPLETE_VINTAGES_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    CALENDAR_SILVER_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    LIMITATIONS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    release_stage_silver = build_release_stage_silver(target_lookup)
    complete_vintages_silver = build_complete_vintages_silver(target_lookup)
    release_calendar_silver = build_release_calendar_silver()
    limitations_registry = build_limitations_registry()

    release_stage_silver.to_csv(RELEASE_STAGE_OUTPUT, index=False)
    complete_vintages_silver.to_csv(COMPLETE_VINTAGES_OUTPUT, index=False)
    release_calendar_silver.to_csv(CALENDAR_SILVER_OUTPUT, index=False)
    limitations_registry.to_csv(LIMITATIONS_OUTPUT, index=False)

    print(f"[OK] Wrote {len(release_stage_silver)} rows -> {RELEASE_STAGE_OUTPUT}")
    print(f"[OK] Wrote {len(complete_vintages_silver)} rows -> {COMPLETE_VINTAGES_OUTPUT}")
    print(f"[OK] Wrote {len(release_calendar_silver)} rows -> {CALENDAR_SILVER_OUTPUT}")
    print(f"[OK] Wrote {len(limitations_registry)} rows -> {LIMITATIONS_OUTPUT}")


if __name__ == "__main__":
    main()
