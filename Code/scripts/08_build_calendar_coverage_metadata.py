from pathlib import Path

import pandas as pd

from stage2_semantic_registry import get_calendar_coverage_definition

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_CALENDAR_PATH = PROJECT_ROOT / "data" / "bronze" / "calendars" / "release_calendar_master.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "silver" / "calendars" / "calendar_coverage_metadata.csv"

OUTPUT_COLUMNS = [
    "source_family",
    "source_subsource",
    "source_type",
    "observed_coverage_scope",
    "coverage_status",
    "intended_research_role",
    "is_historical_archive_complete",
    "is_current_snapshot_only",
    "has_official_release_time",
    "has_only_release_date",
    "is_proxy",
    "downstream_usage_constraint",
    "release_row_count_observed",
    "release_blocks_observed",
    "notes",
]


def build_calendar_coverage_metadata():
    bronze_calendar = pd.read_csv(BRONZE_CALENDAR_PATH, dtype="string", keep_default_na=False)
    grouped = bronze_calendar.groupby(["source_family", "source_subsource"], sort=True)

    rows = []
    for (source_family, source_subsource), subset in grouped:
        coverage = get_calendar_coverage_definition(source_family, source_subsource)
        observed_source_types = sorted(set(subset["source_type"]))
        observed_scopes = sorted(set(subset["coverage_scope"]))
        if observed_source_types != [coverage["source_type"]]:
            raise ValueError(
                f"Calendar coverage registry/source mismatch for {(source_family, source_subsource)}: "
                f"{observed_source_types} vs {coverage['source_type']}"
            )
        if observed_scopes != [coverage["observed_coverage_scope"]]:
            raise ValueError(
                f"Calendar coverage scope mismatch for {(source_family, source_subsource)}: "
                f"{observed_scopes} vs {coverage['observed_coverage_scope']}"
            )

        coverage["release_row_count_observed"] = int(len(subset))
        coverage["release_blocks_observed"] = ";".join(sorted(set(subset["release_block"])))
        rows.append(coverage)

    frame = pd.DataFrame(rows)[OUTPUT_COLUMNS]
    frame = frame.sort_values(["source_family", "source_subsource"], kind="stable").reset_index(drop=True)
    return frame


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame = build_calendar_coverage_metadata()
    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"[OK] Wrote {len(frame)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
