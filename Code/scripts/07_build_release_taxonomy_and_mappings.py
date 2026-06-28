from pathlib import Path

import pandas as pd

from stage2_semantic_registry import CURRENTLY_UNMAPPED_BLOCK, get_indicator_definition

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INDICATOR_METADATA_PATH = PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_metadata.csv"
BRONZE_CALENDAR_PATH = PROJECT_ROOT / "data" / "bronze" / "calendars" / "release_calendar_master.csv"
INDICATOR_RELEASE_MAP_OUTPUT = PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_release_map.csv"
RELEASE_BLOCK_TAXONOMY_OUTPUT = PROJECT_ROOT / "data" / "silver" / "calendars" / "release_block_taxonomy.csv"


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def humanize_block(block: str) -> str:
    return block.replace("_", " ").title()


def build_indicator_release_map():
    indicator_metadata = pd.read_csv(INDICATOR_METADATA_PATH, dtype="string", keep_default_na=False)

    rows = []
    for row in indicator_metadata.itertuples(index=False):
        semantics = get_indicator_definition(row.series_id)
        rows.append(
            {
                "series_id": row.series_id,
                "canonical_indicator_id": row.canonical_indicator_id,
                "release_block": semantics["release_block"],
                "source_family_for_timing": semantics["source_family_for_timing"],
                "mapping_confidence": semantics["mapping_confidence"],
                "mapping_basis": semantics["mapping_basis"],
                "source_type": semantics["source_type"],
                "notes": semantics["notes"],
            }
        )

    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["series_id", "release_block"], kind="stable").reset_index(drop=True)
    return frame


def build_release_block_taxonomy(indicator_release_map: pd.DataFrame):
    bronze_calendar = pd.read_csv(BRONZE_CALENDAR_PATH, dtype="string", keep_default_na=False)
    calendar_grouped = bronze_calendar.groupby("release_block", sort=True)

    rows = []
    mapped_series_by_block = (
        indicator_release_map.groupby("release_block", sort=True)["series_id"]
        .agg(lambda values: sorted(set(values)))
        .to_dict()
    )

    observed_blocks = sorted(set(bronze_calendar["release_block"]))
    taxonomy_blocks = sorted(set(observed_blocks).union(mapped_series_by_block))

    for release_block in taxonomy_blocks:
        observed = release_block in observed_blocks
        if observed:
            subset = calendar_grouped.get_group(release_block)
            observed_source_families = ";".join(sorted(set(subset["source_family"])))
            observed_source_types = ";".join(sorted(set(subset["source_type"])))
            observed_source_subsources = ";".join(sorted(set(subset["source_subsource"])))
            if set(subset["source_type"]) == {"proxy"}:
                timing_support_class = "proxy"
            elif set(subset["source_type"]) == {"official"}:
                timing_support_class = "official"
            else:
                timing_support_class = "mixed"
            notes = "Observed directly in the Stage 1 bronze release calendar master."
        else:
            observed_source_families = ""
            observed_source_types = ""
            observed_source_subsources = ""
            timing_support_class = "unmapped"
            notes = "Placeholder taxonomy block for indicators that do not yet have a defensible release-calendar mapping in the current repo."

        indicator_series = mapped_series_by_block.get(release_block, [])
        if release_block == CURRENTLY_UNMAPPED_BLOCK:
            release_block_label = "Currently Unmapped"
            release_block_taxonomy_status = "indicator_placeholder_only"
        else:
            release_block_label = humanize_block(release_block)
            release_block_taxonomy_status = "observed_in_bronze_calendar" if observed else "curated_placeholder"

        rows.append(
            {
                "release_block": release_block,
                "release_block_label": release_block_label,
                "observed_in_bronze_calendar": observed,
                "observed_source_families": observed_source_families,
                "observed_source_types": observed_source_types,
                "observed_source_subsources": observed_source_subsources,
                "timing_support_class": timing_support_class,
                "release_block_taxonomy_status": release_block_taxonomy_status,
                "indicator_series_count": len(indicator_series),
                "indicator_series_ids": ";".join(indicator_series),
                "notes": notes,
            }
        )

    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["release_block"], kind="stable").reset_index(drop=True)
    return frame


def validate_mapped_blocks(indicator_release_map: pd.DataFrame, release_block_taxonomy: pd.DataFrame):
    observed_blocks = set(
        release_block_taxonomy.loc[
            release_block_taxonomy["observed_in_bronze_calendar"],
            "release_block",
        ]
    )
    missing_blocks = sorted(
        set(
            indicator_release_map.loc[
                indicator_release_map["source_type"].isin(["official", "proxy"]),
                "release_block",
            ]
        ).difference(observed_blocks)
    )
    if missing_blocks:
        raise ValueError(
            f"Indicator release mappings refer to release blocks not observed in the bronze calendar: {missing_blocks}"
        )


def main():
    INDICATOR_RELEASE_MAP_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    RELEASE_BLOCK_TAXONOMY_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    indicator_release_map = build_indicator_release_map()
    release_block_taxonomy = build_release_block_taxonomy(indicator_release_map)
    validate_mapped_blocks(indicator_release_map, release_block_taxonomy)

    indicator_release_map.to_csv(INDICATOR_RELEASE_MAP_OUTPUT, index=False)
    release_block_taxonomy.to_csv(RELEASE_BLOCK_TAXONOMY_OUTPUT, index=False)
    print(f"[OK] Wrote {len(indicator_release_map)} rows -> {INDICATOR_RELEASE_MAP_OUTPUT}")
    print(f"[OK] Wrote {len(release_block_taxonomy)} rows -> {RELEASE_BLOCK_TAXONOMY_OUTPUT}")


if __name__ == "__main__":
    main()
