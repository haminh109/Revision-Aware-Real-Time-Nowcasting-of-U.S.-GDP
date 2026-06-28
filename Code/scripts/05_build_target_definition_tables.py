from pathlib import Path

import pandas as pd

from stage2_semantic_registry import get_target_registry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_RELEASE_STAGE_PATH = PROJECT_ROOT / "data" / "bronze" / "targets" / "gdp_release_targets.csv"
BRONZE_COMPLETE_VINTAGES_PATH = PROJECT_ROOT / "data" / "bronze" / "targets" / "gdp_complete_vintages_long.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "silver" / "targets" / "target_definition_table.csv"

OUTPUT_COLUMNS = [
    "canonical_target_id",
    "target_family",
    "target_object_type",
    "source_family",
    "source_dataset",
    "target_variable_id",
    "measurement_semantics",
    "unit_semantics",
    "release_structure_type",
    "comparability_group",
    "is_real_time_release_target",
    "is_revision_history_target",
    "source_artifact",
    "notes",
]


def ensure_expected_bronze_sources():
    release_stage = pd.read_csv(BRONZE_RELEASE_STAGE_PATH, usecols=["source_family", "source_dataset", "target_variable_id"])
    complete_vintages = pd.read_csv(
        BRONZE_COMPLETE_VINTAGES_PATH,
        usecols=["source_family", "source_dataset", "target_variable_id"],
    )

    expected_pairs = {
        ("RTDSM", "routput_first_second_third", "ROUTPUT"),
        ("RTDSM", "ROUTPUTQvQd", "ROUTPUT"),
    }
    observed_pairs = set()
    observed_pairs.update(map(tuple, release_stage.drop_duplicates().itertuples(index=False, name=None)))
    observed_pairs.update(map(tuple, complete_vintages.drop_duplicates().itertuples(index=False, name=None)))

    missing_pairs = sorted(expected_pairs.difference(observed_pairs))
    if missing_pairs:
        raise ValueError(f"Missing expected bronze target source pairs: {missing_pairs}")


def build_target_definition_table():
    ensure_expected_bronze_sources()
    registry = get_target_registry()

    rows = [registry["routput_first_second_third"], registry["ROUTPUTQvQd"]]
    frame = pd.DataFrame(rows)[OUTPUT_COLUMNS]
    frame = frame.sort_values(
        ["target_family", "target_object_type", "canonical_target_id"],
        kind="stable",
    ).reset_index(drop=True)
    return frame


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame = build_target_definition_table()
    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"[OK] Wrote {len(frame)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
