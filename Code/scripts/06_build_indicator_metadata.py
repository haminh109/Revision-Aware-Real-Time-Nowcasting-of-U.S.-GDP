import json
from pathlib import Path

import pandas as pd

from stage2_semantic_registry import get_indicator_definition

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGE0_MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage0_manifest.json"
BRONZE_INDICATORS_PATH = PROJECT_ROOT / "data" / "bronze" / "indicators" / "alfred_monthly_long.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "silver" / "indicators" / "indicator_metadata.csv"

USECOLS = [
    "source_family",
    "series_id",
    "series_frequency",
    "observation_date",
    "realtime_start",
    "realtime_end",
    "provenance_observations_file",
    "provenance_vintage_dates_file",
]

OUTPUT_COLUMNS = [
    "series_id",
    "canonical_indicator_id",
    "source_family",
    "series_frequency",
    "provenance_observations_file",
    "provenance_vintage_dates_file",
    "release_block_candidate",
    "release_block_mapping_status",
    "calendar_support_type",
    "indicator_family",
    "observed_observation_start",
    "observed_observation_end",
    "observed_realtime_start",
    "observed_realtime_end",
    "notes",
]


def load_stage0_manifest():
    with open(STAGE0_MANIFEST_PATH, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def update_iso_min(current_value: str, candidate: str) -> str:
    if not candidate:
        return current_value
    if not current_value:
        return candidate
    return candidate if candidate < current_value else current_value


def update_iso_max(current_value: str, candidate: str) -> str:
    if not candidate:
        return current_value
    if not current_value:
        return candidate
    return candidate if candidate > current_value else current_value


def update_single_value(entry: dict, field_name: str, value: str, series_id: str):
    if not value:
        return
    current_value = entry[field_name]
    if not current_value:
        entry[field_name] = value
        return
    if current_value != value:
        raise ValueError(
            f"Series {series_id} has inconsistent values for {field_name}: {current_value} vs {value}"
        )


def collect_series_stats():
    stats = {}
    chunks = pd.read_csv(
        BRONZE_INDICATORS_PATH,
        usecols=USECOLS,
        dtype="string",
        keep_default_na=False,
        chunksize=250000,
    )

    for chunk in chunks:
        for row in chunk.itertuples(index=False):
            source_family = normalize_text(row.source_family)
            series_id = normalize_text(row.series_id)
            series_frequency = normalize_text(row.series_frequency)
            observation_date = normalize_text(row.observation_date)
            realtime_start = normalize_text(row.realtime_start)
            realtime_end = normalize_text(row.realtime_end)
            provenance_observations_file = normalize_text(row.provenance_observations_file)
            provenance_vintage_dates_file = normalize_text(row.provenance_vintage_dates_file)

            entry = stats.setdefault(
                series_id,
                {
                    "series_id": series_id,
                    "source_family": "",
                    "series_frequency": "",
                    "provenance_observations_file": "",
                    "provenance_vintage_dates_file": "",
                    "observed_observation_start": "",
                    "observed_observation_end": "",
                    "observed_realtime_start": "",
                    "observed_realtime_end": "",
                },
            )

            update_single_value(entry, "source_family", source_family, series_id)
            update_single_value(entry, "series_frequency", series_frequency, series_id)
            update_single_value(entry, "provenance_observations_file", provenance_observations_file, series_id)
            update_single_value(entry, "provenance_vintage_dates_file", provenance_vintage_dates_file, series_id)

            entry["observed_observation_start"] = update_iso_min(
                entry["observed_observation_start"], observation_date
            )
            entry["observed_observation_end"] = update_iso_max(
                entry["observed_observation_end"], observation_date
            )
            entry["observed_realtime_start"] = update_iso_min(
                entry["observed_realtime_start"], realtime_start
            )
            entry["observed_realtime_end"] = update_iso_max(
                entry["observed_realtime_end"], realtime_end
            )

    return stats


def build_indicator_metadata():
    stats = collect_series_stats()
    manifest = load_stage0_manifest()
    required_series = sorted(manifest.get("required_alfred_series", []))
    missing_required = [series_id for series_id in required_series if series_id not in stats]
    if missing_required:
        raise ValueError(f"Missing required Stage 1 ALFRED series in bronze indicators file: {missing_required}")

    rows = []
    for series_id in sorted(stats):
        entry = dict(stats[series_id])
        semantics = get_indicator_definition(series_id)
        rows.append(
            {
                "series_id": series_id,
                "canonical_indicator_id": semantics["canonical_indicator_id"],
                "source_family": entry["source_family"],
                "series_frequency": entry["series_frequency"],
                "provenance_observations_file": entry["provenance_observations_file"],
                "provenance_vintage_dates_file": entry["provenance_vintage_dates_file"],
                "release_block_candidate": semantics["release_block"],
                "release_block_mapping_status": semantics["release_block_mapping_status"],
                "calendar_support_type": semantics["calendar_support_type"],
                "indicator_family": semantics["indicator_family"],
                "observed_observation_start": entry["observed_observation_start"],
                "observed_observation_end": entry["observed_observation_end"],
                "observed_realtime_start": entry["observed_realtime_start"],
                "observed_realtime_end": entry["observed_realtime_end"],
                "notes": semantics["notes"],
            }
        )

    frame = pd.DataFrame(rows)[OUTPUT_COLUMNS]
    frame = frame.sort_values(["series_id", "canonical_indicator_id"], kind="stable").reset_index(drop=True)
    return frame


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame = build_indicator_metadata()
    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"[OK] Wrote {len(frame)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
