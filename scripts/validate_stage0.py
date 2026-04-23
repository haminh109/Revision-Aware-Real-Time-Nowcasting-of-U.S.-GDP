import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "configs" / "stage0_manifest.json"
REPORT_PATH = PROJECT_ROOT / "data" / "metadata" / "stage0_validation_report.json"


def load_manifest():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def check_paths(paths):
    missing = []
    present = []
    for rel in paths:
        path = PROJECT_ROOT / rel
        if path.exists():
            present.append(rel)
        else:
            missing.append(rel)
    return present, missing


def check_alfred_series(series_list):
    missing = []
    present = []
    for series_id in series_list:
        expected = [
            PROJECT_ROOT / "data" / "raw" / "alfred" / "vintage_dates" / f"{series_id}.csv",
            PROJECT_ROOT / "data" / "raw" / "alfred" / "vintage_dates" / f"{series_id}.json",
            PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations" / f"{series_id}.csv",
            PROJECT_ROOT / "data" / "raw" / "alfred" / "series_observations" / f"{series_id}.json",
        ]
        if all(p.exists() for p in expected):
            present.append(series_id)
        else:
            missing.append(series_id)
    return present, missing


def main():
    manifest = load_manifest()
    (PROJECT_ROOT / "data" / "metadata").mkdir(parents=True, exist_ok=True)

    results = {}

    for key in [
        "required_root_files",
        "required_directories",
        "required_rtdsm_files",
        "optional_rtdsm_files",
        "required_bea_files",
        "required_calendar_files",
        "optional_calendar_files",
        "recommended_calendar_files",
    ]:
        present, missing = check_paths(manifest.get(key, []))
        results[key] = {
            "present": present,
            "missing": missing,
        }

    alfred_present, alfred_missing = check_alfred_series(manifest.get("required_alfred_series", []))
    results["required_alfred_series"] = {
        "present": alfred_present,
        "missing": alfred_missing,
    }
    optional_alfred_present, optional_alfred_missing = check_alfred_series(
        manifest.get("optional_alfred_series", [])
    )
    results["optional_alfred_series"] = {
        "present": optional_alfred_present,
        "missing": optional_alfred_missing,
    }

    fail_groups = [
        "required_root_files",
        "required_directories",
        "required_rtdsm_files",
        "required_bea_files",
        "required_calendar_files",
        "required_alfred_series",
    ]

    status = "PASS"
    if any(results[group]["missing"] for group in fail_groups):
        status = "FAIL"

    report = {
        "stage": "stage_0",
        "status": status,
        "project_root": str(PROJECT_ROOT),
        "results": results,
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Stage 0 validation status: {status}")

    for group in fail_groups:
        missing = results[group]["missing"]
        if missing:
            print(f"\n[{group}] missing ({len(missing)}):")
            for item in missing:
                print(f"  - {item}")

    informational_groups = [
        "optional_rtdsm_files",
        "optional_calendar_files",
        "recommended_calendar_files",
        "optional_alfred_series",
    ]
    for group in informational_groups:
        missing = results.get(group, {}).get("missing", [])
        if missing:
            print(f"\n[{group}] missing ({len(missing)}) [informational]:")
            for item in missing:
                print(f"  - {item}")

    print(f"\nValidation report written to: {REPORT_PATH}")

    if status != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
