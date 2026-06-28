from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_FOR_IMPORT))

from full_state_space_release_revision_dfm.q2_benchmarks import (
    PHILLY_FED_SPF_RGDP_MEAN_GROWTH_URL,
    build_public_spf_rgdp_growth_csv,
)


DEFAULT_OUTPUT = Path("data/external/spf/spf_rgdp_growth_benchmark.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a public Philadelphia Fed SPF RGDP benchmark CSV.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--raw-cache", type=Path, default=Path("data/external/spf/Mean_RGDP_Growth.xlsx"))
    parser.add_argument("--source-url", default=PHILLY_FED_SPF_RGDP_MEAN_GROWTH_URL)
    parser.add_argument("--statistic", default="mean", choices=["mean", "median"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frame = build_public_spf_rgdp_growth_csv(
        args.output,
        source_url=args.source_url,
        raw_cache_path=args.raw_cache,
        statistic=args.statistic,
    )
    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "output": str(args.output),
        "raw_cache": str(args.raw_cache),
        "source_url": args.source_url,
        "statistic": args.statistic,
        "rows": int(len(frame)),
        "availability_rule": "survey_quarter_second_month_end_conservative_proxy",
        "schema": ["forecast_origin_date", "target_quarter", "target_id", "forecast_value"],
    }
    manifest_path = args.output.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote SPF benchmark: {args.output.resolve()}")
    print(f"Rows: {len(frame)}")
    print(f"Manifest: {manifest_path.resolve()}")


if __name__ == "__main__":
    main()
