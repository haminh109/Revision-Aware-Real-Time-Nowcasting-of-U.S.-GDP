import os
import json
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
if not FRED_API_KEY:
    raise ValueError("Missing FRED_API_KEY in .env")

BASE_FRED = "https://api.stlouisfed.org/fred"
BASE_DIR = Path("data/raw")

SERIES = [
    "GDPC1", "A191RL1Q225SBEA",
    "PAYEMS", "UNRATE", "INDPRO", "TCU", "AWHMAN",
    "W875RX1", "DSPIC96", "PCECC96", "RSAFS", "RSXFS", "UMCSENT",
    "HOUST", "PERMIT", "DGORDER", "NEWORDER",
    "BUSINV", "ISRATIO", "BOPGSTB", "BOPTEXP", "BOPTIMP",
    "FEDFUNDS", "TB3MS", "GS10", "T10Y3M", "SP500", "NAPM"
]

def ensure_dirs():
    dirs = [
        BASE_DIR / "alfred" / "vintage_dates",
        BASE_DIR / "alfred" / "series_observations",
        BASE_DIR / "calendars" / "bea",
        BASE_DIR / "calendars" / "bls",
        BASE_DIR / "calendars" / "census",
        BASE_DIR / "calendars" / "fed_g17",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

def get_json(endpoint, params):
    url = f"{BASE_FRED}/{endpoint}"
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def download_vintage_dates(series_id):
    data = get_json(
        "series/vintagedates",
        {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "realtime_start": "1776-07-04",
            "realtime_end": "9999-12-31",
        },
    )
    out_json = BASE_DIR / "alfred" / "vintage_dates" / f"{series_id}.json"
    out_csv = BASE_DIR / "alfred" / "vintage_dates" / f"{series_id}.csv"
    save_json(out_json, data)

    vintages = data.get("vintage_dates", [])
    df = pd.DataFrame({"vintage_date": vintages})
    df.to_csv(out_csv, index=False)

def download_all_observations_by_vintage(series_id):
    all_rows = []
    offset = 0
    limit = 100000

    while True:
        data = get_json(
            "series/observations",
            {
                "series_id": series_id,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "output_type": 2,
                "realtime_start": "1776-07-04",
                "realtime_end": "9999-12-31",
                "limit": limit,
                "offset": offset,
                "sort_order": "asc",
            },
        )

        rows = data.get("observations", [])
        if not rows:
            break

        all_rows.extend(rows)

        count = int(data.get("count", 0))
        offset += limit
        if offset >= count:
            break

        time.sleep(0.2)

    out_json = BASE_DIR / "alfred" / "series_observations" / f"{series_id}.json"
    out_csv = BASE_DIR / "alfred" / "series_observations" / f"{series_id}.csv"

    save_json(out_json, {"series_id": series_id, "observations": all_rows})

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.to_csv(out_csv, index=False)
    else:
        pd.DataFrame(columns=["realtime_start", "realtime_end", "date", "value"]).to_csv(out_csv, index=False)

def download_calendar_page(url, raw_html_path, parsed_prefix):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    html = r.text

    with open(raw_html_path, "w", encoding="utf-8") as f:
        f.write(html)

    try:
        tables = pd.read_html(StringIO(html))
        for i, tbl in enumerate(tables):
            tbl.to_csv(f"{parsed_prefix}_table_{i+1}.csv", index=False)
    except Exception:
        pass

def download_calendars():
    calendar_jobs = [
        (
            "https://www.bea.gov/news/schedule/full",
            BASE_DIR / "calendars" / "bea" / "full_release_schedule.html",
            BASE_DIR / "calendars" / "bea" / "full_release_schedule"
        ),
        (
            "https://www.bls.gov/schedule/news_release/empsit.htm",
            BASE_DIR / "calendars" / "bls" / "employment_situation.html",
            BASE_DIR / "calendars" / "bls" / "employment_situation"
        ),
        (
            "https://www.bls.gov/schedule/news_release/current_year.asp",
            BASE_DIR / "calendars" / "bls" / "current_year.html",
            BASE_DIR / "calendars" / "bls" / "current_year"
        ),
        (
            "https://www.census.gov/economic-indicators/calendar-listview.html",
            BASE_DIR / "calendars" / "census" / "economic_indicators_calendar.html",
            BASE_DIR / "calendars" / "census" / "economic_indicators_calendar"
        ),
        (
            "https://www.federalreserve.gov/releases/g17/release_dates.htm",
            BASE_DIR / "calendars" / "fed_g17" / "release_dates.html",
            BASE_DIR / "calendars" / "fed_g17" / "release_dates"
        ),
    ]

    for url, raw_html_path, parsed_prefix in calendar_jobs:
        print(f"Downloading calendar: {url}")
        download_calendar_page(url, raw_html_path, str(parsed_prefix))
        time.sleep(0.2)

def main():
    ensure_dirs()

    for i, series_id in enumerate(SERIES, start=1):
        print(f"[{i}/{len(SERIES)}] vintage dates -> {series_id}")
        download_vintage_dates(series_id)
        time.sleep(0.2)

        print(f"[{i}/{len(SERIES)}] observations -> {series_id}")
        download_all_observations_by_vintage(series_id)
        time.sleep(0.2)

    download_calendars()
    print("Done.")

if __name__ == "__main__":
    main()