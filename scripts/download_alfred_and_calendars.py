import json
import os
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

FRED_API_KEY = os.getenv("FRED_API_KEY")
if not FRED_API_KEY:
    raise ValueError(f"Missing FRED_API_KEY in {PROJECT_ROOT / '.env'}")

BASE_FRED = "https://api.stlouisfed.org/fred"
BASE_DIR = PROJECT_ROOT / "data" / "raw"
CENSUS_CALENDAR_URL = "https://www.census.gov/economic-indicators/calendar-listview.html"


def resolve_path_from_env(env_name, default_path):
    value = os.getenv(env_name, "").strip()
    if not value:
        return default_path

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate


CENSUS_CALENDAR_MANUAL_HTML = resolve_path_from_env(
    "CENSUS_CALENDAR_MANUAL_HTML",
    BASE_DIR / "calendars" / "census" / "economic_indicators_calendar.manual.html",
)

CALENDAR_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

SERIES = [
    "GDPC1", "A191RL1Q225SBEA",
    "PAYEMS", "UNRATE", "INDPRO", "TCU", "AWHMAN",
    "W875RX1", "DSPIC96", "PCECC96", "RSAFS", "RSXFS", "UMCSENT",
    "HOUST", "PERMIT", "DGORDER", "NEWORDER",
    "BUSINV", "ISRATIO", "BOPGSTB", "BOPTEXP", "BOPTIMP",
    "FEDFUNDS", "TB3MS", "GS10", "T10Y3MM", "SP500", "NAPM"
]

CALENDAR_JOBS = [
    {
        "name": "bea",
        "url": "https://www.bea.gov/news/schedule/full",
        "raw_html_path": BASE_DIR / "calendars" / "bea" / "full_release_schedule.html",
        "parsed_prefix": BASE_DIR / "calendars" / "bea" / "full_release_schedule",
    },
    {
        "name": "bls_empsit",
        "url": "https://www.bls.gov/schedule/news_release/empsit.htm",
        "raw_html_path": BASE_DIR / "calendars" / "bls" / "employment_situation.html",
        "parsed_prefix": BASE_DIR / "calendars" / "bls" / "employment_situation",
    },
    {
        "name": "bls_current_year",
        "url": "https://www.bls.gov/schedule/news_release/current_year.asp",
        "raw_html_path": BASE_DIR / "calendars" / "bls" / "current_year.html",
        "parsed_prefix": BASE_DIR / "calendars" / "bls" / "current_year",
    },
    {
        "name": "census",
        "url": CENSUS_CALENDAR_URL,
        "raw_html_path": BASE_DIR / "calendars" / "census" / "economic_indicators_calendar.html",
        "parsed_prefix": BASE_DIR / "calendars" / "census" / "economic_indicators_calendar",
        "manual_html_path": CENSUS_CALENDAR_MANUAL_HTML,
        "blocked_html_path": BASE_DIR / "calendars" / "census" / "economic_indicators_calendar.blocked.html",
    },
    {
        "name": "fed_g17",
        "url": "https://www.federalreserve.gov/releases/g17/release_dates.htm",
        "raw_html_path": BASE_DIR / "calendars" / "fed_g17" / "release_dates.html",
        "parsed_prefix": BASE_DIR / "calendars" / "fed_g17" / "release_dates",
    },
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


def get_json(endpoint, params, max_retries=3, sleep_seconds=1.0):
    url = f"{BASE_FRED}/{endpoint}"
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(sleep_seconds * attempt)
            else:
                raise last_error


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def save_text(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


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
    pd.DataFrame({"vintage_date": data.get("vintage_dates", [])}).to_csv(out_csv, index=False)


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

    if all_rows:
        pd.DataFrame(all_rows).to_csv(out_csv, index=False)
    else:
        pd.DataFrame(columns=["realtime_start", "realtime_end", "date", "value"]).to_csv(out_csv, index=False)


def build_calendar_session():
    session = requests.Session()
    session.headers.update(CALENDAR_HEADERS)
    return session


def looks_like_access_block(html, status_code):
    markers = [
        "attention required! | cloudflare",
        "sorry, you have been blocked",
        "you are unable to access",
        "cf-error-details",
        "cloudflare ray id",
        "please enable cookies",
    ]
    lowered = html.lower()
    return status_code in {403, 429} or any(marker in lowered for marker in markers)


def parse_calendar_tables(html, parsed_prefix):
    try:
        tables = pd.read_html(StringIO(html))
        for i, table in enumerate(tables, start=1):
            table.to_csv(f"{parsed_prefix}_table_{i}.csv", index=False)
        return len(tables)
    except Exception:
        return 0


def load_manual_calendar_html(path):
    if path is None or not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def fetch_calendar_html(session, job):
    response = session.get(job["url"], timeout=120)
    html = response.text

    if looks_like_access_block(html, response.status_code):
        blocked_html_path = job.get("blocked_html_path")
        if blocked_html_path is not None:
            save_text(blocked_html_path, html)
            print(
                f"[WARN] Saved blocked {job['name']} response -> "
                f"{blocked_html_path}"
            )

        manual_html_path = job.get("manual_html_path")
        manual_html = load_manual_calendar_html(manual_html_path)
        if manual_html is not None:
            print(
                f"[WARN] {job['name']} calendar blocked by source; "
                f"using manual fallback from {manual_html_path}"
            )
            return manual_html, f"manual:{manual_html_path}"

        if job["name"] == "census":
            print(
                "[WARN] Census calendar request was blocked by the source. "
                "Skipping the official Census HTML download and continuing. "
                "Use the ALFRED-based Census proxy calendar for release-day alignment."
            )
            return None, "blocked"

        raise RuntimeError(
            f"Calendar download blocked for {job['url']} "
            f"(status {response.status_code})."
        )

    response.raise_for_status()
    return html, "remote"


def download_calendar_page(session, job):
    html, source = fetch_calendar_html(session, job)
    if html is None:
        return False

    save_text(job["raw_html_path"], html)
    table_count = parse_calendar_tables(html, job["parsed_prefix"])
    print(
        f"[OK] Saved {job['name']} calendar from {source} "
        f"-> {job['raw_html_path']}"
    )
    if table_count:
        print(
            f"[OK] Parsed {table_count} table(s) -> "
            f"{job['parsed_prefix']}_table_*.csv"
        )
    return True


def download_calendars():
    session = build_calendar_session()
    failures = []

    for job in CALENDAR_JOBS:
        print(f"Downloading calendar: {job['url']}")
        job["raw_html_path"].parent.mkdir(parents=True, exist_ok=True)
        try:
            downloaded = download_calendar_page(session, job)
            if not downloaded:
                failures.append(
                    (job["name"], "calendar blocked by source; official HTML skipped")
                )
        except Exception as exc:
            failures.append((job["name"], str(exc)))
            print(f"[WARN] Failed calendar {job['name']}: {exc}")
        time.sleep(0.2)

    return failures


def main():
    ensure_dirs()

    for i, series_id in enumerate(SERIES, start=1):
        try:
            print(f"[{i}/{len(SERIES)}] vintage dates -> {series_id}")
            download_vintage_dates(series_id)
            time.sleep(0.2)

            print(f"[{i}/{len(SERIES)}] observations -> {series_id}")
            download_all_observations_by_vintage(series_id)
            time.sleep(0.2)
        except Exception as e:
            print(f"[WARN] Failed series {series_id}: {e}")
            continue

    calendar_failures = download_calendars()
    if calendar_failures:
        print("\nCalendar download warnings:")
        for name, message in calendar_failures:
            print(f"  - {name}: {message}")
    print("Done.")


if __name__ == "__main__":
    main()
