import re
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BEA_HTML_PATH = PROJECT_ROOT / "data" / "raw" / "calendars" / "bea" / "full_release_schedule.html"
BLS_CURRENT_YEAR_PATH = PROJECT_ROOT / "data" / "raw" / "calendars" / "bls" / "current_year.html"
BLS_EMPLOYMENT_PATH = PROJECT_ROOT / "data" / "raw" / "calendars" / "bls" / "employment_situation.html"
FED_G17_PATH = PROJECT_ROOT / "data" / "raw" / "calendars" / "fed_g17" / "release_dates.html"
CENSUS_PROXY_PATH = PROJECT_ROOT / "data" / "raw" / "calendars" / "census" / "census_proxy_release_calendar.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "calendars" / "release_calendar_master.csv"

BLOCK_KEYWORDS = [
    ("gross domestic product", "gdp"),
    ("personal income and outlays", "personal_income_outlays"),
    ("employment situation", "employment_situation"),
    ("industrial production and capacity utilization", "industrial_production_capacity_utilization"),
    ("industrial production", "industrial_production_capacity_utilization"),
    ("capacity utilization", "industrial_production_capacity_utilization"),
    ("job openings and labor turnover survey", "jolts"),
    ("state employment and unemployment", "state_employment_unemployment"),
    ("consumer price index", "consumer_price_index"),
    ("producer price index", "producer_price_index"),
    ("retail trade", "retail_trade"),
    ("business employment dynamics", "business_employment_dynamics"),
    ("productivity and costs", "productivity_and_costs"),
    ("international trade", "international_trade"),
    ("u.s. import and export price indexes", "import_export_prices"),
    ("import and export price indexes", "import_export_prices"),
    ("housing vacancies", "housing"),
]


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def collapse_whitespace(value) -> str:
    return re.sub(r"\s+", " ", normalize_text(value))


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return cleaned or "unknown"


def normalize_time_to_hhmm(value) -> str:
    text = collapse_whitespace(value)
    if not text:
        return ""
    return pd.to_datetime(text, errors="raise").strftime("%H:%M")


def normalize_date_to_iso(value) -> str:
    text = collapse_whitespace(value)
    if not text:
        raise ValueError("Missing date value.")
    return pd.to_datetime(text, errors="raise").strftime("%Y-%m-%d")


def humanize_block(block: str) -> str:
    return block.replace("_", " ").title()


def normalize_release_block(release_name: str, fallback_prefix: str = "event") -> str:
    normalized = release_name.lower()
    normalized = re.sub(r"\s+\([^)]*\)", "", normalized)
    normalized = re.sub(r"\s+for\s+.+$", "", normalized)
    for keyword, release_block in BLOCK_KEYWORDS:
        if keyword in normalized:
            return release_block
    return f"{fallback_prefix}_{slugify(normalized)[:60]}"


def extract_reference_period_from_title(release_name: str) -> str:
    normalized = collapse_whitespace(release_name)
    if " for " in normalized:
        return normalized.rsplit(" for ", 1)[1].strip()
    return ""


def finalize_frame(frame: pd.DataFrame):
    frame = frame.copy()
    frame["reference_period_label"] = frame["reference_period_label"].fillna("").map(collapse_whitespace)
    frame["release_time_et"] = frame["release_time_et"].fillna("").map(collapse_whitespace)
    frame["included_series"] = frame["included_series"].fillna("").map(collapse_whitespace)
    frame["proxy_method"] = frame["proxy_method"].fillna("").map(collapse_whitespace)
    frame["notes"] = frame["notes"].fillna("").map(collapse_whitespace)

    event_reference = frame["reference_period_label"].where(
        frame["reference_period_label"] != "",
        frame["release_name"],
    )

    event_id_components = (
        frame["source_family"].str.lower()
        + "__"
        + frame["release_date"]
        + "__"
        + frame["release_block"]
        + "__"
        + frame["release_time_et"].replace({"": "notime"})
        + "__"
        + event_reference.map(slugify)
    )
    frame["event_id"] = event_id_components.map(slugify)

    frame = frame.sort_values(
        ["release_date", "release_time_et", "source_family", "release_block", "release_name", "reference_period_label"],
        kind="stable",
    ).reset_index(drop=True)

    return frame[
        [
            "event_id",
            "source_family",
            "source_type",
            "source_subsource",
            "coverage_scope",
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


def build_bea_events():
    tables = pd.read_html(BEA_HTML_PATH)
    if not tables:
        raise ValueError("Could not parse any BEA release-schedule tables.")

    table = tables[0].copy()
    columns = [collapse_whitespace(col) for col in table.columns]
    table.columns = columns

    year_match = re.search(r"(19|20)\d{2}", columns[0]) if columns else None
    if year_match is None:
        raise ValueError("Could not infer the BEA schedule year from the parsed table header.")
    schedule_year = int(year_match.group(0))

    rows = []
    for _, row in table.iterrows():
        date_time_raw = collapse_whitespace(row[columns[0]])
        release_name = collapse_whitespace(row.get("Release", ""))
        if not date_time_raw or not release_name:
            continue

        if "to be announced" in date_time_raw.lower():
            continue

        match = re.match(r"^(?P<month_day>[A-Za-z]+ \d{1,2}) (?P<time>\d{1,2}:\d{2} [AP]M)$", date_time_raw)
        if match is None:
            raise ValueError(f"Could not parse BEA date/time cell: {date_time_raw}")

        release_date = normalize_date_to_iso(f"{match.group('month_day')} {schedule_year}")
        release_time_et = normalize_time_to_hhmm(match.group("time"))
        reference_period_label = extract_reference_period_from_title(release_name)

        rows.append(
            {
                "source_family": "BEA",
                "source_type": "official",
                "source_subsource": "full_release_schedule",
                "coverage_scope": "current_year_snapshot",
                "release_block": normalize_release_block(release_name, fallback_prefix="bea"),
                "release_name": release_name,
                "reference_period_label": reference_period_label,
                "release_date": release_date,
                "release_time_et": release_time_et,
                "release_time_status": "official_published",
                "included_series": "",
                "proxy_method": "",
                "provenance_file": BEA_HTML_PATH.relative_to(PROJECT_ROOT).as_posix(),
                "notes": "Official BEA current-year release schedule snapshot.",
            }
        )

    return pd.DataFrame(rows)


def build_bls_current_year_events():
    tables = pd.read_html(BLS_CURRENT_YEAR_PATH)
    rows = []
    for table in tables:
        columns = [collapse_whitespace(col) for col in table.columns]
        if not {"Date", "Time", "Release"}.issubset(columns):
            continue

        frame = table.copy()
        frame.columns = columns
        frame = frame[["Date", "Time", "Release"]]
        frame = frame[
            ~(frame["Date"].map(collapse_whitespace) == "Date")
            & ~(frame["Release"].map(collapse_whitespace) == "Release")
        ]
        frame = frame[frame["Date"].map(collapse_whitespace) != ""]
        frame = frame[frame["Time"].map(collapse_whitespace) != ""]

        for _, row in frame.iterrows():
            release_name = collapse_whitespace(row["Release"])
            if not release_name:
                continue

            rows.append(
                {
                    "source_family": "BLS",
                    "source_type": "official",
                    "source_subsource": "current_year",
                    "coverage_scope": "current_year_snapshot",
                    "release_block": normalize_release_block(release_name, fallback_prefix="bls"),
                    "release_name": release_name,
                    "reference_period_label": extract_reference_period_from_title(release_name),
                    "release_date": normalize_date_to_iso(row["Date"]),
                    "release_time_et": normalize_time_to_hhmm(row["Time"]),
                    "release_time_status": "official_published",
                    "included_series": "",
                    "proxy_method": "",
                    "provenance_file": BLS_CURRENT_YEAR_PATH.relative_to(PROJECT_ROOT).as_posix(),
                    "notes": "Official BLS current-year release schedule snapshot.",
                }
            )

    if not rows:
        raise ValueError("Could not parse any usable rows from the BLS current-year release calendar.")

    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["release_date", "release_time_et", "release_name"], kind="stable")
    frame = frame.drop_duplicates(subset=["release_date", "release_time_et", "release_name"], keep="first")
    return frame.reset_index(drop=True)


def build_bls_employment_events():
    tables = pd.read_html(BLS_EMPLOYMENT_PATH)
    selected = None
    for table in tables:
        columns = [collapse_whitespace(col) for col in table.columns]
        if {"Reference Month", "Release Date", "Release Time"}.issubset(columns):
            selected = table.copy()
            selected.columns = columns
            break

    if selected is None:
        raise ValueError("Could not parse the BLS Employment Situation schedule table.")

    rows = []
    for _, row in selected.iterrows():
        reference_period = collapse_whitespace(row["Reference Month"])
        release_date_raw = collapse_whitespace(row["Release Date"])
        release_time_raw = collapse_whitespace(row["Release Time"])
        if not reference_period or not release_date_raw or not release_time_raw:
            continue

        release_name = f"Employment Situation for {reference_period}"
        rows.append(
            {
                "source_family": "BLS",
                "source_type": "official",
                "source_subsource": "employment_situation",
                "coverage_scope": "scheduled_release_specific_page",
                "release_block": "employment_situation",
                "release_name": release_name,
                "reference_period_label": reference_period,
                "release_date": normalize_date_to_iso(release_date_raw),
                "release_time_et": normalize_time_to_hhmm(release_time_raw),
                "release_time_status": "official_published",
                "included_series": "",
                "proxy_method": "",
                "provenance_file": BLS_EMPLOYMENT_PATH.relative_to(PROJECT_ROOT).as_posix(),
                "notes": "Official BLS Employment Situation schedule page.",
            }
        )

    return pd.DataFrame(rows)


def combine_bls_events():
    employment = build_bls_employment_events()
    current_year = build_bls_current_year_events()
    employment["priority"] = 0
    current_year["priority"] = 1

    combined = pd.concat([employment, current_year], ignore_index=True)
    combined = combined.sort_values(
        ["release_date", "release_time_et", "release_name", "priority"],
        kind="stable",
    )
    combined = combined.drop_duplicates(
        subset=["release_date", "release_time_et", "release_name"],
        keep="first",
    ).reset_index(drop=True)
    return combined.drop(columns=["priority"])


def build_fed_g17_events():
    html = FED_G17_PATH.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    pre_block = soup.find("pre")
    if pre_block is None:
        raise ValueError("Could not find the expected preformatted G.17 release-date block.")

    rows = []
    pattern = re.compile(r"^(?P<reference>[A-Za-z]+ \d{4})\s+(?P<release_dates>.+)$")
    for raw_line in pre_block.get_text("\n").splitlines():
        line = collapse_whitespace(raw_line)
        if not line:
            continue

        match = pattern.match(line)
        if match is None:
            continue

        reference_period = match.group("reference")
        release_dates_text = match.group("release_dates")
        if release_dates_text == "NA":
            continue

        date_fragments = [fragment.strip() for fragment in release_dates_text.split(" and ") if fragment.strip()]
        note = (
            "Official Federal Reserve release page row contained multiple release dates and was split into separate events."
            if len(date_fragments) > 1
            else "Official Federal Reserve historical/scheduled G.17 release dates page."
        )

        for date_fragment in date_fragments:
            rows.append(
                {
                    "source_family": "FED_G17",
                    "source_type": "official",
                    "source_subsource": "release_dates",
                    "coverage_scope": "historical_and_scheduled",
                    "release_block": "industrial_production_capacity_utilization",
                    "release_name": "Industrial Production and Capacity Utilization - G.17",
                    "reference_period_label": reference_period,
                    "release_date": normalize_date_to_iso(date_fragment),
                    "release_time_et": "",
                    "release_time_status": "official_not_published",
                    "included_series": "",
                    "proxy_method": "",
                    "provenance_file": FED_G17_PATH.relative_to(PROJECT_ROOT).as_posix(),
                    "notes": note,
                }
            )

    if not rows:
        raise ValueError("Could not parse any Federal Reserve G.17 release-date rows.")
    return pd.DataFrame(rows)


def build_census_proxy_events():
    frame = pd.read_csv(CENSUS_PROXY_PATH, dtype="string").fillna("")
    if frame.empty:
        raise ValueError("Census proxy calendar CSV exists but contains no rows.")

    rows = []
    for _, row in frame.iterrows():
        release_block = collapse_whitespace(row["release_block"])
        release_date = normalize_date_to_iso(row["release_date"])
        included_series = collapse_whitespace(row.get("included_series", ""))
        proxy_method = collapse_whitespace(row.get("proxy_method", ""))
        notes = collapse_whitespace(row.get("notes", ""))

        rows.append(
            {
                "source_family": "CENSUS_PROXY",
                "source_type": "proxy",
                "source_subsource": "alfred_proxy_calendar",
                "coverage_scope": "proxy_from_available_vintages",
                "release_block": release_block,
                "release_name": f"Census proxy availability: {humanize_block(release_block)}",
                "reference_period_label": "",
                "release_date": release_date,
                "release_time_et": "",
                "release_time_status": "proxy_not_official",
                "included_series": included_series,
                "proxy_method": proxy_method,
                "provenance_file": CENSUS_PROXY_PATH.relative_to(PROJECT_ROOT).as_posix(),
                "notes": notes,
            }
        )

    return pd.DataFrame(rows)


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    combined = pd.concat(
        [
            build_bea_events(),
            combine_bls_events(),
            build_fed_g17_events(),
            build_census_proxy_events(),
        ],
        ignore_index=True,
    )
    output = finalize_frame(combined)
    output.to_csv(OUTPUT_PATH, index=False)
    print(f"[OK] Wrote {len(output)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
