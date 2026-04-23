# Revision-Aware Real-Time Nowcasting of U.S. GDP

This repository supports the project **"Release-Structured Revision-Aware Real-Time Nowcasting of U.S. GDP"**.

The goal is to build a research-grade, reproducible data and modeling pipeline for real-time GDP nowcasting using:

- **RTDSM / Philadelphia Fed** for release-specific GDP targets and complete vintage histories
- **ALFRED / FRED** for vintage monthly indicators
- **Official BEA / BLS / Federal Reserve release calendars**, plus an **ALFRED-based Census proxy availability calendar**
- **BEA NIPA API tables** for sanity checks and robustness checks

---

## Stage definitions

### Stage 0 — Raw-data foundation and reproducibility

Stage 0 is complete only when the repository has:

1. A documented project structure
2. Environment and dependency files
3. Source-aware download scripts
4. Raw data saved in a stable folder structure
5. A validation script that checks Stage 0 completeness

### Stage 1 — Bronze normalization

Stage 1 begins only after Stage 0 is locked. It will create normalized artifacts such as:

- `data/bronze/targets/gdp_release_targets.csv`
- `data/bronze/targets/gdp_complete_vintages_long.csv`
- `data/bronze/indicators/alfred_monthly_long.csv`
- `data/bronze/calendars/release_calendar_master.csv`

---

## Repository structure

```text
.
├── configs/
│   └── stage0_manifest.json
├── data/
│   ├── raw/
│   │   ├── alfred/
│   │   │   ├── series_observations/
│   │   │   └── vintage_dates/
│   │   ├── bea/
│   │   │   └── api/
│   │   ├── calendars/
│   │   │   ├── bea/
│   │   │   ├── bls/
│   │   │   ├── census/
│   │   │   └── fed_g17/
│   │   └── rtdsm/
│   │       └── routput/
│   └── metadata/
│       └── stage0_validation_report.json
├── docs/
│   └── STAGE_0_CHECKLIST.md
├── scripts/
│   ├── build_census_proxy_calendar.py
│   ├── download_alfred_and_calendars.py
│   ├── download_bea.py
│   └── validate_stage0.py
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Data sources

### 1. RTDSM / Philadelphia Fed
Used as the **primary source of release-structured GDP targets**.

Required files:

- `data/raw/rtdsm/routput/ROUTPUTQvQd.xlsx`
- `data/raw/rtdsm/routput/routput_first_second_third.xlsx`

Optional but recommended:

- `data/raw/rtdsm/routput/routputMvQd.xlsx`

### 2. ALFRED / FRED
Used for **real-time monthly indicator vintages**.

Required Stage 0 indicator set:

- `GDPC1`, `A191RL1Q225SBEA`
- `PAYEMS`, `UNRATE`, `INDPRO`, `TCU`, `AWHMAN`
- `W875RX1`, `DSPIC96`, `PCECC96`, `RSAFS`, `RSXFS`, `UMCSENT`
- `HOUST`, `PERMIT`, `DGORDER`, `NEWORDER`
- `BUSINV`, `ISRATIO`, `BOPGSTB`, `BOPTEXP`, `BOPTIMP`
- `FEDFUNDS`, `TB3MS`, `GS10`, `T10Y3MM`

Optional but supported ALFRED series:

- `SP500`, `NAPM`

Each series should exist in both:

- `data/raw/alfred/vintage_dates/`
- `data/raw/alfred/series_observations/`

with both `.csv` and `.json` files.

### 3. BEA API tables
Used only for **sanity checks / robustness**, not for constructing release-specific GDP targets.

Required files:

- `data/raw/bea/api/T10101_Q_ALL.json`
- `data/raw/bea/api/T10106_Q_ALL.json`

### 4. Release calendars and timing metadata
Used to construct daily real-time information sets.

Required directories:

- `data/raw/calendars/bea/`
- `data/raw/calendars/bls/`
- `data/raw/calendars/census/`
- `data/raw/calendars/fed_g17/`

Direct handling by source:

- **BEA / BLS / Federal Reserve** release calendars are used directly from the official source pages.
- **Census release timing** is handled through an ALFRED-based proxy calendar built from `realtime_start`
  availability dates for the Census-related series already used in the repo.
- The Census proxy preserves **daily availability logic** but does **not** claim exact official intraday
  release timestamps. `release_time_et` is intentionally left blank for Census proxy events.
- Census indicator values themselves remain the canonical **ALFRED / FRED** values already used elsewhere
  in the project.

---

## Environment setup

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a local `.env` file from the template:

```bash
cp .env.example .env
```

Then fill in your keys:

```text
FRED_API_KEY=YOUR_FRED_KEY
BEA_API_KEY=YOUR_BEA_KEY
CENSUS_CALENDAR_MANUAL_HTML=data/raw/calendars/census/economic_indicators_calendar.manual.html
```

`CENSUS_CALENDAR_MANUAL_HTML` is optional. Use it when Cloudflare blocks direct access to
`https://www.census.gov/economic-indicators/calendar-listview.html` and you still want to keep a
manual raw HTML export. It is not required for the ALFRED-based Census proxy calendar.

---

## Running Stage 0 download scripts

### ALFRED + calendars

```bash
python scripts/download_alfred_and_calendars.py
```

If the Census calendar is blocked from your network, the script will continue downloading the
other calendars, warn, and keep any blocked response separate from a successful raw HTML download.

### Census proxy availability calendar

```bash
python scripts/build_census_proxy_calendar.py
```

This script reads the existing ALFRED observation CSVs in
`data/raw/alfred/series_observations/{SERIES_ID}.csv` for the Census-related indicator set and
builds:

- `data/raw/calendars/census/census_proxy_release_events.csv`
- `data/raw/calendars/census/census_proxy_release_calendar.csv`
- `data/raw/calendars/census/census_proxy_calendar_metadata.json`

The output is a transparent proxy availability calendar derived from ALFRED vintage dates. It is
not an official Census release calendar and does not assert exact release-time timestamps.

If you still want to keep an official Census HTML export, point `CENSUS_CALENDAR_MANUAL_HTML` to a
local browser-saved file. If you need Census indicator values rather than timing metadata, use the
official EITS API: `https://api.census.gov/data/timeseries/eits/`

### BEA NIPA sanity tables

```bash
python scripts/download_bea.py
```

---

## Validating Stage 0

Run:

```bash
python scripts/validate_stage0.py
```

This script will:

- verify required raw files and directories
- check ALFRED coverage against the baseline panel
- report optional Census HTML artifacts and recommended Census proxy artifacts
- write a JSON report to `data/metadata/stage0_validation_report.json`
- exit with a nonzero status if required Stage 0 items are missing

---

## Stage 0 completion criteria

Stage 0 is considered **locked** only when:

- all required RTDSM raw files are present
- all required ALFRED baseline series are present in both raw ALFRED subfolders
- BEA sanity tables are present
- official BEA / BLS / Federal Reserve calendar raw files are present
- Census proxy availability calendar artifacts are generated
- `python scripts/validate_stage0.py` returns **PASS**

Only after that should the project move to Stage 1 bronze normalization.

---

## Immediate next step after Stage 0 passes

Implement the Stage 1 scripts in this order:

1. `01_parse_rtdsm_release_specific.py`
2. `02_parse_rtdsm_complete_vintages.py`
3. `03_parse_alfred_monthly.py`
4. `04_build_release_calendar_master.py`
