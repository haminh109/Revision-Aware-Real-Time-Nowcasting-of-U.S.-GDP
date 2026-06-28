# RADFM Code: Release-Structured Revision-Aware Real-Time GDP Nowcasting

This `Code/` folder contains the reproducible code, configuration files, tests,
documentation, validation metadata, and empirical output artifacts for the
course submission **Release-Ladder Real-Time Nowcasting of U.S. GDP**.

The repository root is intentionally organized as a compact submission package:

- `Final Report/`: the editable LaTeX report, compiled PDF, and report figures.
- `Code/`: source code, run scripts, tests, configs, documentation, validation
  metadata, and result outputs.

The empirical claim supported by this package is:

> Release-structured, revision-aware real-time nowcasting of U.S. GDP using a
> quarterly release-ladder state-space model fed by vintage-correct
> mixed-frequency monthly information.

Generated empirical outputs and replication-style result artifacts live under
`outputs/`. Raw and intermediate vintage data are source-dependent and are not
bundled in full; see `data/README.md` for the expected local data layout and
rebuild path.

## Reproducing and inspecting the code

The repository includes a `Makefile` so common replication commands can be run
from short, stable targets:

```bash
make help           # list available targets
make data-build     # rebuild data layers and validation reports
make pipeline-smoke # quick smoke test of the journal pipeline
make pipeline-full  # full frozen journal-candidate run; compute intensive
make pdf            # rebuild ../Final Report/Final Report.pdf
make test           # run the test suite
```

The Makefile is a convenience layer over the scripts in `scripts/`; it does not
replace the detailed runbooks in `docs/`.

The project builds a reproducible data and modeling pipeline for real-time GDP
nowcasting using:

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
Code/
├── configs/
│   └── stage0_manifest.json
├── data/
│   ├── README.md
│   └── metadata/
│       └── stage0_validation_report.json
├── docs/
├── full_state_space_release_revision_dfm/
├── outputs/
├── scripts/
├── tests/
├── .env.example
├── .gitignore
├── Makefile
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

Use Python 3.10 or newer. On this machine the verified interpreter is:

```bash
/opt/anaconda3/bin/python --version
```

Create a virtual environment and install dependencies:

```bash
/opt/anaconda3/bin/python -m venv .venv
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

---

## Journal-version modeling groundwork

The folder `full_state_space_release_revision_dfm/` contains the upgraded modeling groundwork for a later journal version:

- full Kalman filter, Rauch--Tung--Striebel smoother, and EM estimation;
- joint GDP release ladder `A/S/T/M` state-space model;
- GDP revision state;
- monthly indicator first-vintage versus mature-vintage revision state;
- an `indicator_revision_only_dfm_kalman_em` ablation to separate indicator-revision gains from GDP-revision gains;
- model-implied state-space forecast variances for Kalman/EM rows;
- density forecast utilities for intervals, log score, CRPS, and coverage;
- synthetic and bronze-data smoke examples;
- an expanding-window `prototype_quarterly` backtest runner that writes point/revision forecast CSVs and metrics under `outputs/full_state_space_release_revision_dfm/`.
- an `exact_pseudo_backtest` runner that compares `ar`, `bridge`, `standard_dfm`, `release_dfm`, `revision_dfm_kalman_em`, `indicator_revision_only_dfm_kalman_em`, and `joint_indicator_revision_dfm_full_kalman_em` on the same release-checkpoint origins.
- a `build_report_package` utility that converts exact/pseudo backtest outputs into report-ready CSV tables, robustness summaries, LaTeX snippets, figures, a manifest, and `journal_results_draft.md`.
- a `build_journal_evidence_package` utility that adds HAC DM tests, Clark-West-style diagnostics, model-confidence-set proxy tables, block-bootstrap MCS-style tables, model-implied or residual-calibrated density scores, cumulative loss tables, and data audit outputs.
- a mature-target robustness builder that creates `M_1y`, `M_3y`, and `M_latest` target panels.
- a convergence stability builder for comparing multiple `max_iter` runs.
- a variance audit builder for predictive variance, interval coverage, symmetry, and positive-semidefinite covariance checks.
- a multi-initialization audit runner for EM local-solution sensitivity.
- a journal-candidate orchestration script that builds backtests, report packages, evidence packages, variance audits, robustness runs, convergence stability, and a frozen manifest.

The full-state-space model is now wired into the exact/pseudo backtest. A report should use one frozen exact/pseudo build plus its generated report package as the source of truth.

Useful commands:

```bash
python -m full_state_space_release_revision_dfm.run_smoke_tests
python -m full_state_space_release_revision_dfm.example_bronze_smoke
python -m full_state_space_release_revision_dfm.prototype_quarterly_backtest --max-origins 6 --max-iter 6
python scripts/10_build_gdp_release_calendar_from_alfred.py
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest --max-origins 6 --max-iter 3
python -m full_state_space_release_revision_dfm.exact_pseudo_backtest --max-origins 0 --max-iter 10
python scripts/11_build_mature_target_robustness_panels.py
python -m full_state_space_release_revision_dfm.build_report_package
python -m full_state_space_release_revision_dfm.build_journal_evidence_package
python -m full_state_space_release_revision_dfm.build_variance_audit
python -m full_state_space_release_revision_dfm.build_convergence_stability_table
python -m full_state_space_release_revision_dfm.run_initialization_audit --max-origins 12 --max-iter 50
python scripts/12_run_journal_candidate_pipeline.py --max-iters 50 100 --max-origins 0 --mcs-bootstrap-reps 5000 --mature-max-iter 50 --run-initialization-audit
```

For a journal-style freeze, prefer the full-sample high-iteration run, then build both the report package and the journal evidence package. The package uses `forecast_error = forecast_value - realized_value`, so reported bias is mean forecast minus realized value.
