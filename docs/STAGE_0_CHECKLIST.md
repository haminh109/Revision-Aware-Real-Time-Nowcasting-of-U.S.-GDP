# Stage 0 checklist

Use this checklist before starting Stage 1.

## A. Repository hygiene

- [ ] `README.md` explains project scope, data sources, stage definitions, folder structure, and setup
- [ ] `requirements.txt` exists and installs cleanly in a fresh virtual environment
- [ ] `.env.example` exists
- [ ] `.gitignore` excludes `.env`, `.venv/`, caches, and local metadata outputs

## B. RTDSM raw files

- [ ] `data/raw/rtdsm/routput/ROUTPUTQvQd.xlsx`
- [ ] `data/raw/rtdsm/routput/routput_first_second_third.xlsx`
- [ ] `data/raw/rtdsm/routput/routputMvQd.xlsx` (recommended)

## C. ALFRED raw files

- [ ] all baseline series exist in `data/raw/alfred/vintage_dates/`
- [ ] all baseline series exist in `data/raw/alfred/series_observations/`
- [ ] each required series has both `.csv` and `.json`

## D. BEA raw files

- [ ] `data/raw/bea/api/T10101_Q_ALL.json`
- [ ] `data/raw/bea/api/T10106_Q_ALL.json`

## E. Official release calendars

- [ ] `data/raw/calendars/bea/full_release_schedule.html`
- [ ] `data/raw/calendars/bls/employment_situation.html`
- [ ] `data/raw/calendars/bls/current_year.html`
- [ ] `data/raw/calendars/census/economic_indicators_calendar.html`
- [ ] `data/raw/calendars/fed_g17/release_dates.html`

## F. Validation

- [ ] `python scripts/validate_stage0.py` returns PASS
- [ ] `data/metadata/stage0_validation_report.json` is written successfully

## G. Exit condition

Only move to Stage 1 after every required item above is complete.
