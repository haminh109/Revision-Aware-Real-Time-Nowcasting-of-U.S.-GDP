# GDP Release Calendar Note

The RTDSM release-stage target file used in this repository contains first, second, third, and most-recent GDP release values, but it does not provide daily publication timestamps for each target quarter.

To reduce the deterministic timing approximation in the exact/pseudo backtest, the project now builds:

```text
data/silver/calendars/gdp_release_calendar_alfred.csv
```

using:

```text
data/bronze/indicators/alfred_monthly_long.csv
series_id = GDPC1
```

For each target quarter, the builder takes the first three `GDPC1` ALFRED `realtime_start` dates after the quarter end as the A/S/T GDP release dates, provided they fall within a broad 180-day plausibility window. This handles delayed releases such as 2018Q4 while avoiding false matches caused by the left edge of the ALFRED vintage archive. The mature-release row uses the first vintage at least 12 quarters after the target quarter where available.

The exact backtest then sets the forecast origin to one business day before the mapped GDP release date. Release time is recorded using the BEA 08:30 ET convention.

For the headline 2005Q1--2024Q4 A/S/T evaluation sample, all GDP release dates are derived from ALFRED `GDPC1` vintages rather than deterministic month-end fallback dates.

Remaining limitation: this is a vintage-date-derived GDP release calendar, not a separately scraped official BEA intraday news-release archive. A journal submission should describe it as ALFRED vintage-derived and, where feasible, cross-check a sample of dates against BEA archive pages.
