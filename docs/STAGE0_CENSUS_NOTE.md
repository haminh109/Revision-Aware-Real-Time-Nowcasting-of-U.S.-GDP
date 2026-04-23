The repository no longer depends on the live Census economic indicators calendar for Stage 0
usability.

Why:

1. `https://www.census.gov/economic-indicators/calendar-listview.html` is unreliable for automated
   acquisition because Cloudflare may block access.
2. The repository already uses ALFRED / FRED as the canonical source for the Census-related
   indicator values.
3. The missing component was release-day timing metadata, not the indicator values themselves.

Replacement design:

1. Census indicator values remain unchanged and continue to come from ALFRED / FRED.
2. Census release timing is represented by a proxy availability calendar derived from ALFRED
   vintage availability dates (`realtime_start`) for the Census-related series already used in the
   repo.
3. The proxy preserves daily availability logic but does not claim exact official intraday release
   timestamps.
4. `release_time_et` is intentionally left blank for Census proxy events.

Proxy artifacts:

- `data/raw/calendars/census/census_proxy_release_events.csv`
- `data/raw/calendars/census/census_proxy_release_calendar.csv`
- `data/raw/calendars/census/census_proxy_calendar_metadata.json`

Build command:

```bash
python scripts/build_census_proxy_calendar.py
```

Optional official HTML:

- `data/raw/calendars/census/economic_indicators_calendar.html` can still be stored when available.
- `CENSUS_CALENDAR_MANUAL_HTML` can point to a browser-saved HTML export if you want to keep that
  raw file for reference.
- If the live Census page is blocked, Stage 0 should continue with the proxy calendar artifacts.
