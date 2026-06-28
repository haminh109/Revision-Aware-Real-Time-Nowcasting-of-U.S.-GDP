# Data Directory

This directory is intentionally kept lightweight in git. It is the local working
area for raw, bronze, and silver data used by the RADFM GDP release-ladder
nowcasting pipeline.

Large vintage datasets are not committed to the repository. Rebuild or restore
them locally using the scripts and source files documented in the root
`README.md`.

Expected local layout after rebuilding:

```text
data/
├── raw/
│   ├── alfred/
│   ├── bea/
│   ├── calendars/
│   └── rtdsm/
├── bronze/
│   ├── indicators/
│   └── targets/
├── silver/
│   └── calendars/
└── metadata/
```

Minimum rebuild order:

```bash
python scripts/download_alfred_and_calendars.py
python scripts/build_census_proxy_calendar.py
python scripts/download_bea.py
python scripts/validate_stage0.py
```

Then run the Stage 1 and release-calendar scripts documented in the root
`README.md`.

Do not restore the old `../full_state/data` symlink. The cleaned repository no
longer depends on sibling branch folders.
