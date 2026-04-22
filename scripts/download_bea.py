import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

BEA_API_KEY = os.getenv("BEA_API_KEY")
if not BEA_API_KEY:
    raise ValueError(f"Missing BEA_API_KEY in {PROJECT_ROOT / '.env'}")

OUTDIR = PROJECT_ROOT / "data" / "raw" / "bea" / "api"
OUTDIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://apps.bea.gov/api/data/"
JOBS = {
    "T10101_Q_ALL.json": {
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": "T10101",
        "Frequency": "Q",
        "Year": "ALL",
        "ResultFormat": "JSON",
    },
    "T10106_Q_ALL.json": {
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": "T10106",
        "Frequency": "Q",
        "Year": "ALL",
        "ResultFormat": "JSON",
    },
}

for filename, params in JOBS.items():
    query = dict(params)
    query["UserID"] = BEA_API_KEY
    response = requests.get(BASE_URL, params=query, timeout=120)
    response.raise_for_status()
    data = response.json()

    with open(OUTDIR / filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved: {OUTDIR / filename}")
