
# HeatSafe Pakistan - Download OpenAQ Karachi PM2.5
# Third independent data source

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

API_KEY = os.environ.get("OPENAQ_API_KEY")
SENSOR_ID = 13368464

if not API_KEY:
    raise RuntimeError("OPENAQ_API_KEY secret is missing")

HEADERS = {
    "X-API-Key": API_KEY,
    "accept": "application/json",
}

BASE_URL = (
    f"https://api.openaq.org/v3/sensors/{SENSOR_ID}/hours"
)

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = OUTPUT_DIR / "openaq_karachi_pm25.csv"
METADATA_PATH = OUTPUT_DIR / "openaq_karachi_metadata.json"


def main():
    rows = []
    page = 1
    limit = 1000
    max_pages = 5

    while page <= max_pages:
        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params={
                "limit": limit,
                "page": page,
                "datetime_from": "2026-10-01T00:00:00Z",
                "datetime_to": "2026-10-21T00:00:00Z",
            },
            timeout=60,
        )

        response.raise_for_status()
        payload = response.json()
        results = payload.get("results", [])

        if not results:
            break

        for item in results:
            period = item.get("period", {})
            datetime_from = period.get("datetimeFrom", {})

            rows.append({
                "row_id": (
                    f"OPENAQ_{SENSOR_ID}_"
                    f"{datetime_from.get('utc', '')}"
                ),
                "sensor_id": SENSOR_ID,
                "station": "Urban Resource Center",
                "datetime_utc": datetime_from.get("utc"),
                "pm25": item.get("value"),
            })

        print(f"Page {page}: {len(results)} measurements")

        if len(results) < limit:
            break

        page += 1

    if not rows:
        raise RuntimeError(
            "No OpenAQ measurements returned for this sensor/date range"
        )

    with CSV_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "row_id",
                "sensor_id",
                "station",
                "datetime_utc",
                "pm25",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    metadata = {
        "source": "OpenAQ API v3",
        "endpoint": BASE_URL,
        "sensor_id": SENSOR_ID,
        "station": "Urban Resource Center",
        "query_start_utc": "2026-10-01T00:00:00Z",
        "query_end_utc": "2026-10-21T00:00:00Z",
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": len(rows),
        "units": "TO_VERIFY",
        "licence": "TO_VERIFY",
    }

    METADATA_PATH.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(f"Downloaded {len(rows)} PM2.5 measurements")
    print(f"CSV saved: {CSV_PATH}")
    print(f"Metadata saved: {METADATA_PATH}")


if __name__ == "__main__":
    main()
