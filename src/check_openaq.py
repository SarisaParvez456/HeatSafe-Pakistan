
# HeatSafe Pakistan - Check OpenAQ Karachi Data

import os
import json
import requests

API_KEY = os.environ.get("OPENAQ_API_KEY")

if not API_KEY:
    raise RuntimeError("OPENAQ_API_KEY secret is missing")

BASE_URL = "https://api.openaq.org/v3"

HEADERS = {
    "X-API-Key": API_KEY,
    "accept": "application/json",
}


def get_locations():
    response = requests.get(
        f"{BASE_URL}/locations",
        headers=HEADERS,
        params={
            "coordinates": "24.8607,67.0011",
            "radius": 25000,
            "limit": 100,
        },
        timeout=30,
    )

    response.raise_for_status()
    return response.json().get("results", [])


def main():
    locations = get_locations()

    print(f"OpenAQ locations found: {len(locations)}")

    summary = []

    for location in locations:
        sensors = location.get("sensors", [])

        pm25_sensors = [
            sensor for sensor in sensors
            if sensor.get("parameter", {}).get("name", "").lower()
            in ("pm25", "pm2.5")
        ]

        if not pm25_sensors:
            continue

        item = {
            "location_id": location.get("id"),
            "name": location.get("name"),
            "locality": location.get("locality"),
            "datetime_last": location.get("datetimeLast"),
            "pm25_sensors": [
                {
                    "sensor_id": sensor.get("id"),
                    "parameter": sensor.get("parameter", {}).get("name"),
                }
                for sensor in pm25_sensors
            ],
        }

        summary.append(item)

    print(f"Locations with PM2.5 sensors: {len(summary)}")
    print(json.dumps(summary, indent=2))

    with open(
        "openaq_karachi_check.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(summary, file, indent=2)


if __name__ == "__main__":
    main()
