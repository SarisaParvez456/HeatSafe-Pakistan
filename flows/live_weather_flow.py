
# HeatSafe Pakistan - Live Weather Prefect Flow

from datetime import datetime, timezone
from pathlib import Path
import json
import requests

from prefect import flow, task, get_run_logger

LATITUDE = 24.8607
LONGITUDE = 67.0011

@task(retries=2, retry_delay_seconds=10)
def fetch_live_weather():
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current": (
            "temperature_2m,relative_humidity_2m,"
            "dew_point_2m,apparent_temperature,"
            "wind_speed_10m"
        ),
        "timezone": "Asia/Karachi"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    current = data["current"]

    required = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "wind_speed_10m"
    ]

    for field in required:
        if current.get(field) is None:
            raise ValueError(f"Missing weather field: {field}")

    return {
        "source": "Open-Meteo",
        "location": "Karachi, Pakistan",
        "weather_time": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "dew_point": current["dew_point_2m"],
        "apparent_temperature": current["apparent_temperature"],
        "wind_speed": current["wind_speed_10m"],
        "fetched_at_utc": datetime.now(
            timezone.utc
        ).isoformat()
    }


@flow(name="HeatSafe Karachi Live Weather")
def live_weather_flow():
    logger = get_run_logger()

    weather = fetch_live_weather()

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "latest_weather.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(weather, file, indent=2)

    logger.info(
        "Weather fetched successfully: %s",
        weather["weather_time"]
    )
    logger.info(
        "Saved weather to %s",
        output_file
    )

    return weather


if __name__ == "__main__":
    live_weather_flow()
