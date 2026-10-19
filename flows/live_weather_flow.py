
# HeatSafe Pakistan - Live Weather and ML Prediction

from datetime import datetime, timezone
from pathlib import Path
import json

import joblib
import pandas as pd
import requests

from prefect import flow, task, get_run_logger


LATITUDE = 24.8607
LONGITUDE = 67.0011

MODEL_PATH = Path("models/heatsafe_live_improved_model.pkl")


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

    response = requests.get(
        url,
        params=params,
        timeout=30
    )
    response.raise_for_status()

    current = response.json()["current"]

    required = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "wind_speed_10m",
        "time"
    ]

    for field in required:
        if current.get(field) is None:
            raise ValueError(
                f"Missing weather field: {field}"
            )

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


@task
def predict_heat_risk(weather):
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found: {MODEL_PATH}"
        )

    # Load only our own trusted model file.
    saved = joblib.load(MODEL_PATH)

    model = saved["model"]
    features = saved["features"]

    weather_time = datetime.fromisoformat(
        weather["weather_time"]
    )

    input_data = pd.DataFrame([{
        "meteo_temperature": weather["temperature"],
        "meteo_humidity": weather["humidity"],
        "dew_point_2m": weather["dew_point"],
        "apparent_temperature": weather["apparent_temperature"],
        "meteo_wind_speed": weather["wind_speed"],
        "hour_karachi": weather_time.hour,
        "month": weather_time.month
    }])

    prediction = model.predict(
        input_data[features]
    )

    risk_level = int(prediction[0])

    risk_names = {
        0: "Low",
        1: "Moderate",
        2: "High",
        3: "Extreme"
    }

    return {
        **weather,
        "prediction_for": "Next hour",
        "predicted_risk_level": risk_level,
        "predicted_risk_name": risk_names.get(
            risk_level,
            "Unknown"
        ),
        "model_status": "Experimental demo"
    }


@flow(name="HeatSafe Karachi Live Prediction")
def live_weather_flow():
    logger = get_run_logger()

    weather = fetch_live_weather()
    result = predict_heat_risk(weather)

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "latest_prediction.json"

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(result, file, indent=2)

    logger.info(
        "Weather time: %s",
        result["weather_time"]
    )
    logger.info(
        "Predicted next-hour risk: %s",
        result["predicted_risk_name"]
    )
    logger.info(
        "Prediction saved: %s",
        output_file
    )

    return result


if __name__ == "__main__":
    live_weather_flow()
