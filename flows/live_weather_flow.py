
# HeatSafe Pakistan - Live Weather and ML Prediction
# Includes live data quality checks and freshness monitoring.

from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import math

import joblib
import pandas as pd
import requests

from prefect import flow, task, get_run_logger


LATITUDE = 24.8607
LONGITUDE = 67.0011

MODEL_PATH = Path("models/heatsafe_live_improved_model.pkl")
OUTPUT_DIR = Path("outputs")

# Screening bounds, not official worker-safety thresholds.
# Wind speed is assumed to be km/h from Open-Meteo.
VALID_RANGES = {
    "temperature": (-30, 60),
    "humidity": (0, 100),
    "dew_point": (-40, 50),
    "apparent_temperature": (-40, 75),
    "wind_speed": (0, 200),
}

# Allow weather observations up to 2 hours old.
# This is our project monitoring rule, not a provider guarantee.
MAX_AGE_HOURS = 2


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
        "timezone": "Asia/Karachi",
        "wind_speed_unit": "kmh",
    }

    response = requests.get(
        url,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    current = payload["current"]

    required = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "wind_speed_10m",
        "time",
    ]

    for field in required:
        if current.get(field) is None:
            raise ValueError(f"Missing weather field: {field}")

    return {
        "source": "Open-Meteo",
        "location": "Karachi, Pakistan",
        "weather_time": current["time"],
        "weather_timezone": payload.get(
            "timezone", "Asia/Karachi"
        ),
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "dew_point": current["dew_point_2m"],
        "apparent_temperature": current["apparent_temperature"],
        "wind_speed": current["wind_speed_10m"],
        "fetched_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }


@task
def check_live_data_quality(weather):
    logger = get_run_logger()
    checked_at = datetime.now(timezone.utc)

    audit = []

    def flag(rule, reason):
        audit.append({
            "timestamp_utc": checked_at.isoformat(),
            "row_id": weather.get("weather_time", "UNKNOWN"),
            "rule": rule,
            "action": "FLAG",
            "reason": reason,
        })

    # Open-Meteo returns local time without an offset
    # when timezone=Asia/Karachi.
    try:
        local_weather_time = pd.Timestamp(
            weather["weather_time"]
        )

        if local_weather_time.tzinfo is None:
            local_weather_time = local_weather_time.tz_localize(
                "Asia/Karachi"
            )
        else:
            local_weather_time = local_weather_time.tz_convert(
                "Asia/Karachi"
            )

        weather_time_utc = local_weather_time.tz_convert("UTC")
        age_hours = (
            checked_at - weather_time_utc.to_pydatetime()
        ).total_seconds() / 3600

        if age_hours > MAX_AGE_HOURS:
            flag(
                "live_data_freshness",
                f"Weather observation is {age_hours:.2f} hours old",
            )

        if age_hours < -0.5:
            flag(
                "future_weather_timestamp",
                f"Weather timestamp is {-age_hours:.2f} hours ahead",
            )

    except (ValueError, TypeError) as error:
        age_hours = None
        flag("invalid_weather_time", str(error))

    # Validate numeric readings.
    for field, (minimum, maximum) in VALID_RANGES.items():
        try:
            value = float(weather[field])

            if not math.isfinite(value):
                flag(
                    f"invalid_{field}",
                    "Reading is not a finite number",
                )
            elif not minimum <= value <= maximum:
                flag(
                    f"range_{field}",
                    f"Value {value} outside [{minimum}, {maximum}]",
                )

        except (KeyError, ValueError, TypeError):
            flag(
                f"missing_{field}",
                "Missing or nonnumeric weather reading",
            )

    quality = {
        "checked_at_utc": checked_at.isoformat(),
        "source": weather["source"],
        "weather_time": weather["weather_time"],
        "age_hours": (
            round(age_hours, 3)
            if age_hours is not None
            else None
        ),
        "status": "PASS" if not audit else "REVIEW_REQUIRED",
        "audit_events": len(audit),
        "audit": audit,
    }

    logger.info(
        "Live weather quality: %s (%d issues)",
        quality["status"],
        len(audit),
    )

    return quality


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

    weather_time = pd.Timestamp(weather["weather_time"])

    if weather_time.tzinfo is None:
        weather_time = weather_time.tz_localize(
            "Asia/Karachi"
        )
    else:
        weather_time = weather_time.tz_convert(
            "Asia/Karachi"
        )

    input_data = pd.DataFrame([{
        "meteo_temperature": weather["temperature"],
        "meteo_humidity": weather["humidity"],
        "dew_point_2m": weather["dew_point"],
        "apparent_temperature": weather["apparent_temperature"],
        "meteo_wind_speed": weather["wind_speed"],
        "hour_karachi": weather_time.hour,
        "month": weather_time.month,
    }])

    prediction = model.predict(input_data[features])
    risk_level = int(prediction[0])

    risk_names = {
        0: "Low",
        1: "Moderate",
        2: "High",
        3: "Extreme",
    }

    return {
        **weather,
        "prediction_for": "Next hour",
        "predicted_risk_level": risk_level,
        "predicted_risk_name": risk_names.get(
            risk_level, "Unknown"
        ),
        "model_status": "Experimental demo",
    }


@flow(name="HeatSafe Karachi Live Prediction")
def live_weather_flow():
    logger = get_run_logger()

    weather = fetch_live_weather()
    quality = check_live_data_quality(weather)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    quality_file = OUTPUT_DIR / "live_quality_report.json"

    with open(quality_file, "w", encoding="utf-8") as file:
        json.dump(quality, file, indent=2)

    if quality["status"] != "PASS":
        logger.warning(
            "Live data quality failed; prediction withheld"
        )

        result = {
            **weather,
            "prediction_for": "Next hour",
            "predicted_risk_level": None,
            "predicted_risk_name": "Unavailable",
            "model_status": "Prediction withheld: data quality review",
            "data_quality_status": quality["status"],
            "quality_audit_events": quality["audit_events"],
        }

    else:
        result = predict_heat_risk(weather)
        result["data_quality_status"] = quality["status"]
        result["quality_audit_events"] = 0

    result["last_successful_pipeline_run_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    output_file = OUTPUT_DIR / "latest_prediction.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(result, file, indent=2)

    logger.info(
        "Weather time: %s",
        result["weather_time"],
    )
    logger.info(
        "Prediction status: %s",
        result["predicted_risk_name"],
    )
    logger.info(
        "Prediction saved: %s",
        output_file,
    )

    return result


if __name__ == "__main__":
    live_weather_flow()
