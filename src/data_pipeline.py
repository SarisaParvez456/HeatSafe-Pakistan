
# HeatSafe Pakistan - Data Pipeline
# Combines historical NASA POWER and Open-Meteo weather data.
# Does not retrain the ML model.

from pathlib import Path
import pandas as pd
import numpy as np

RAW_DIR = Path("data/raw")
CLEAN_DIR = Path("data/cleaned")
PROCESSED_DIR = Path("data/processed")

METEO_FILE = RAW_DIR / "open_meteo_karachi_2022_2026.csv"
NASA_FILE = RAW_DIR / "nasa_power_karachi_2022_2026.csv"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# 1. Read original CSV files
meteo = pd.read_csv(METEO_FILE)
nasa = pd.read_csv(NASA_FILE)

# 2. Standardize timestamps
meteo["time"] = pd.to_datetime(meteo["time"], utc=True)
nasa["time"] = pd.to_datetime(nasa["time"], utc=True)

# Rename original API columns to our project column names

meteo = meteo.rename(columns={
    "temperature_2m": "meteo_temperature",
    "relative_humidity_2m": "meteo_humidity",
    "wind_speed_10m": "meteo_wind_speed"
})

nasa = nasa.rename(columns={
    "T2M": "nasa_temperature",
    "RH2M": "nasa_humidity",
    "WS2M": "nasa_wind_speed"
})

# 3. Check expected columns
meteo_columns = [
    "time",
    "meteo_temperature",
    "meteo_humidity",
    "dew_point_2m",
    "apparent_temperature",
    "meteo_wind_speed",
]

nasa_columns = [
    "time",
    "nasa_temperature",
    "nasa_humidity",
    "nasa_wind_speed",
]

missing_meteo = set(meteo_columns) - set(meteo.columns)
missing_nasa = set(nasa_columns) - set(nasa.columns)

if missing_meteo:
    raise ValueError(
        f"Missing Open-Meteo columns: {sorted(missing_meteo)}"
    )

if missing_nasa:
    raise ValueError(
        f"Missing NASA columns: {sorted(missing_nasa)}"
    )

# 4. Check for duplicate timestamps
if meteo["time"].duplicated().any():
    raise ValueError("Duplicate timestamps in Open-Meteo data")

if nasa["time"].duplicated().any():
    raise ValueError("Duplicate timestamps in NASA data")

# 5. Combine datasets
combined = pd.merge(
    meteo[meteo_columns],
    nasa[nasa_columns],
    on="time",
    how="inner",
    validate="one_to_one",
)

combined = combined.sort_values("time").reset_index(drop=True)

if combined.empty:
    raise ValueError("No matching timestamps between datasets")

# 6. Calculate source disagreements
combined["temperature_difference"] = (
    combined["meteo_temperature"]
    - combined["nasa_temperature"]
).abs()

combined["humidity_difference"] = (
    combined["meteo_humidity"]
    - combined["nasa_humidity"]
).abs()

# 7. Data quality checks
# 5 degrees Celsius is a demo review rule, not a safety limit.
combined["temperature_quality_flag"] = (
    combined["temperature_difference"] > 5
)

combined["quality_action"] = np.where(
    combined["temperature_quality_flag"],
    "FLAG_FOR_REVIEW",
    "KEEP",
)

combined["quality_reason"] = np.where(
    combined["temperature_quality_flag"],
    "Temperature disagreement exceeds 5 deg C",
    "Temperature disagreement is within 5 deg C",
)

# 8. Save cleaned dataset
clean_path = CLEAN_DIR / "heatsafe_cleaned_weather.csv"
combined.to_csv(clean_path, index=False)

print("Cleaned data saved:", clean_path)
print("Total rows:", len(combined))
print("Flagged rows:",
      int(combined["temperature_quality_flag"].sum()))

# 9. Prepare ML dataset
processed = combined.copy()

karachi_time = processed["time"].dt.tz_convert("Asia/Karachi")

processed["hour_karachi"] = karachi_time.dt.hour
processed["month"] = karachi_time.dt.month

# Experimental demo categories, NOT validated worker-safety limits.
processed["heat_risk_level"] = pd.cut(
    processed["apparent_temperature"],
    bins=[-np.inf, 27, 32, 41, np.inf],
    labels=[0, 1, 2, 3],
    right=False,
).astype(int)

# Target is the next consecutive hour's category.
next_time = processed["time"].shift(-1)
next_risk = processed["heat_risk_level"].shift(-1)

consecutive_hour = (
    next_time - processed["time"]
) == pd.Timedelta(hours=1)

processed["target_next_hour"] = next_risk.where(
    consecutive_hour
)

# 10. Save processed dataset
processed_path = (
    PROCESSED_DIR / "heatsafe_processed_weather.csv"
)

processed.to_csv(processed_path, index=False)

print("Processed data saved:", processed_path)
print("Rows:", len(processed))
print("Pipeline completed successfully!")
