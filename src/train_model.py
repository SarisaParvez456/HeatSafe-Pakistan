
# HeatSafe Pakistan - ML Training
# Experimental heat-risk categories for a hackathon demo.
# These are NOT validated worker-safety thresholds.

import pandas as pd
import numpy as np
import joblib

from pathlib import Path
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    classification_report,
    recall_score
)

# 1. Load weather data
data_path = Path("data/cleaned/heatsafe_processed_weather.csv")

df = pd.read_csv(data_path)
df["time"] = pd.to_datetime(df["time"], utc=True)
df = df.sort_values("time").reset_index(drop=True)

# 2. Prepare time features
karachi_time = df["time"].dt.tz_convert("Asia/Karachi")
df["hour_karachi"] = karachi_time.dt.hour
df["month"] = karachi_time.dt.month

# 3. Create experimental heat-risk labels
# 0 = Low, 1 = Moderate, 2 = High, 3 = Extreme
df["heat_risk_level"] = pd.cut(
    df["apparent_temperature"],
    bins=[-np.inf, 27, 32, 41, np.inf],
    labels=[0, 1, 2, 3],
    right=False
).astype(int)

# Predict the following hour
df["target_next_hour"] = df["heat_risk_level"].shift(-1)

# Keep only consecutive hourly records
next_time = df["time"].shift(-1)
consecutive = (next_time - df["time"]) == pd.Timedelta(hours=1)
df = df.loc[consecutive].copy()

# 4. Use only features available from Open-Meteo
features = [
    "meteo_temperature",
    "meteo_humidity",
    "dew_point_2m",
    "apparent_temperature",
    "meteo_wind_speed",
    "hour_karachi",
    "month"
]

df = df.dropna(subset=features + ["target_next_hour"])
df["target_next_hour"] = df["target_next_hour"].astype(int)

# 5. Chronological split: 70% train, 15% validation, 15% test
n = len(df)
train_end = int(n * 0.70)
val_end = int(n * 0.85)

train = df.iloc[:train_end]
validation = df.iloc[train_end:val_end]
test = df.iloc[val_end:]

# 6. Choose model using validation data only
weight_options = [
    {0: 1, 1: 1, 2: 1, 3: 1},
    {0: 1, 1: 1, 2: 2, 3: 8}
]

best_score = -1
best_weights = None

for weights in weight_options:
    sample_weights = (
        train["target_next_hour"].map(weights).to_numpy()
    )

    candidate = HistGradientBoostingClassifier(
        max_iter=200,
        learning_rate=0.05,
        max_leaf_nodes=15,
        l2_regularization=1.0,
        random_state=42
    )

    candidate.fit(
        train[features],
        train["target_next_hour"],
        sample_weight=sample_weights
    )

    predictions = candidate.predict(validation[features])
    score = f1_score(
        validation["target_next_hour"],
        predictions,
        average="macro",
        labels=[0, 1, 2, 3],
        zero_division=0
    )

    print("Weights:", weights, "| Validation macro F1:", round(score, 4))

    if score > best_score:
        best_score = score
        best_weights = weights

# 7. Retrain on training + validation data
training_data = pd.concat([train, validation])
final_weights = (
    training_data["target_next_hour"]
    .map(best_weights)
    .to_numpy()
)

model = HistGradientBoostingClassifier(
    max_iter=200,
    learning_rate=0.05,
    max_leaf_nodes=15,
    l2_regularization=1.0,
    random_state=42
)

model.fit(
    training_data[features],
    training_data["target_next_hour"],
    sample_weight=final_weights
)

# 8. Evaluate on the test period
y_test = test["target_next_hour"]
ml_predictions = model.predict(test[features])

# Simple baseline: next hour has same risk as current hour
baseline_predictions = test["heat_risk_level"]

print("\n--- TEST RESULTS ---")

print("Baseline macro F1:",
      round(f1_score(y_test, baseline_predictions,
                     average="macro", labels=[0, 1, 2, 3],
                     zero_division=0), 4))

print("ML accuracy:",
      round(accuracy_score(y_test, ml_predictions), 4))

print("ML macro F1:",
      round(f1_score(y_test, ml_predictions,
                     average="macro", labels=[0, 1, 2, 3],
                     zero_division=0), 4))

print("Extreme recall:",
      round(recall_score(
          y_test, ml_predictions,
          labels=[3], average="macro", zero_division=0
      ), 4))

print("\nClassification report:")
print(classification_report(
    y_test, ml_predictions,
    labels=[0, 1, 2, 3],
    target_names=["Low", "Moderate", "High", "Extreme"],
    zero_division=0
))

# 9. Save model
Path("models").mkdir(exist_ok=True)

joblib.dump(
    {"model": model, "features": features},
    "models/heatsafe_live_improved_model.pkl"
)

print("\nModel saved successfully!")

