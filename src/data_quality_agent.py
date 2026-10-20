
"""
HeatSafe Pakistan - Data Quality Agent

Validates cleaned historical weather data with Pandera.
Records quality decisions and evidence in CSV and JSON files.

FIX  = safe, deterministic correction
FILL = missing value replaced using an approved rule
FLAG = suspicious data kept for investigation

This version flags suspicious measurements rather than
silently changing or filling them.
"""

from pathlib import Path
from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema


INPUT = Path("data/cleaned/heatsafe_cleaned_weather.csv")
OUTPUT = Path("outputs/data_quality")

OUTPUT.mkdir(parents=True, exist_ok=True)

AUDIT_FILE = OUTPUT / "quality_audit.csv"
REPORT_FILE = OUTPUT / "quality_report.json"

REQUIRED_NUMERIC = {
    "meteo_temperature": (-30, 60),
    "nasa_temperature": (-30, 60),
    "meteo_humidity": (0, 100),
    "nasa_humidity": (0, 100),
    "dew_point_2m": (-40, 50),
    "apparent_temperature": (-40, 75),
    "meteo_wind_speed": (0, 200),
    "nasa_wind_speed": (0, 200),
}

# Broad screening bounds, not worker-safety thresholds.
# Wind bounds assume both source columns use km/h.
# Verify source units before treating these as final rules.

audit = []
run_time = datetime.now(timezone.utc).isoformat()


def add_issue(rule, action, reason, row_ids):
    for row_id in row_ids:
        audit.append({
            "timestamp_utc": run_time,
            "row_id": str(row_id),
            "rule": rule,
            "action": action,
            "reason": reason,
        })


def main():
    if not INPUT.exists():
        raise FileNotFoundError(INPUT)

    df = pd.read_csv(INPUT)

    if "time" not in df.columns:
        raise ValueError("Required column missing: time")

    # Stable identifier based on the hourly timestamp.
    # This identifies a record across repeated pipeline runs.
    df["row_id"] = df["time"].astype(str)

    schema = DataFrameSchema(
        {
            "time": Column(str, nullable=False),
            **{
                name: Column(
                    float,
                    checks=Check.in_range(low, high),
                    nullable=True,
                    coerce=True,
                )
                for name, (low, high) in REQUIRED_NUMERIC.items()
            },
        },
        strict=False,
        coerce=False,
    )

    # Schema validation: record failures without silently
    # removing any rows from the cleaned dataset.
    try:
        schema.validate(df, lazy=True)
        schema_status = "PASS"
    except pa.errors.SchemaErrors as error:
        schema_status = "FLAG"
        failures = error.failure_cases

        for _, failure in failures.iterrows():
            index = failure.get("index")
            if pd.notna(index) and int(index) in df.index:
                row_ids = [df.loc[int(index), "row_id"]]
            else:
                row_ids = ["SCHEMA"]

            add_issue(
                "pandera_schema",
                "FLAG",
                str(failure.get("failure_case")),
                row_ids,
            )

    # Missing values
    for column in REQUIRED_NUMERIC:
        if column not in df.columns:
            continue

        missing = df[column].isna()
        if missing.any():
            add_issue(
                f"missing_{column}",
                "FLAG",
                "Missing measurement; no unsupported fill applied",
                df.loc[missing, "row_id"],
            )

    # Duplicate timestamps
    duplicates = df["time"].duplicated(keep=False)

    if duplicates.any():
        add_issue(
            "duplicate_timestamp",
            "FLAG",
            "Repeated timestamp detected",
            df.loc[duplicates, "row_id"],
        )

    # Missing hourly timestamps
    timestamps = pd.to_datetime(
        df["time"], utc=True, errors="coerce"
    )

    invalid_times = timestamps.isna()

    if invalid_times.any():
        add_issue(
            "invalid_timestamp",
            "FLAG",
            "Timestamp could not be parsed",
            df.loc[invalid_times, "row_id"],
        )

    valid_times = timestamps.dropna().sort_values()

    if not valid_times.empty:
        expected = pd.date_range(
            valid_times.min(),
            valid_times.max(),
            freq="h",
            tz="UTC",
        )

        missing_hours = expected.difference(
            pd.DatetimeIndex(valid_times)
        )

        for missing_hour in missing_hours:
            add_issue(
                "missing_hour",
                "FLAG",
                "Expected hourly record is absent",
                [missing_hour.isoformat()],
            )

    # Existing cross-source disagreement rule
    if "temperature_difference" in df.columns:
        disagreement = (
            pd.to_numeric(
                df["temperature_difference"],
                errors="coerce",
            ) > 5
        )

        if disagreement.any():
            add_issue(
                "temperature_source_disagreement",
                "FLAG",
                "Open-Meteo and NASA differ by more than 5 C",
                df.loc[disagreement, "row_id"],
            )

    # Historical data freshness:
    # This archive intentionally ends September 2026.
    # Do not label it stale using live-weather standards.
    latest_data_time = (
        valid_times.max().isoformat()
        if not valid_times.empty
        else None
    )

    # Distribution drift:
    # Compare the earlier 80% with the latest 20%.
    # This is a monitoring signal, not proof of an error.
    ordered = df.assign(
        parsed_time=timestamps
    ).sort_values("parsed_time")

    split = int(len(ordered) * 0.8)
    reference = ordered.iloc[:split]
    recent = ordered.iloc[split:]

    drift_results = {}

    for column in [
        "meteo_temperature",
        "meteo_humidity",
        "meteo_wind_speed",
    ]:
        if column not in df.columns:
            continue

        reference_values = pd.to_numeric(
            reference[column], errors="coerce"
        ).dropna()

        recent_values = pd.to_numeric(
            recent[column], errors="coerce"
        ).dropna()

        if reference_values.empty or recent_values.empty:
            continue

        reference_mean = float(reference_values.mean())
        recent_mean = float(recent_values.mean())

        reference_std = float(
            reference_values.std(ddof=0)
        )

        standardized_shift = (
            abs(recent_mean - reference_mean)
            / max(reference_std, 0.001)
        )

        drift_results[column] = {
            "reference_mean": reference_mean,
            "recent_mean": recent_mean,
            "standardized_mean_shift": standardized_shift,
            "flagged": standardized_shift > 1.0,
        }

        if standardized_shift > 1.0:
            add_issue(
                f"distribution_drift_{column}",
                "FLAG",
                "Mean shift exceeds 1 reference standard deviation; "
                "seasonality may explain this",
                ["DATASET"],
            )

    audit_columns = [
        "timestamp_utc",
        "row_id",
        "rule",
        "action",
        "reason",
    ]

    pd.DataFrame(
        audit, columns=audit_columns
    ).to_csv(AUDIT_FILE, index=False)

    report = {
        "run_time_utc": run_time,
        "dataset": str(INPUT),
        "rows_checked": len(df),
        "schema_status": schema_status,
        "audit_events": len(audit),
        "unique_affected_ids": len({
            item["row_id"] for item in audit
        }),
        "latest_historical_timestamp": latest_data_time,
        "historical_freshness_note":
            "Archive ends in September 2026 by design; "
            "live source freshness must be checked separately",
        "distribution_drift": drift_results,
        "audit_file": str(AUDIT_FILE),
        "overall_status": (
            "PASS" if not audit else "REVIEW_REQUIRED"
        ),
    }

    REPORT_FILE.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("Data Quality Agent completed")
    print("Rows checked:", len(df))
    print("Schema status:", schema_status)
    print("Audit events:", len(audit))
    print("Overall:", report["overall_status"])
    print("Audit:", AUDIT_FILE)
    print("Report:", REPORT_FILE)


if __name__ == "__main__":
    main()
