
# HeatSafe Pakistan - DuckDB Data Warehouse

from pathlib import Path
import duckdb

DATABASE_PATH = Path("data/heatsafe.duckdb")

DATASETS = {
    "raw_open_meteo": "data/raw/open_meteo_karachi_2022_2026.csv",
    "raw_nasa_power": "data/raw/nasa_power_karachi_2022_2026.csv",
    "cleaned_weather": "data/cleaned/heatsafe_processed_weather.csv",
    "processed_weather": "data/processed/heatsafe_ml_dataset.csv",
}


def build_warehouse():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(DATABASE_PATH))

    try:
        for table_name, file_name in DATASETS.items():
            path = Path(file_name)

            if not path.exists():
                raise FileNotFoundError(
                    f"Required dataset missing: {file_name}"
                )

            connection.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" '
                "AS SELECT * FROM read_csv_auto(?)",
                [str(path)]
            )

            count = connection.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]

            print(f"{table_name}: {count} rows")

        print(f"Warehouse saved: {DATABASE_PATH}")
        print("DuckDB warehouse completed successfully!")

    finally:
        connection.close()


if __name__ == "__main__":
    build_warehouse()
