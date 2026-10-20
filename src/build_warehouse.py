
# HeatSafe Pakistan - DuckDB Data Warehouse
# Builds raw, cleaned, and processed tables.
# Verifies stable row IDs and matching row counts.

from pathlib import Path
import duckdb

DATABASE_PATH = Path("data/heatsafe.duckdb")

DATASETS = {
    "raw_open_meteo": "data/raw/open_meteo_karachi_2022_2026.csv",
    "raw_nasa_power": "data/raw/nasa_power_karachi_2022_2026.csv",
    "cleaned_weather": "data/cleaned/heatsafe_cleaned_weather.csv",
    "processed_weather": "data/processed/heatsafe_processed_weather.csv",
}


def build_warehouse():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(DATABASE_PATH))

    try:
        # 1. Create warehouse tables
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

        # 2. Check row IDs in cleaned and processed tables
        for table_name in ["cleaned_weather", "processed_weather"]:
            columns = {
                row[0]
                for row in connection.execute(
                    f'DESCRIBE "{table_name}"'
                ).fetchall()
            }

            if "row_id" not in columns:
                raise ValueError(
                    f"Missing row_id in {table_name}"
                )

            duplicate_ids = connection.execute(
                f'''
                SELECT COUNT(*) - COUNT(DISTINCT row_id)
                FROM "{table_name}"
                '''
            ).fetchone()[0]

            missing_ids = connection.execute(
                f'''
                SELECT COUNT(*)
                FROM "{table_name}"
                WHERE row_id IS NULL OR row_id = ''
                '''
            ).fetchone()[0]

            if duplicate_ids or missing_ids:
                raise ValueError(
                    f"{table_name}: {duplicate_ids} duplicate IDs, "
                    f"{missing_ids} missing IDs"
                )

            print(f"{table_name}: row IDs verified")

        # 3. Verify cleaned and processed row counts
        cleaned_count = connection.execute(
            "SELECT COUNT(*) FROM cleaned_weather"
        ).fetchone()[0]

        processed_count = connection.execute(
            "SELECT COUNT(*) FROM processed_weather"
        ).fetchone()[0]

        if cleaned_count != processed_count:
            raise ValueError(
                "Cleaned and processed row counts do not match: "
                f"{cleaned_count} vs {processed_count}"
            )

        print("Cleaned and processed row counts match")

        # 4. Success
        print(f"Warehouse saved: {DATABASE_PATH}")
        print("DuckDB warehouse completed successfully!")

    finally:
        connection.close()


if __name__ == "__main__":
    build_warehouse()
