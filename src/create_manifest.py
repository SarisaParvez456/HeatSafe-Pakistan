
from pathlib import Path
from datetime import datetime, timezone
import csv
import hashlib

RAW_FILES = [
    ("data/raw/open_meteo_karachi_2022_2026.csv", "Open-Meteo"),
    ("data/raw/nasa_power_karachi_2022_2026.csv", "NASA POWER"),
]

def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def main():
    timestamp = datetime.now(timezone.utc).isoformat()

    with open("manifest.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "file", "source", "query", "fetch_time",
            "sha256", "licence"
        ])

        for filename, source in RAW_FILES:
            path = Path(filename)
            if not path.exists():
                raise FileNotFoundError(filename)

            writer.writerow([
                filename,
                source,
                "Karachi hourly weather, 2022-01-01 to 2026-09-30",
                "",  # Original fetch time must be verified
                sha256_file(path),
                "TO_VERIFY"
            ])

    print("manifest.csv created successfully!")
    print(f"Manifest generated at: {timestamp}")
    print("Note: Original fetch times and licences still need verification.")

if __name__ == "__main__":
    main()
