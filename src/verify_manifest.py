
import csv
import hashlib
from pathlib import Path


def calculate_sha256(filename):
    digest = hashlib.sha256()

    with open(filename, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


with open("manifest.csv", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    if reader.fieldnames is None:
        raise ValueError("manifest.csv is empty")

    reader.fieldnames = [
        name.strip() for name in reader.fieldnames
    ]

    required = {"file", "source", "query", "fetch_time", "sha256", "licence"}

    missing = required - set(reader.fieldnames)

    if missing:
        raise ValueError(
            f"Missing manifest columns: {sorted(missing)}. "
            f"Found: {reader.fieldnames}"
        )

    records = list(reader)


if not records:
    raise ValueError("Manifest contains no data rows")


for record in records:
    filename = record["file"].strip()
    expected_hash = record["sha256"].strip().lower()

    if not Path(filename).is_file():
        raise FileNotFoundError(filename)

    actual_hash = calculate_sha256(filename)

    if actual_hash != expected_hash:
        raise ValueError(
            f"SHA-256 mismatch for {filename}"
        )

    print(f"PASS: {filename}")


print("All raw files match the manifest!")
