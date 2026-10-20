
import csv
import hashlib
from pathlib import Path


def calculate_sha256(filename):
    digest = hashlib.sha256()

    with open(filename, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


with open("manifest.csv", newline="") as file:
    records = list(csv.DictReader(file))

if not records:
    raise ValueError("Manifest is empty!")

for record in records:
    filename = record["file"]
    expected_hash = record["sha256"]

    if not Path(filename).is_file():
        raise FileNotFoundError(filename)

    actual_hash = calculate_sha256(filename)

    if actual_hash != expected_hash:
        raise ValueError(f"SHA-256 mismatch: {filename}")

    print(f"PASS: {filename}")

print("All raw files match the manifest!")
