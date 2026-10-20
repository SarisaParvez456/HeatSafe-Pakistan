
import csv
import hashlib
from pathlib import Path


def calculate_sha256(filename):
    digest = hashlib.sha256()

    with open(filename, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


manifest_path = Path("manifest.csv")

if not manifest_path.exists():
    raise FileNotFoundError("manifest.csv is missing")

with open(manifest_path, encoding="utf-8-sig", newline="") as file:
    # Ignore empty lines before the header
    nonempty_lines = [
        line for line in file
        if line.strip()
    ]

if not nonempty_lines:
    raise ValueError("manifest.csv is empty")

reader = csv.DictReader(nonempty_lines)

reader.fieldnames = [
    name.strip() for name in reader.fieldnames
]

required = {
    "file", "source", "query",
    "fetch_time", "sha256", "licence"
}

missing = required - set(reader.fieldnames)

if missing:
    raise ValueError(
        f"Missing columns: {sorted(missing)}. "
        f"Found: {reader.fieldnames}. "
        f"First line: {nonempty_lines[0]!r}"
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
        raise ValueError(f"SHA-256 mismatch: {filename}")

    print(f"PASS: {filename}")

print("All raw files match the manifest!")
