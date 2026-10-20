
from pathlib import Path
import hashlib
import json

csv_path = Path("data/raw/openaq_karachi_pm25.csv")
metadata_path = Path("data/raw/openaq_karachi_metadata.json")

if not csv_path.exists():
    raise FileNotFoundError(csv_path)

sha256 = hashlib.sha256(csv_path.read_bytes()).hexdigest()

with csv_path.open(encoding="utf-8") as file:
    rows = sum(1 for _ in file) - 1

print("FILE:", csv_path)
print("SHA256:", sha256)
print("ROWS:", rows)

if metadata_path.exists():
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    print("FETCH_TIME:", metadata.get("fetched_at_utc"))
    print("SOURCE:", metadata.get("source"))
    print("ENDPOINT:", metadata.get("endpoint"))
    print("LICENCE:", metadata.get("licence"))
    print("UNITS:", metadata.get("units"))
