import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# Path to the latest raw file (change filename if needed)
RAW_FILE = sorted(Path("data").glob("opensky_raw_*.json"))[-1]

with open(RAW_FILE, "r") as f:
    raw_data = json.load(f)

snapshot_time = raw_data["time"]
states = raw_data["states"]

clean_rows = []

for s in states:
    row = {
        "icao24": s[0],
        "callsign": s[1].strip() if s[1] else None,
        "origin_country": s[2],
        "event_time": snapshot_time,
        "longitude": s[5],
        "latitude": s[6],
        "altitude": s[7],
        "velocity": s[9],
        "heading": s[10],
    }
    # If these are null we keep the rows
    if (
        row["latitude"] is not None
        and row["longitude"] is not None 
        and row["altitude"] is not None
        and row["velocity"] is not None
    ):
        clean_rows.append(row)

# Show sample output
for r in clean_rows[:5]:
    print(r)

print(f"\nTotal aircraft records: {len(clean_rows)}")



# Convert list of dicts to Arrow Table
table = pa.Table.from_pylist(clean_rows)

# Create output folder
Path("data/processed").mkdir(parents=True, exist_ok=True)

# Output file with ingestion timestamp
ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

dt = datetime.utcnow()
out_dir = Path(
    f"data/processed/date={dt.strftime('%Y-%m-%d')}/hour={dt.strftime('%H')}"
)
out_dir.mkdir(parents=True, exist_ok=True)

output_file = out_dir / "flights.parquet"


# Write Parquet file
pq.write_table(table, output_file)

print(f"\nSaved clean data to {output_file}")
