import requests
import json
from datetime import datetime
from pathlib import Path

# API endpoint
URL = "https://opensky-network.org/api/states/all"

# Create data folder if it doesn't exist
Path("data").mkdir(exist_ok=True)

# Call the API
response = requests.get(URL)
response.raise_for_status()  # fail fast if API breaks

data = response.json()

# Timestamp for ingestion time
ingestion_time = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

# Save raw JSON
file_path = f"data/opensky_raw_{ingestion_time}.json"
with open(file_path, "w") as f:
    json.dump(data, f)

print(f"Saved raw OpenSky data to {file_path}")
