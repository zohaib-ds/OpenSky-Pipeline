from pathlib import Path
from datetime import datetime, timedelta
import os

RAW_DAYS = 7
PROCESSED_DAYS = 30

now = datetime.utcnow()

def cleanup(folder, days):
    cutoff = now - timedelta(days=days)
    for file in Path(folder).glob("*"):
        if file.is_file():
            mtime = datetime.utcfromtimestamp(file.stat().st_mtime)
            if mtime < cutoff:
                os.remove(file)

cleanup("data", RAW_DAYS)
cleanup("data/processed", PROCESSED_DAYS)
