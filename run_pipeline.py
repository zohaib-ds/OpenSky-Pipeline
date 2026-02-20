import subprocess
import logging
from pathlib import Path
from datetime import datetime

# Create logs folder
Path("logs").mkdir(exist_ok=True)

# Log file with date
log_file = f"logs/pipeline_{datetime.utcnow().strftime('%Y-%m-%d')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.info("Pipeline started")

try:
    subprocess.run(["python", "ingest.py"], check=True)
    logging.info("Ingestion completed successfully")

    subprocess.run(["python", "extract.py"], check=True)
    logging.info("Extraction and Parquet write completed successfully")

    subprocess.run(["python", "retention.py"], check=True)
    logging.info("Retention cleanup completed")

    logging.info("Pipeline finished successfully")

except Exception as e:
    logging.exception("Pipeline failed")
    raise
