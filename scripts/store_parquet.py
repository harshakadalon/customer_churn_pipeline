import os
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename="logs/storage.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Define storage paths
RAW_CSV_PATH = "data/processed/customer_churn_cleaned.csv"  # Input CSV file
BASE_DIR = "data/raw/parquet/"  # Parquet storage location
TIMESTAMP_DIR = f"{BASE_DIR}{datetime.now().strftime('%Y-%m-%d')}/"  # Date-based partition
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
OUTPUT_FILE = f"{TIMESTAMP_DIR}customer_churn_{timestamp}.parquet"

def create_directories():
    """Ensure necessary directories exist."""
    os.makedirs(TIMESTAMP_DIR, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

def convert_to_parquet():
    """Reads the ingested CSV file and saves it as a Parquet file."""
    create_directories()

    # Check if the CSV file exists
    if not os.path.exists(RAW_CSV_PATH):
        logging.error(f"❌ CSV file not found: {RAW_CSV_PATH}")
        raise FileNotFoundError(f"❌ CSV file not found: {RAW_CSV_PATH}")

    # Read CSV and convert to Parquet
    try:
        df = pd.read_csv(RAW_CSV_PATH)
        logging.info(f"✅ Successfully read {len(df)} records from {RAW_CSV_PATH}")
        df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
        logging.info(f"📂 File successfully stored as Parquet: {OUTPUT_FILE}")
        print(f"✅ Data stored as Parquet at: {OUTPUT_FILE}")

    except Exception as e:
        logging.error(f"❌ Error converting to Parquet: {e}")
        print(f"❌ Error converting to Parquet: {e}")
        raise

if __name__ == "__main__":
    convert_to_parquet()
