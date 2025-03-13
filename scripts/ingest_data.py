import os
import pandas as pd
import logging
import time

# Configure logging
logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# File paths
CSV_FILE_PATH = "data/raw/customer_churn.csv"
OUTPUT_FOLDER = "data/processed/"
MAX_RETRIES = 3  # Retry up to 3 times before failing
RETRY_DELAY = 10  # Wait 10 seconds between retries

def fetch_data():
    """Simulates fetching new data periodically."""
    logging.info("✅ Fetching new data...")
    print("✅ Fetching new data...")
    return True

def ingest_data():
    """Reads CSV file and saves a backup in the processed folder with retries and error handling."""
    attempts = 0

    while attempts < MAX_RETRIES:
        try:
            if not os.path.exists(CSV_FILE_PATH):
                raise FileNotFoundError(f"❌ CSV file not found: {CSV_FILE_PATH}")

            df = pd.read_csv(CSV_FILE_PATH)
            record_count = len(df)
            logging.info(f"✅ Successfully read {record_count} records.")
            print(f"✅ Successfully read {record_count} records.")

            os.makedirs(OUTPUT_FOLDER, exist_ok=True)
            processed_file = os.path.join(OUTPUT_FOLDER, "customer_churn_backup.csv")
            df.to_csv(processed_file, index=False)
            logging.info(f"📂 File saved to {processed_file}")
            print(f"📂 File saved to {processed_file}")

            print("✅ Ingestion successful!")
            return

        except Exception as e:
            attempts += 1
            logging.error(f"⚠️ Ingestion attempt {attempts} failed: {e}")
            print(f"⚠️ Ingestion attempt {attempts} failed: {e}")
            time.sleep(RETRY_DELAY)

    logging.critical("❌ Ingestion failed after multiple attempts.")
    raise Exception("❌ Data ingestion failed after multiple attempts.")

if __name__ == "__main__":
    if fetch_data():
        ingest_data()
