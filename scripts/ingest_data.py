import os
import pandas as pd
import logging
import time
import subprocess
 
# Configure logging
logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
 
# File paths
CSV_FILE_PATH = "data/raw/customer_churn.csv"
KAGGLE_DATASET_PATH = "samridhi350/customer-churn"  # Update with your private Kaggle dataset path
KAGGLE_OUTPUT_FOLDER = "data/kaggle_downloads"
OUTPUT_FOLDER = "data/processed/"
MAX_RETRIES = 3  # Retry up to 3 times before failing
RETRY_DELAY = 10  # Wait 10 seconds between retries
 
def fetch_data():
    """Fetches data from primary and secondary sources (Kaggle)."""
    logging.info("✅ Fetching new data...")
    print("✅ Fetching new data...")
 
    # Download from Kaggle
    os.makedirs(KAGGLE_OUTPUT_FOLDER, exist_ok=True)
    kaggle_download_cmd = f"kaggle datasets download -d {KAGGLE_DATASET_PATH} -p {KAGGLE_OUTPUT_FOLDER} --unzip"
    try:
        subprocess.run(kaggle_download_cmd, shell=True, check=True)
        logging.info("✅ Successfully downloaded Kaggle dataset.")
        print("✅ Successfully downloaded Kaggle dataset.")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Kaggle dataset download failed: {e}")
        print(f"❌ Kaggle dataset download failed: {e}")
        return False
 
def ingest_data():
    """Reads CSV files from local and Kaggle sources and saves backups."""
    attempts = 0
 
    while attempts < MAX_RETRIES:
        try:
            # Check if the primary dataset exists
            if not os.path.exists(CSV_FILE_PATH):
                raise FileNotFoundError(f"❌ CSV file not found: {CSV_FILE_PATH}")
 
            # Read primary dataset
            df_main = pd.read_csv(CSV_FILE_PATH)
            record_count_main = len(df_main)
            logging.info(f"✅ Successfully read {record_count_main} records from primary source.")
            print(f"✅ Successfully read {record_count_main} records from primary source.")
 
            # Read Kaggle dataset
            kaggle_files = [f for f in os.listdir(KAGGLE_OUTPUT_FOLDER) if f.endswith(".csv")]
            if not kaggle_files:
                raise FileNotFoundError("❌ No CSV file found in Kaggle dataset.")
 
            kaggle_file_path = os.path.join(KAGGLE_OUTPUT_FOLDER, kaggle_files[0])
            df_kaggle = pd.read_csv(kaggle_file_path)
            record_count_kaggle = len(df_kaggle)
            logging.info(f"✅ Successfully read {record_count_kaggle} records from Kaggle dataset.")
            print(f"✅ Successfully read {record_count_kaggle} records from Kaggle dataset.")
 
            # Merge both datasets
            df_combined = pd.concat([df_main, df_kaggle], ignore_index=True)
            logging.info(f"✅ Combined dataset now has {len(df_combined)} records.")
            print(f"✅ Combined dataset now has {len(df_combined)} records.")
 
            # Save combined data
            os.makedirs(OUTPUT_FOLDER, exist_ok=True)
            processed_file = os.path.join(OUTPUT_FOLDER, "customer_churn_cleaned.csv")
            df_combined.to_csv(processed_file, index=False)
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