import pandas as pd
import os

# File paths
INPUT_FILE = "data/processed/customer_churn_backup.csv"
OUTPUT_FILE = "data/processed/customer_churn_cleaned.csv"

def process_data():
    """Loads, cleans, and processes the data."""
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"❌ Processed file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Data processed and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_data()

