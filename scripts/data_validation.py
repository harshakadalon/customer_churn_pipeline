import os
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename="logs/validation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Define paths
PARQUET_DIR = "data/raw/parquet/"
REPORT_PATH = "reports/data_quality_report.csv"

def get_latest_parquet():
    """Finds the latest Parquet file in the directory."""
    if not os.path.exists(PARQUET_DIR):
        raise FileNotFoundError(f"❌ Parquet directory not found: {PARQUET_DIR}")

    # Get all timestamped subdirectories (sorted latest first)
    subdirs = sorted(os.listdir(PARQUET_DIR), reverse=True)
    for subdir in subdirs:
        folder_path = os.path.join(PARQUET_DIR, subdir)
        if os.path.isdir(folder_path):
            parquet_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".parquet")], reverse=True)
            if parquet_files:
                return os.path.join(folder_path, parquet_files[0])

    raise FileNotFoundError("❌ No Parquet files found in any timestamped folder.")

def load_data():
    """Loads the latest Parquet file for validation."""
    latest_parquet_file = get_latest_parquet()
    logging.info(f"✅ Loading latest Parquet file: {latest_parquet_file}")
    print(f"✅ Loading latest Parquet file: {latest_parquet_file}")

    df = pd.read_parquet(latest_parquet_file)
    return df

def check_missing_values(df):
    """Checks for missing values in the dataset."""
    missing = df.isnull().sum()
    return missing[missing > 0]  # Only return columns with missing values

def check_data_types(df):
    """Checks for incorrect data types."""
    expected_types = {
        "CustomerID": "int64",
        "Churn": "object",
        "Tenure": "int64",
        "MonthlyCharges": "float64",
        "TotalCharges": "float64",
    }
    
    actual_types = df.dtypes
    type_issues = {col: (actual_types[col], expected_types[col]) 
                   for col in expected_types if col in df.columns and actual_types[col] != expected_types[col]}
    return type_issues

def check_duplicates(df):
    """Identifies duplicate records."""
    return df.duplicated().sum()

def generate_quality_report(df):
    """Generates a CSV report summarizing data validation issues."""
    os.makedirs("reports", exist_ok=True)

    missing_values = check_missing_values(df)
    type_issues = check_data_types(df)
    duplicates = check_duplicates(df)

    report_data = {
        "Metric": ["Missing Values", "Data Type Issues", "Duplicate Records"],
        "Count": [missing_values.sum() if not missing_values.empty else 0,
                  len(type_issues),
                  duplicates]
    }

    report_df = pd.DataFrame(report_data)
    report_df.to_csv(REPORT_PATH, index=False)

    logging.info(f"📊 Data Quality Report saved: {REPORT_PATH}")
    print(f"✅ Data Quality Report saved at: {REPORT_PATH}")

if __name__ == "__main__":
    df = load_data()
    generate_quality_report(df)
