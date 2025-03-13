import os
import pandas as pd
import logging
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# ✅ Configure logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "preparation.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ✅ Define Paths
PARQUET_DIR = "data/raw/parquet/"

def get_latest_parquet():
    """Finds the latest Parquet file."""
    if not os.path.exists(PARQUET_DIR):
        raise FileNotFoundError(f"❌ Parquet directory not found: {PARQUET_DIR}")

    subdirs = sorted(os.listdir(PARQUET_DIR), reverse=True)
    for subdir in subdirs:
        folder_path = os.path.join(PARQUET_DIR, subdir)
        if os.path.isdir(folder_path):
            parquet_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".parquet")], reverse=True)
            if parquet_files:
                return os.path.join(folder_path, parquet_files[0])

    raise FileNotFoundError("❌ No Parquet files found in any timestamped folder.")

def load_data():
    """Loads the latest Parquet file."""
    latest_parquet_file = get_latest_parquet()
    logging.info(f"✅ Loading latest Parquet file: {latest_parquet_file}")
    print(f"✅ Loading latest Parquet file: {latest_parquet_file}")

    df = pd.read_parquet(latest_parquet_file)
    return df

def prepare_data(df):
    """Prepares data by handling missing values, encoding, and scaling."""

    # ✅ Handling Missing Values
    for col in df.columns:
        if df[col].dtype == "object":
            df.fillna({col: df[col].mode()[0]}, inplace=True)  # Use mode for categorical
        else:
            df.fillna({col: df[col].median()}, inplace=True)  # Use median for numerical

    #✅ Standardization (Z-score Scaling)
    numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    numerical_cols = [col for col in numerical_cols if col != "Churn"]  # Exclude target variable

    #scaler = StandardScaler()
    #df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

    # ✅ Encoding Categorical Variables (Check Existence)
    categorical_columns = ["gender", "Partner", "Dependents", "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup"]
    existing_categorical_columns = [col for col in categorical_columns if col in df.columns]

    df = pd.get_dummies(df, columns=existing_categorical_columns, drop_first=True)

    # ✅ Normalization (Min-Max Scaling)
    minmax_scaler = MinMaxScaler()
    df[numerical_cols] = minmax_scaler.fit_transform(df[numerical_cols])

    logging.info("✅ Data Preparation Completed Successfully.")
    print("✅ Data Preparation Completed Successfully.")
    
    return df

def save_prepared_data(df):
    """Saves the prepared dataset back to the Parquet folder."""
    os.makedirs(PARQUET_DIR, exist_ok=True)

    latest_folder = sorted(os.listdir(PARQUET_DIR), reverse=True)[0]
    prepared_file_path = os.path.join(PARQUET_DIR, latest_folder, "customer_churn_prepared.parquet")

    df.to_parquet(prepared_file_path, index=False)
    logging.info(f"📂 Prepared Data Saved: {prepared_file_path}")
    print(f"✅ Prepared Data Saved at: {prepared_file_path}")

if __name__ == "__main__":
    df = load_data()
    df_prepared = prepare_data(df)
    save_prepared_data(df_prepared)
