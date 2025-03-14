import pandas as pd
import numpy as np
import pyodbc
import os
import logging
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# ✅ Configure logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "transformation.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ✅ Define Paths
PARQUET_DIR = "data/processed/parquet/"
TRANSFORMED_DIR = "data/transformed/"

def get_latest_prepared_parquet():
    """Finds the latest `customer_churn_prepared.parquet` file in the newest timestamped folder."""
    subdirs = sorted(os.listdir(PARQUET_DIR), reverse=True)  # Get latest folder first
    for subdir in subdirs:
        folder_path = os.path.join(PARQUET_DIR, subdir)
        if os.path.isdir(folder_path):
            prepared_file_path = os.path.join(folder_path, "customer_churn_prepared.parquet")
            if os.path.exists(prepared_file_path):
                return prepared_file_path
    raise FileNotFoundError("❌ No prepared Parquet files found.")

def load_data():
    """Loads the prepared Parquet file."""
    latest_prepared_file = get_latest_prepared_parquet()
    logging.info(f"✅ Loading prepared Parquet file: {latest_prepared_file}")
    print(f"✅ Loading prepared Parquet file: {latest_prepared_file}")
    df = pd.read_parquet(latest_prepared_file)
    return df

def transform_data(df):
    """Feature Engineering & Final Transformations."""

    # ✅ 1️⃣ Recency Score (More recent = Higher score)
    df["last_purchase_recency"] = df["tenure"].apply(lambda x: 1 / (x + 1))

    # ✅ 2️⃣ Engagement Score (Sum of subscribed services → Already encoded as 0/1)
    service_cols = [col for col in df.columns if "OnlineSecurity" in col or "OnlineBackup" in col or "PhoneService" in col or "MultipleLines" in col]
    if service_cols:

        df["engagement_score"] = df[service_cols].sum(axis=1)  # Sum of binary 1s
        df["total_services_used"] = df[service_cols].sum(axis=1)  # Same logic

    # ✅ 3️⃣ Spending Features
   # df["total_spend"] = df["tenure"] * 50  # Assume avg spend of $50/month
   # df["customer_tenure_years"] = df["tenure"] / 12  # Convert months to years  

    # ✅ 4️⃣ High Support Calls (Flagging risky customers)
    if "OnlineSecurity_Yes" in df.columns:
        df["high_support_calls"] = np.where(df["OnlineSecurity_Yes"] == 0, 1, 0)

    # ✅ Scaling Numerical Features
    numerical_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    #scaler = StandardScaler()
    #df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

    # ✅ Normalization (Min-Max Scaling)
    minmax_scaler = MinMaxScaler()
    df[numerical_cols] = minmax_scaler.fit_transform(df[numerical_cols])

    logging.info("✅ Data Transformation Completed Successfully.")
    print("✅ Data Transformation Completed Successfully.")
    
    return df

def save_transformed_data(df):
    """Saves transformed dataset."""
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    latest_folder = sorted(os.listdir(PARQUET_DIR), reverse=True)[0]
    transformed_file_path = os.path.join(TRANSFORMED_DIR, f"{latest_folder}_transformed.parquet")

    df.to_parquet(transformed_file_path, index=False)
    logging.info(f"📂 Transformed Data Saved: {transformed_file_path}")
    print(f"✅ Transformed Data Saved at: {transformed_file_path}")

def store_in_sql(df):
    """Stores transformed data into SQL Server with table recreation & truncation."""
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.29.40;'
        'DATABASE=PG;'
        'UID=bits;'
        'PWD=mnblkj147'
    )
    cursor = conn.cursor()
    
    table_name = "CustomerChurnTransformed"

    # ✅ Drop table if exists
    drop_table_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};"
    cursor.execute(drop_table_sql)
    conn.commit()

    # ✅ Create Table Dynamically Based on DataFrame Columns
    column_definitions = []
    for col in df.columns:
        if df[col].dtype == "int64":
            col_type = "INT"
        elif df[col].dtype == "float64":
            col_type = "FLOAT"
        else:
            col_type = "NVARCHAR(255)"  # Default to string
        column_definitions.append(f"[{col}] {col_type}")

    create_table_sql = f"""
    CREATE TABLE {table_name} (
        {', '.join(column_definitions)}
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # ✅ Insert Data Dynamically
    column_names = ", ".join([f"[{col}]" for col in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    conn.close()
    logging.info("✅ Data successfully stored in SQL Server.")
    print("✅ Data successfully stored in SQL Server.")

if __name__ == "__main__":
    df = load_data()
    df_transformed = transform_data(df)
    save_transformed_data(df_transformed)
    store_in_sql(df_transformed)
