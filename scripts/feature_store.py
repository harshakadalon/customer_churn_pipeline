import pandas as pd
import pyodbc
import os
import logging
from datetime import datetime
 
# ✅ Configure Logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "feature_store.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
 
# ✅ Define Paths
PARQUET_DIR = "data/transformed/"
DB_SERVER = "192.168.29.40"
DB_NAME = "PG"
DB_USER = "bits"
DB_PASSWORD = "mnblkj147"
 
def get_db_connection():
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_NAME};'
        f'UID={DB_USER};'
        f'PWD={DB_PASSWORD}'
    )
    return conn
 
def load_transformed_data():
    """Loads the latest transformed Parquet file."""
    transformed_files = sorted(os.listdir(PARQUET_DIR), reverse=True)
    if not transformed_files:
        raise FileNotFoundError("❌ No transformed Parquet files found!")
 
    latest_transformed_file = os.path.join(PARQUET_DIR, transformed_files[0])
    logging.info(f"✅ Loading transformed data from: {latest_transformed_file}")
    print(f"✅ Loading transformed data from: {latest_transformed_file}")
 
    df = pd.read_parquet(latest_transformed_file)
    return df
 
def create_feature_store_tables(df):
    """Creates Feature Store Table and Metadata Table in SQL Server."""
    conn = get_db_connection()
    cursor = conn.cursor()
 
    # Drop old tables if exist (for fresh inserts)
    cursor.execute("IF OBJECT_ID('FeatureStore', 'U') IS NOT NULL DROP TABLE FeatureStore;")
    cursor.execute("IF OBJECT_ID('FeatureMetadata', 'U') IS NOT NULL DROP TABLE FeatureMetadata;")
    conn.commit()
 
    # Generate Feature Store Table Schema
    column_definitions = [f"[{col}] FLOAT" if df[col].dtype in ["float64", "int64"] else f"[{col}] NVARCHAR(255)" for col in df.columns]
    create_table_sql = f"""
    CREATE TABLE FeatureStore (
        FeatureID INT IDENTITY(1,1) PRIMARY KEY,
        {', '.join(column_definitions)},
        CreatedAt DATETIME DEFAULT GETDATE(),
        Version INT
    );
    """
    # Create Feature Metadata Table
    create_metadata_sql = """
    CREATE TABLE FeatureMetadata (
        FeatureName NVARCHAR(255) PRIMARY KEY,
        Description NVARCHAR(1000),
        Source NVARCHAR(255),
        Version INT DEFAULT 1,
        CreatedAt DATETIME DEFAULT GETDATE()
    );
    """
    cursor.execute(create_table_sql)
    cursor.execute(create_metadata_sql)
    conn.commit()
    conn.close()
 
    logging.info("✅ Feature Store & Metadata Tables Created Successfully.")
    print("✅ Feature Store & Metadata Tables Created Successfully.")
 
def store_features(df):
    """Stores all transformed features dynamically in SQL Server."""
    conn = get_db_connection()
    cursor = conn.cursor()
 
    # Insert Data into FeatureStore
    column_names = ", ".join([f"[{col}]" for col in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql = f"INSERT INTO FeatureStore ({column_names}, Version) VALUES ({placeholders}, ?)"
 
    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row) + (1,))
 
    conn.commit()
    conn.close()
 
    logging.info("✅ Features successfully stored in SQL Server.")
    print("✅ Features successfully stored in SQL Server.")
 
def store_feature_metadata(df):
    """Stores metadata for each feature with specific descriptions."""
    conn = get_db_connection()
    cursor = conn.cursor()
 
    feature_descriptions = {
        "Churn": "Indicates whether the customer churned (1) or not (0).",
        "Dependents": "Indicates if the customer has dependents (1) or not (0).",
        "engagement_score": "Numerical score representing customer engagement.",
        "gender": "Gender of the customer (0 for Female, 1 for Male).",
        "high_support_calls": "Flag if the customer made excessive support calls.",
        "InternetService_Fiber optic": "Indicates if the customer has fiber optic internet.",
        "InternetService_No": "Indicates if the customer has no internet service.",
        "last_purchase_recency": "Days since the last purchase by the customer.",
        "MultipleLines_No phone service": "Indicates if the customer has no phone service.",
        "MultipleLines_Yes": "Indicates if the customer has multiple lines.",
        "OnlineBackup_No internet service": "Indicates if the customer has no internet and no backup service.",
        "OnlineBackup_Yes": "Indicates if the customer has an online backup service.",
        "OnlineSecurity_No internet service": "Indicates if the customer has no internet and no security service.",
        "OnlineSecurity_Yes": "Indicates if the customer has an online security service.",
        "Partner": "Indicates if the customer has a partner (1) or not (0).",
        "PhoneService": "Indicates if the customer has a phone service (1) or not (0).",
        "SeniorCitizen": "Indicates if the customer is a senior citizen (1) or not (0).",
        "tenure": "Number of months the customer has stayed with the company.",
        "total_services_used": "Count of total services used by the customer."
    }
 
    for col in df.columns:
        description = feature_descriptions.get(col, "No description available.")
        cursor.execute("""
            INSERT INTO FeatureMetadata (FeatureName, Description, Source, Version)
            VALUES (?, ?, ?, ?)
        """, (col, description, "Data Transformation Pipeline", 1))
 
    conn.commit()
    conn.close()
 
    logging.info("✅ Feature metadata stored successfully.")
    print("✅ Feature metadata stored successfully.")
 
if __name__ == "__main__":
    df_transformed = load_transformed_data()
    create_feature_store_tables(df_transformed)
    store_features(df_transformed)
    store_feature_metadata(df_transformed)