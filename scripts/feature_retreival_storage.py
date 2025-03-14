import pyodbc
import pandas as pd
import os
import datetime

# ✅ Define Paths
FEATURE_DIR = "data/features/"
os.makedirs(FEATURE_DIR, exist_ok=True)

# ✅ Database Connection Function
def get_db_connection():
    """Establishes a connection to SQL Server."""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=192.168.29.40;'
            'DATABASE=PG;'
            'UID=bits;'
            'PWD=mnblkj147'
        )
        return conn
    except Exception as e:
        print(f"❌ Database Connection Error: {str(e)}")
        return None

# ✅ Fetch All Features
def fetch_all_features():
    """Retrieves all features from FeatureStore for model training."""
    conn = get_db_connection()
    if not conn:
        return None

    query = """
    SELECT 
      [gender]
      ,[SeniorCitizen]
      ,[Partner]
      ,[Dependents]
      ,[tenure]
      ,[PhoneService]
      ,[Churn]
      ,[MultipleLines_No phone service]
      ,[MultipleLines_Yes]
      ,[InternetService_Fiber optic]
      ,[InternetService_No]
      ,[OnlineSecurity_No internet service]
      ,[OnlineSecurity_Yes]
      ,[OnlineBackup_No internet service]
      ,[OnlineBackup_Yes]
      ,[last_purchase_recency]
      ,[engagement_score]
      ,[total_services_used]
      ,[high_support_calls]
    FROM FeatureStore ORDER BY CreatedAt DESC;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ✅ Store Features in Parquet
def store_features(df):
    """Stores retrieved features as a Parquet file with versioning."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    feature_file_path = os.path.join(FEATURE_DIR, f"customer_churn_features_{timestamp}.parquet")

    df.to_parquet(feature_file_path, index=False)
    print(f"✅ Features stored at: {feature_file_path}")
    return feature_file_path

if __name__ == "__main__":
    df_all = fetch_all_features()
    if df_all is not None:
        print("✅ Retrieved All Features for Model Training:")
        print(df_all.head())  # Print first 5 rows

        # ✅ Store Retrieved Features
        stored_path = store_features(df_all)
        print(f"📂 Features saved as Parquet: {stored_path}")
