import os
import pandas as pd
import logging
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns
from imblearn.over_sampling import SMOTE
 
# ✅ Configure logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "preparation.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
 
# ✅ Define Paths
PARQUET_DIR = "data/processed/parquet/"
 
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
    # Normalize Churn Value
    df["Churn"] = df["Churn"].replace({"Yes": 1, "No": 0}).astype(int)
    # Drop the "customerID" column if present
    if "customerID" in df.columns.tolist():
        df.drop(columns=["customerID"], inplace=True)
    # ✅ Handling Missing Values Using Recommended Pandas Method
    df.fillna(df.mode().iloc[0], inplace=True)  # Use mode for categorical
    df.fillna(df.median(numeric_only=True), inplace=True)  # Use median for numerical
    # ✅ Standardization (Z-score Scaling)
    numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    numerical_cols = [col for col in numerical_cols if col != "Churn"]  # Exclude target variable
    # ✅ Label Encoding for Binary Categorical Columns
    binary_categorical_cols = ["gender", "Partner", "Dependents", "PhoneService"]
    label_encoder = LabelEncoder()
    for col in binary_categorical_cols:
        if col in df.columns:
            df[col] = label_encoder.fit_transform(df[col])
    # ✅ One-Hot Encoding for Multi-Category Columns
    multi_category_cols = ["MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup"]
    existing_categorical_columns = [col for col in multi_category_cols if col in df.columns]
    if existing_categorical_columns:
        df = pd.get_dummies(df, columns=existing_categorical_columns, drop_first=True)
    # ✅ Normalization (Min-Max Scaling)
    if numerical_cols:
        minmax_scaler = MinMaxScaler()
        df[numerical_cols] = minmax_scaler.fit_transform(df[numerical_cols])

    #✅ Apply SMOTE for Imbalanced Data
    smote = SMOTE(random_state=42)
    X, y = df.drop(columns=["Churn"]), df["Churn"]
    X_resampled, y_resampled = smote.fit_resample(X, y)
    df = pd.concat([pd.DataFrame(X_resampled, columns=X.columns), pd.DataFrame(y_resampled, columns=["Churn"])], axis=1)
    
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
 
def generate_visualizations(df):
    """Generates meaningful visualizations for customer churn analysis."""
    VISUAL_DIR = "visualizations"
    os.makedirs(VISUAL_DIR, exist_ok=True)
    # ✅ Churn Distribution
    plt.figure(figsize=(6, 4))
    sns.countplot(x=df['Churn'])
    plt.title("Churn Distribution")
    plt.savefig(os.path.join(VISUAL_DIR, "churn_distribution.png"))
    plt.close()

    # ✅ Tenure Distribution by Churn
    plt.figure(figsize=(8, 5))
    sns.boxplot(x='Churn', y='tenure', data=df)
    plt.title("Tenure Distribution by Churn")
    plt.savefig(os.path.join(VISUAL_DIR, "tenure_distribution.png"))
    plt.close()
    # ✅ Correlation Heatmap (Numerical Features)
    numeric_df = df.select_dtypes(include=['number'])
    plt.figure(figsize=(10, 6))
    sns.heatmap(numeric_df.corr(), annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Feature Correlation Heatmap")
    plt.savefig(os.path.join(VISUAL_DIR, "correlation_heatmap.png"))
    plt.close()
    print("✅ Visualizations generated and saved to", VISUAL_DIR)
    logging.info("✅ Visualizations generated successfully.")
 
if __name__ == "__main__":
    df = load_data()
    df_prepared = prepare_data(df)
    save_prepared_data(df_prepared)
    generate_visualizations(df_prepared)