import pandas as pd
import numpy as np
import os
import pickle
import mlflow
import mlflow.sklearn
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from mlflow.models import infer_signature
 
# Define Paths
FEATURES_DIR = "data/features/"
MODELS_DIR = "models/"
REPORTS_DIR = "reports/"
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
 
# Load Latest Feature Data
def get_latest_feature_file():
    """Finds the latest feature dataset."""
    feature_files = sorted(os.listdir(FEATURES_DIR), reverse=True)
    for file in feature_files:
        if file.endswith(".parquet"):
            return os.path.join(FEATURES_DIR, file)
    raise FileNotFoundError("No feature files found.")
 
def load_features():
    """Loads the latest feature file."""
    latest_feature_file = get_latest_feature_file()
    print(f"Loading latest feature file: {latest_feature_file}")
    return pd.read_parquet(latest_feature_file)
 
# Train & Evaluate Model
def train_model(df):
    """Trains different models and logs results in MLflow."""
 
    # Define Target & Features
    X = df.drop(columns=["Churn"])
    y = df["Churn"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
 
    # Define models to train
    models = {
        "Logistic Regression": LogisticRegression(),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42)
    }
 
    for model_name, model in models.items():
        with mlflow.start_run():  # Start a new MLflow run for each model
            print(f"Training Model: {model_name}...")
 
            # Train model
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
 
            # Compute evaluation metrics
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)
 
            # Prepare input example & signature
            input_example = X_test.iloc[:1].to_dict(orient="records")  # Single row as example
            signature = infer_signature(X_test, y_pred)
 
            # Log metrics & parameters
            mlflow.log_param("Model", model_name)
            mlflow.log_metrics({
                "Accuracy": accuracy,
                "Precision": precision,
                "Recall": recall,
                "F1 Score": f1
            })
 
            # Define fixed model filename (overwrite existing file)
            model_filename = f"{MODELS_DIR}/{model_name.lower().replace(' ', '_')}.pkl"
            with open(model_filename, "wb") as f:
                pickle.dump(model, f)
 
            # Log Model with Signature & Input Example
            mlflow.sklearn.log_model(
                model,
                model_name.lower().replace(" ", "_"),
                signature=signature,
                input_example=input_example
            )
 
            print(f"{model_name} - Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}, F1 Score: {f1:.4f}")
            print(f"Model saved : {model_filename}")
 
            # Define fixed report filename (overwrite existing file)
            report_filename = f"{REPORTS_DIR}/{model_name.lower().replace(' ', '_')}.txt"
            with open(report_filename, "w") as report_file:  # "w" mode replaces old content
                report_file.write(f"Model: {model_name}\n")
                report_file.write(f"Accuracy: {accuracy:.4f}\n")
                report_file.write(f"Precision: {precision:.4f}\n")
                report_file.write(f"Recall: {recall:.4f}\n")
                report_file.write(f"F1 Score: {f1:.4f}\n")
 
            print(f"Report saved: {report_filename}")
            print(f"{model_name} - Modeling Completed")
 
if __name__ == "__main__":
    df_features = load_features()
    train_model(df_features)
 