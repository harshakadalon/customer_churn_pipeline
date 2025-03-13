import os
import subprocess
from datetime import datetime

# ✅ Define Paths
PARQUET_DIR = "data/raw/parquet/"

def get_latest_parquet_file():
    """Finds the latest Parquet file in the newest timestamped folder."""
    subdirs = sorted(os.listdir(PARQUET_DIR), reverse=True)
    for subdir in subdirs:
        folder_path = os.path.join(PARQUET_DIR, subdir)
        if os.path.isdir(folder_path):
            parquet_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".parquet")], reverse=True)
            if parquet_files:
                return os.path.join("data/raw/parquet", subdir, parquet_files[0])  # Latest file
    raise FileNotFoundError("❌ No Parquet files found.")

if __name__ == "__main__":
    try:
        latest_parquet = get_latest_parquet_file()
        print(f"✅ Tracking latest dataset: {latest_parquet}")

        # ✅ Remove old tracking (if exists)
        subprocess.run(["dvc", "remove", "data/raw/parquet/*.dvc"], check=False)  # Ignore errors if no previous .dvc file

        # ✅ Add new dataset to DVC
        subprocess.run(["dvc", "add", latest_parquet], check=True)

        # ✅ Commit changes to Git
        subprocess.run(["git", "add", f"{latest_parquet}.dvc", ".gitignore"], check=True)
        subprocess.run(["git", "commit", "-m", f"Updated dataset version: {latest_parquet}"], check=True)

        print("✅ Raw dataset version tracked successfully.")
    
    except Exception as e:
        print(f"❌ Error tracking dataset: {e}")
