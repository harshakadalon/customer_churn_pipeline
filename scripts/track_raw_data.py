import os
import subprocess
import logging

# ✅ Configure logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "dvc_tracking.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ✅ Define Base Paths
RAW_DATA_DIR = "data/raw/parquet/"
TRANSFORMED_DATA_DIR = "data/transformed/"

def get_latest_parquet_folder(base_dir):
    """Finds the latest timestamped folder inside base_dir."""
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"❌ Directory not found: {base_dir}")

    subdirs = sorted(os.listdir(base_dir), reverse=True)
    for subdir in subdirs:
        folder_path = os.path.join(base_dir, subdir)
        if os.path.isdir(folder_path):
            return folder_path  # Return the latest timestamped folder

    raise FileNotFoundError(f"❌ No timestamped folders found in {base_dir}")

def track_data():
    """Tracks changes in raw and transformed datasets using DVC."""
    try:
        # Get latest parquet folders
        latest_raw_folder = get_latest_parquet_folder(RAW_DATA_DIR)
        latest_transformed_folder = get_latest_parquet_folder(TRANSFORMED_DATA_DIR)

        # ✅ Track latest Parquet files in their respective folders
        subprocess.run(["dvc", "add", latest_raw_folder], check=True)
        subprocess.run(["dvc", "add", latest_transformed_folder], check=True)

        # ✅ Add the DVC-tracked files to Git
        subprocess.run(["git", "add", f"{latest_raw_folder}.dvc"], check=True)
        subprocess.run(["git", "add", f"{latest_transformed_folder}.dvc"], check=True)

        # ✅ Commit changes
        subprocess.run(["git", "commit", "-m", "Updated dataset version"], check=True)
        subprocess.run(["dvc", "push"], check=True)  # Push data to remote storage
        subprocess.run(["git", "push"], check=True)  # Push metadata to GitHub

        logging.info("✅ Successfully tracked and pushed dataset changes using DVC.")
        print("✅ Successfully tracked and pushed dataset changes using DVC.")

    except subprocess.CalledProcessError as e:
        logging.error(f"❌ DVC tracking failed: {str(e)}")
        print(f"❌ DVC tracking failed: {str(e)}")

if __name__ == "__main__":
    track_data()
