import os
import subprocess
import logging
from datetime import datetime

# ✅ Set up logging
LOG_FILE = "logs/track_raw_data.log"
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ✅ Define dataset paths
RAW_DATA_DIR = "data/raw"
PARQUET_DIR = os.path.join(RAW_DATA_DIR, "parquet")
PROCESSED_DIR = "data/processed"
FEATURES_DIR = "data/features"
TRANSFORMED_DIR = "data/transformed"

def run_command(command):
    """Executes a shell command and logs output or errors."""
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        logging.info(f"✅ SUCCESS: {command}")
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ ERROR: {command}\n{e.stderr}")
        return None

def get_latest_dataset():
    """Finds the latest dataset folder inside data/raw/parquet/."""
    if not os.path.exists(PARQUET_DIR):
        logging.error(f"❌ Directory not found: {PARQUET_DIR}")
        return None

    folders = [f for f in os.listdir(PARQUET_DIR) if os.path.isdir(os.path.join(PARQUET_DIR, f))]
    if not folders:
        logging.error(f"❌ No timestamped dataset found in {PARQUET_DIR}.")
        return None

    latest_folder = max(folders)  # Assuming folders are named with timestamps (YYYY-MM-DD)
    latest_path = os.path.join(PARQUET_DIR, latest_folder)
    
    logging.info(f"✅ Latest dataset found: {latest_path}")
    return latest_path

def remove_git_tracking(path):
    """Removes a directory from Git tracking if it's already tracked."""
    if os.path.exists(path):
        git_ls = run_command(f"git ls-files --error-unmatch {path}")
        if git_ls is not None:
            run_command(f"git rm -r --cached {path}")
            run_command(f"git commit -m 'Stop tracking {path} in Git'")
            logging.info(f"✅ Removed {path} from Git tracking.")

def track_data_with_dvc():
    """Tracks dataset directories in DVC & commits changes to Git."""
    
    logging.info("🚀 Starting data tracking process...")

    # Get latest dataset version
    latest_dataset = get_latest_dataset()
    if latest_dataset:
        logging.info(f"📂 Tracking latest dataset in {latest_dataset}")
        run_command(f"dvc add {latest_dataset}")
        run_command(f"git add {latest_dataset}.dvc")
        run_command(f"git commit -m 'Tracked new dataset: {latest_dataset}'")

    # Remove `data/raw` and `data/processed` from Git tracking (if necessary)
    remove_git_tracking(RAW_DATA_DIR)
    remove_git_tracking(PROCESSED_DIR)

    # Track major dataset directories in DVC
    for folder in [RAW_DATA_DIR, PROCESSED_DIR, FEATURES_DIR, TRANSFORMED_DIR]:
        if os.path.exists(folder):
            run_command(f"dvc add {folder}")
            run_command(f"git add {folder}.dvc")
            run_command(f"git commit -m 'Tracked {folder} with DVC'")

    # Push changes to Git & DVC
    run_command("git push origin main")
    run_command("dvc push")
    logging.info("🚀 All dataset versions tracked & pushed successfully.")
    print("🚀 All dataset versions tracked & pushed successfully.")

if __name__ == "__main__":
    track_data_with_dvc()
