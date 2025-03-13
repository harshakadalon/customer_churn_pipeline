import os
import subprocess
import logging

# ✅ Configure logging
LOG_FILE = "logs/track_data.log"
os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Define the base data directory
DATA_DIR = "data/"

def get_all_subdirectories():
    """Finds all subdirectories under data/."""
    if not os.path.exists(DATA_DIR):
        logging.error(f"❌ Data directory {DATA_DIR} not found.")
        return []

    subdirs = []
    for root, dirs, _ in os.walk(DATA_DIR):
        for dir_name in dirs:
            folder_path = os.path.join(root, dir_name)
            logging.info(f"📂 Found dataset folder: {folder_path}")
            yield folder_path

def run_command(command):
    """Executes a shell command and handles errors."""
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        logging.info(f"✅ Command succeeded: {command}")
        print(result.stdout.strip())  # Display output
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Command failed: {command}\nError: {e.stderr}")
        print(f"❌ Error: {e.stderr}")
        return None

def track_all_data():
    """Tracks all dataset folders under 'data/' with DVC (no Git push)."""
    tracked = False
    for folder in get_all_subdirectories():
        run_command(f"dvc add {folder}")
        tracked = True

    if tracked:
        logging.info("🚀 All dataset folders tracked with DVC (no commit/push).")
        print("🚀 All dataset folders tracked with DVC (no commit/push).")
    else:
        logging.info("❌ No new dataset folders to track.")
        print("❌ No new dataset folders to track.")

if __name__ == "__main__":
    track_all_data()
