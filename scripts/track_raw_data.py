import os
import subprocess

# ✅ Define Paths
DVC_REMOTE = "myremote"  # Ensure this matches your DVC remote name
DATASET_DIR = "data/raw/parquet/"  # Updated path to match your storage structure
DVC_STORAGE_PATH = "/mnt/data/dvc-storage"  # Update if different

# ✅ Ensure necessary directories exist
os.makedirs(DATASET_DIR, exist_ok=True)

# ✅ Function to track raw data
def track_data():
    """Adds the latest dataset from data/raw/parquet/ to DVC and Git."""
    
    print(f"🔍 Checking for new dataset versions in {DATASET_DIR}...")
    
    # Get the latest dataset folder
    subdirs = sorted(os.listdir(DATASET_DIR), reverse=True)
    if not subdirs:
        print("❌ No dataset folders found in 'data/raw/parquet/'. Exiting...")
        return

    latest_folder = os.path.join(DATASET_DIR, subdirs[0])  # Latest timestamped folder
    print(f"✅ Found latest dataset version: {latest_folder}")

    # Check if the latest dataset is already tracked
    dvc_file = f"{latest_folder}.dvc"
    if os.path.exists(dvc_file):
        print(f"🔄 Latest dataset version '{latest_folder}' is already tracked by DVC.")
    else:
        print(f"📂 Adding {latest_folder} to DVC...")
        os.system(f"dvc add {latest_folder}")
        os.system(f"git add {latest_folder}.dvc .gitignore")
        os.system("git commit -m 'Track latest dataset version'")

    print("🚀 Committing and pushing latest version to Git and DVC remote storage...")
    os.system("dvc commit")
    os.system("git commit -m 'Updated dataset version'")
    os.system("git push origin main")
    os.system("dvc push")

    print("✅ Dataset versioning completed successfully.")

if __name__ == "__main__":
    track_data()
