import os
import subprocess

# ✅ Define Paths
TRANSFORMED_DIR = "data/transformed/"

def get_latest_transformed_file():
    """Finds the latest transformed Parquet file."""
    transformed_files = sorted(os.listdir(TRANSFORMED_DIR), reverse=True)
    for file in transformed_files:
        if file.endswith(".parquet"):
            return os.path.join(TRANSFORMED_DIR, file)
    raise FileNotFoundError("❌ No transformed Parquet files found.")

if __name__ == "__main__":
    try:
        latest_transformed = get_latest_transformed_file()
        print(f"✅ Tracking latest transformed dataset: {latest_transformed}")

        # ✅ Remove old tracking (if exists)
        subprocess.run(["dvc", "remove", "data/transformed/*.dvc"], check=False)

        # ✅ Add transformed dataset to DVC
        subprocess.run(["dvc", "add", latest_transformed], check=True)

        # ✅ Commit changes to Git
        subprocess.run(["git", "add", f"{latest_transformed}.dvc", ".gitignore"], check=True)
        subprocess.run(["git", "commit", "-m", f"Updated transformed dataset version: {latest_transformed}"], check=True)

        print("✅ Transformed dataset version tracked successfully.")

    except Exception as e:
        print(f"❌ Error tracking transformed dataset: {e}")
