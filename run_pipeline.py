import subprocess

print("🚀 Starting data pipeline...")

subprocess.run(["python3", "scripts/ingest_data.py"], check=True)
subprocess.run(["python3", "scripts/process_data.py"], check=True)

print("✅ Data pipeline completed successfully!")
