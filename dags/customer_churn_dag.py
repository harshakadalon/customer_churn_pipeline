from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

# Default arguments for DAG execution
default_args = {
    'owner': 'harsha',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 7),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_script(script_path):
    """Runs a Python script and logs its output in Airflow."""
    try:
        result = subprocess.run(["python3", script_path], capture_output=True, text=True, check=True)
        logging.info(result.stdout)  # ✅ Capture & log output
        print(result.stdout)  # ✅ Ensure Airflow UI displays output
        return result.stdout  
    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
        print(e.stderr)
        raise

# Define the DAG
with DAG(
    'customer_churn_pipeline',
    default_args=default_args,
    description='Automated data ingestion, processing, storage, validation, and preparation pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Ingest Data
    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=run_script,
        op_args=['/home/harsha/customer_churn_pipeline/scripts/ingest_data.py']
    )

    # Task 2: Process Data
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=run_script,
        op_args=['/home/harsha/customer_churn_pipeline/scripts/process_data.py']
    )

    # Task 3: Store Data as Parquet
    store_parquet_task = PythonOperator(
        task_id='store_parquet',
        python_callable=run_script,
        op_args=['/home/harsha/customer_churn_pipeline/scripts/store_parquet.py']
    )

    # Task 4: Validate Parquet Data
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=run_script,
        op_args=['/home/harsha/customer_churn_pipeline/scripts/data_validation.py']
    )

    # Task 5: Prepare Data
    prepare_data_task = PythonOperator(
        task_id='prepare_data',
        python_callable=run_script,
        op_args=['/home/harsha/customer_churn_pipeline/scripts/data_preparation.py']
    )

    # Define Task Order (Dependency Flow)
    ingest_task >> process_task >> store_parquet_task >> validate_task >> prepare_data_task
