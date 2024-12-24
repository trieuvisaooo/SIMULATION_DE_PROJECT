import io
import requests
import pandas as pd
from pywebhdfs.webhdfs import PyWebHdfsClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Azure AD & Power BI Configuration
CLIENT_ID = "89301c43-9292-4cc0-a894-80d08a728ef1"
CLIENT_SECRET = "auE8Q~qNWEzfH9q-elbE28rhu2qsvsRhMfMd8csM"
TENANT_ID = "40127cd4-45f3-49a3-b05d-315a43a9f033"
SCOPE = "https://analysis.windows.net/powerbi/api/.default"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GROUP_ID = "34bc619a-fc6f-41ea-9682-b19deac7ae9d"
DATASET_ID = "e07ffdcc-34d7-47b2-bffc-d6f33f84cb0a"

# HDFS Configuration
HDFS_HOST = "LAPTOP-QHS1R0BJ.mshome.net"
HDFS_PORT = "9870"
HDFS_USER = "hadoop"
HDFS_PATH = "/user/spark/transactions_csv/"

# Define default arguments for DAG
DEFAULT_ARGS = {
    "OWNER": "airflow",
    "DEPENDS_ON_PAST": False,
    "EMAIL_ON_FAILURE": False,
    "EMAIL_ON_RETRY": False,
    "RETRIES": 1,
    "RETRY_DELAY": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "hdfs_to_powerbi",
    default_args=DEFAULT_ARGS,
    description="Automate data pipeline from Hadoop to Power BI",
    schedule_interval="0 0 * * *",  # Daily at midnight
    start_date=datetime(2023, 12, 24),
    catchup=False,
)

# Task 1: Check for new files in HDFS
def check_hdfs_files():
    logging.info(f"Connecting to WebHDFS at {HDFS_HOST}:{HDFS_PORT}")
    try:
        hdfs = PyWebHdfsClient(host=HDFS_HOST, port=HDFS_PORT, user_name=HDFS_USER, timeout=300)
        files = hdfs.list_dir(HDFS_PATH)["FileStatuses"]["FileStatus"]
        new_files = [file["pathSuffix"] for file in files if file["type"] == "FILE"]
        logging.info(f"Found files: {new_files}")
        return new_files
    except Exception as e:
        logging.error(f"Error checking HDFS files: {e}")
        return []

check_files_task = PythonOperator(
    task_id="check_hdfs_files",
    python_callable=check_hdfs_files,
    dag=dag,
)

# Task 2: Sync data to Power BI
def sync_to_powerbi(**kwargs):
    new_files = kwargs['ti'].xcom_pull(task_ids="check_hdfs_files")

    if not new_files:
        logging.info("No new files found, skipping sync.")
        return

    # Get Access Token from Azure AD
    try:
        auth_response = requests.post(
            AUTH_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope": SCOPE,
            },
        )
        access_token = auth_response.json().get("access_token")
        if not access_token:
            raise ValueError("Failed to get access token.")
    except Exception as e:
        logging.error(f"Error getting access token: {e}")
        return

    # Upload each file to Power BI
    for file_name in new_files:
        try:
            file_path = f"{HDFS_PATH}{file_name}"
            logging.info(f"Processing file: {file_name}")

            # Read the file from HDFS
            hdfs = PyWebHdfsClient(host=HDFS_HOST, port=HDFS_PORT, user_name=HDFS_USER, timeout=120)
            file_data = hdfs.read_file(file_path)
            csv_data = io.StringIO(file_data.decode("utf-8"))
            
            # Try to parse CSV
            try:
                df = pd.read_csv(csv_data)
                df = df.fillna(0)  # Replace NaN with 0
                df.replace([float("inf"), float("-inf")], 0, inplace=True)  # Replace infinity values
            except pd.errors.ParserError as e:
                logging.error(f"Error parsing CSV file {file_name}: {e}")
                continue  # Skip this file and move to the next

            # Convert DataFrame to JSON and send to Power BI
            rows = df.to_dict(orient="records")
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{GROUP_ID}/datasets/{DATASET_ID}/addRows"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            response = requests.post(url, headers=headers, json={"rows": rows})

            if response.status_code == 200:
                logging.info(f"File {file_name} synced successfully to Power BI.")
            else:
                logging.error(f"Failed to sync file {file_name}: {response.status_code} - {response.text}")
        except Exception as e:
            logging.error(f"Error processing file {file_name}: {e}")


sync_to_powerbi_task = PythonOperator(
    task_id="sync_to_powerbi",
    python_callable=sync_to_powerbi,
    provide_context=True,
    dag=dag,
)

# Task 3: Log pipeline's status
def log_status(**kwargs):
    logging.info("Pipeline executed successfully")

log_status_task = PythonOperator(
    task_id="log_pipeline_status",
    python_callable=log_status,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
check_files_task >> sync_to_powerbi_task >> log_status_task