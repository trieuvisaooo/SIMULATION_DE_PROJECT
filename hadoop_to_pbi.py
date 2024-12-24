import io
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
from pywebhdfs.webhdfs import PyWebHdfsClient
import logging


current_path = os.getcwd()
client_id = "89301c43-9292-4cc0-a894-80d08a728ef1"
client_secret = "auE8Q~qNWEzfH9q-elbE28rhu2qsvsRhMfMd8csM"
tenant_id = "40127cd4-45f3-49a3-b05d-315a43a9f033"
scope = "https://analysis.windows.net/powerbi/api/.default"
auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
group_id = "34bc619a-fc6f-41ea-9682-b19deac7ae9d"
dataset_id = "e07ffdcc-34d7-47b2-bffc-d6f33f84cb0a"

# Cấu hình Hadoop HDFS
hdfs_host = "localhost"
hdfs_port = "9870"
hdfs_user = "hadoop"
hdfs_path = "/user/spark/transactions_csv/"  # Thư mục chứa file CSV

# Định nghĩa DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "hdfs_to_powerbi",
    default_args=default_args,
    description="Automate data pipeline from Hadoop to Power BI",
    schedule_interval="0 0 * * *",  # Lập lịch chạy hàng ngày lúc 00:00
    start_date=datetime(2023, 12, 24),
    catchup=False,
)

# Task 1: Kiểm tra file mới trên HDFS
def check_hdfs_files(**kwargs):
    import logging
    logging.info("Connecting to WebHDFS at localhost:9870")
    hdfs = PyWebHdfsClient(host=hdfs_host, port=hdfs_port, user_name=hdfs_user, timeout=120)
    files = hdfs.list_dir(hdfs_path, timeout=60)["FileStatuses"]["FileStatus"]
    logging.info(f"Found files: {files}")    
    new_files = [file["pathSuffix"] for file in files if file["type"] == "FILE"]
    return new_files

check_files_task = PythonOperator(
    task_id="check_hdfs_files",
    python_callable=check_hdfs_files,
    provide_context=True,
    dag=dag,
)

# Task 2: Đọc file và đẩy dữ liệu lên Power BI
def sync_to_powerbi(**kwargs):
    ti = kwargs['ti']
    hdfs = PyWebHdfsClient(host=hdfs_host, port=hdfs_port, user_name=hdfs_user, timeout=120)
    new_files = ti.xcom_pull(task_ids="check_hdfs_files")

    if not new_files:
        print("No new files found in HDFS.")
        return

    # Lấy Access Token từ Azure AD
    auth_response = requests.post(
        auth_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
    )
    access_token = auth_response.json().get("access_token")
    if not access_token:
        raise ValueError("Failed to get access token.")

    # Đẩy từng file lên Power BI
    for file_name in new_files:
        file_path = f"{hdfs_path}{file_name}"
        file_data = hdfs.read_file(file_path)
        csv_data = io.StringIO(file_data.decode("utf-8"))
        df = pd.read_csv(csv_data)

        # Chuyển dữ liệu sang JSON và gửi lên Power BI
        rows = df.to_dict(orient="records")
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/addRows"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        response = requests.post(url, headers=headers, json={"rows": rows})

        if response.status_code == 200:
            print(f"File {file_name} synced successfully to Power BI.")
        else:
            print(f"Failed to sync file {file_name}: {response.status_code} - {response.text}")
        
# Dòng chảy của DAG
sync_to_powerbi_task = PythonOperator(
    task_id="sync_to_powerbi",
    python_callable=sync_to_powerbi,
    provide_context=True,
    dag=dag,
)

# Xác định dòng chảy của DAG
check_files_task >> sync_to_powerbi_task