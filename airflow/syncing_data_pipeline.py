import io
import pandas as pd
from pywebhdfs.webhdfs import PyWebHdfsClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import logging
import numpy as np
import pyodbc

# SQL SERVER Configuration
SQL_SERVER = "10.0.144.236"
SQL_PORT = "1433"
SQL_DATABASE = "CREDIT_CARD_TRANSACTIONS"
SQL_USER = "sa"
SQL_PASSWORD = "saadmin"

# HDFS Configuration
HDFS_HOST = "LAPTOP-QHS1R0BJ.mshome.net"
HDFS_PORT = "9870"
HDFS_USER = "trieu"
HDFS_PATH = "/user/spark/transactions_csv/"

current_date = datetime.now().strftime("%d%m%Y") 
MERGED_CSV_PATH = f"/user/spark/transactions_by_day/trans_{current_date}.csv"

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
        files_response = hdfs.list_dir(HDFS_PATH)
        
        if "FileStatuses" not in files_response or "FileStatus" not in files_response["FileStatuses"]:
            logging.warning("No files found in the specified HDFS directory.")
            return []

        # Get current date 
        today = date.today() 
        
        # Filter files
        new_files = [file["pathSuffix"] for file in files_response["FileStatuses"]["FileStatus"] 
                 if file["type"] == "FILE" and datetime.fromtimestamp(file["modificationTime"] / 1000).date() == today]
        # new_files = [file["pathSuffix"] for file in files_response["FileStatuses"]["FileStatus"] 
        #             if file["type"] == "FILE"]

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

# convert date function
def convert_date(date):
    if pd.isnull(date):
        return None
    if isinstance(date, str):
        try:
            date = pd.to_datetime(date, format='%d/%m/%Y')
        except ValueError:
            return None
    return date.strftime('%Y-%m-%d')

def sync_to_sqlserver(**kwargs):
    new_files = kwargs['ti'].xcom_pull(task_ids="check_hdfs_files")
    logging.info(f"Files received from HDFS check: {new_files}")

    if not new_files:
        logging.info("No new files found, skipping sync.")
        return

    # Kết nối HDFS
    hdfs = PyWebHdfsClient(host=HDFS_HOST, port=HDFS_PORT, user_name=HDFS_USER, timeout=120)

    conn = None
    cursor = None

    try:
        # Chuỗi kết nối sử dụng Windows Authentication
        # conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};PORT={SQL_PORT};DATABASE={SQL_DATABASE};Trusted_Connection=yes;"
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};PORT={SQL_PORT};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for file_name in new_files:
            try:
                # Đọc file từ HDFS
                file_path = f"{HDFS_PATH}{file_name}"
                logging.info(f"Processing file: {file_name}")
                file_data = hdfs.read_file(file_path)
                csv_data = io.StringIO(file_data.decode("utf-8"))
                df = pd.read_csv(csv_data)

                # Transform Amount: delete '$' and convert to float
                df['Amount'] = df['Amount'].str.replace(r"[$,]", "", regex=True).astype(float)
                # Transform date (yyyy-MM-dd)
                df['Date'] = df['Date'].apply(convert_date)
                
                for index, row in df.iterrows():
                    try:
                        cursor.execute("""
                            INSERT INTO TRANSACTIONS (
                                UserID, Card, Year, Month, Day, Date, Time, 
                                AmountUSD, AmountVND, UseChip, MerchantName, MerchantCity, 
                                MerchantState, ZipCode, MCC, Errors, IsFraud
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, 
                        row["User"], row["Card"], row["Year"], row["Month"], row["Day"], 
                        row["Date"], row["Time"], row["Amount"], row["Amount_VND"], 
                        row["Use Chip"], row["Merchant Name"], row["Merchant City"], 
                        row["Merchant State"], row["Zip"], row["MCC"], row["Errors?"], 
                        row["Is Fraud?"])
                    except Exception as e:
                        logging.error(f"Error inserting row: {row}. Error: {e}")

                conn.commit()
                logging.info(f"File {file_name} successfully loaded into SQL Server.")
            except Exception as e:
                logging.error(f"Error processing file {file_name}: {e}")

    except Exception as e:
        logging.error(f"Error connecting to SQL Server: {e}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



sync_to_sqlserver_task = PythonOperator(
    task_id="sync_to_sqlserver",
    python_callable=sync_to_sqlserver,
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
check_files_task >> sync_to_sqlserver_task >> log_status_task