from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../api_scripts')))

from fetch_bettingpros import main as fetch_bettingpros
from fetch_prizepicks import main as fetch_prizepicks
from google_drive_upload import upload_csv_to_drive


# Default arguments for the DAG
default_args = {
    'owner': 'LineDancers',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'nba_sportsbook_pipeline',
    default_args=default_args,
    description='Daily NBA sportsbook data collection pipeline',
    schedule_interval='0 10 * * *',  # Run daily at 10 AM
    catchup=False,
    tags=['nba', 'betting', 'data-pipeline'],
)

def fetch_bettingpros_task():
    filepath = fetch_bettingpros()
    if not filepath:
        raise Exception("Failed to fetch BettingPros data")
    return filepath

def fetch_prizepicks_task():
    """Task to fetch PrizePicks data"""
    filepath = fetch_prizepicks()
    if not filepath:
        raise Exception("Failed to fetch PrizePicks data")
    return filepath

def upload_bettingpros_task(**context):
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='fetch_bettingpros')
    
    if not filepath or not os.path.exists(filepath):
        raise Exception(f"BettingPros file not found: {filepath}")
    
    success = upload_csv_to_drive(filepath)
    if not success:
        raise Exception("Failed to upload BettingPros data to Google Drive")
    
    return f"Successfully uploaded {filepath}"

def upload_prizepicks_task(**context):
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='fetch_prizepicks')
    
    if not filepath or not os.path.exists(filepath):
        raise Exception(f"PrizePicks file not found: {filepath}")
    
    success = upload_csv_to_drive(filepath)
    if not success:
        raise Exception("Failed to upload PrizePicks data to Google Drive")
    
    return f"Successfully uploaded {filepath}"

task_fetch_bettingpros = PythonOperator(
    task_id='fetch_bettingpros',
    python_callable=fetch_bettingpros_task,
    dag=dag,
)

task_fetch_prizepicks = PythonOperator(
    task_id='fetch_prizepicks',
    python_callable=fetch_prizepicks_task,
    dag=dag,
)

task_upload_bettingpros = PythonOperator(
    task_id='upload_bettingpros',
    python_callable=upload_bettingpros_task,
    provide_context=True,
    dag=dag,
)

task_upload_prizepicks = PythonOperator(
    task_id='upload_prizepicks',
    python_callable=upload_prizepicks_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
# Both fetches can run in parallel
# Uploads depend on their respective fetches
task_fetch_bettingpros >> task_upload_bettingpros
task_fetch_prizepicks >> task_upload_prizepicks
