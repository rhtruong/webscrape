from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../api_scripts')))

from fetch_bettingpros import main as fetch_bettingpros
from fetch_prizepicks import main as fetch_prizepicks

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

def migrate_to_postgres_task(**context):
    pass

task_fetch_bettingpros = PythonOperator(
    task_id='fetch_bettingpros',
    python_callable=fetch_bettingpros_task,  # Fixed
    dag=dag,
)

task_fetch_prizepicks = PythonOperator(
    task_id='fetch_prizepicks',
    python_callable=fetch_prizepicks_task,
    dag=dag,
)

task_migrate_to_postgres = PythonOperator(
    task_id='migrate_to_postgres',
    python_callable=migrate_to_postgres_task,  # Fixed
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task_fetch_bettingpros >>  task_migrate_to_postgres
task_fetch_prizepicks  >>  task_migrate_to_postgres