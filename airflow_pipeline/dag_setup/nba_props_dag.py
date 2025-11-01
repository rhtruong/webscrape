from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../dag_setup')))

from migrate_to_postgres import __main__ as migrate_to_postgres_main
from fetch_bettingpros import main as fetch_bettingpros_df
from fetch_prizepicks import main as fetch_prizepicks_df
#from fetch_draftedge import main as fetch_draftedge_df


# Default arguments for the DAG
default_args = {
    'owner': 'LineDancers',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'nba_sportsbook_pipeline',
    default_args=default_args,
    description='Daily NBA sportsbook data scraping and collection pipeline',
    schedule_interval='0 22 * * *',  # Run daily at 10 AM
    catchup=False,
    tags=['nba', 'betting', 'data-pipeline'],
)

def migrate_to_postgres_task():
    table_name = 'player_lines'
    migrate_to_postgres_main(table_name)

task_migrate_to_postgres = PythonOperator(
    task_id='migrate_to_postgres',
    python_callable=migrate_to_postgres_task,
    dag=dag,
)

# Only one task now, no fetch tasks needed
