from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys

# Add project root to Python path
sys.path.append('/Users/abdelrahmanata/stock-data-pipeline')

# Import pipeline scripts
from ingest_stock_data import download_stock_data
from clean_stock_data import clean_stock_data
from load_to_postgres import main as load_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_pipeline',
    default_args=default_args,
    description='Daily stock data pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 6, 19),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_stock_data',
        python_callable=download_stock_data,
    )

    clean_task = PythonOperator(
        task_id='clean_stock_data',
        python_callable=clean_stock_data,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    dbt_task = BashOperator(
        task_id='run_dbt',
        bash_command='cd /Users/abdelrahmanata/stock-data-pipeline/dbt_stock_pipeline && source /Users/abdelrahmanata/stock-data-pipeline/venv/bin/activate && dbt run',
    )

    ingest_task >> clean_task >> load_task >> dbt_task