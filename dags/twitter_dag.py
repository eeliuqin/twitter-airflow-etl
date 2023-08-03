from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_etl_dag',
    default_args=default_args,
    description='DAG for Twitter ETL process',
    # This DAG will be triggered once per day
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag, 
)

run_etl
