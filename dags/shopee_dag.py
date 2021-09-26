from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from shopee_etl import run_shopee_etl

default_args = {
    'owner': 'Phuc',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'shopee_dag',
    default_args=default_args,
    description='The DAG with ETL process!'
)

run_etl = PythonOperator(
    task_id='shopee_etl',
    python_callable=run_shopee_etl,
    dag=dag,
)

run_etl