from airflow import DAG
from datetime import datetime, timedelta
from crawl_url import run
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='crawl_data_6',
    default_args=default_args,
    # i want to schedele it in 5 minutes 
    
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:
    crawl_data = PythonOperator(
        task_id='crawl_data',
        python_callable=run,
    )
    crawl_data