from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator




default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}


with DAG(
    dag_id = 'test_dag',
    default_args=default_args,
    description='test dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
) as dag:
    
    task1 = BashOperator(
                        task_id='first_task',
                        bash_command='echo hello world 1',
            )
    task2 = BashOperator(
        task_id  = 'second_task',
        bash_command = 'echo hello world 2'
    )
    
    task3 = BashOperator(
        task_id  = 'third_task',
        bash_command = 'echo hello world 3'
    )
    
    task1 >> task2
    task1 >> task3