import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    schedule_interval='*/5 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
)
def tutorial_taskflow_api_etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")
        return total_order_value
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])
tutorial_etl_dag = tutorial_taskflow_api_etl()
# import json
# from textwrap import dedent
# # import pendulum
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator

# with DAG(
#     'tutorial_etl_dag',
#     # These args will get passed on to each operator
#     # You can override them on a per-task basis during operator initialization
#     default_args={'retries': 2},
#     description='ETL DAG tutorial',
#     schedule_interval=None,
#     start_date= datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=['example'],
# ) as dag:
#     dag.doc_md = __doc__
#     def extract(**kwargs):
#         ti = kwargs['ti']
#         data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
#         ti.xcom_push('order_data', data_string)
#     def transform(**kwargs):
#         ti = kwargs['ti']
#         extract_data_string = ti.xcom_pull(task_ids='extract', key='order_data')
#         order_data = json.loads(extract_data_string)

#         total_order_value = 0
#         for value in order_data.values():
#             total_order_value += value

#         total_value = {"total_order_value": total_order_value}
#         total_value_json_string = json.dumps(total_value)
#         ti.xcom_push('total_order_value', total_value_json_string)
#     def load(**kwargs):
#         ti = kwargs['ti']
#         total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
#         total_order_value = json.loads(total_value_string)

#         print(total_order_value)
#     extract_task = PythonOperator(
#         task_id='extract',
#         python_callable=extract,
#     )
#     extract_task.doc_md = dedent(
#         """\
#     #### Extract task
#     A simple Extract task to get data ready for the rest of the data pipeline.
#     In this case, getting data is simulated by reading from a hardcoded JSON string.
#     This data is then put into xcom, so that it can be processed by the next task.
#     """
#     )

#     transform_task = PythonOperator(
#         task_id='transform',
#         python_callable=transform,
#     )
#     transform_task.doc_md = dedent(
#         """\
#     #### Transform task
#     A simple Transform task which takes in the collection of order data from xcom
#     and computes the total order value.
#     This computed value is then put into xcom, so that it can be processed by the next task.
#     """
#     )

#     load_task = PythonOperator(
#         task_id='load',
#         python_callable=load,
#     )
#     load_task.doc_md = dedent(
#         """\
#     #### Load task
#     A simple Load task which takes in the result of the Transform task, by reading it
#     from xcom and instead of saving it to end user review, just prints it out.
#     """
#     )

#     extract_task >> transform_task >> load_task
