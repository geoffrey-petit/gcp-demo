from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_sensor_example',
    default_args=default_args,
    description='A simple DAG with SQL Sensor',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

sql_sensor = SqlSensor(
    task_id='wait_for_sql_result',
    conn_id='my_postgres_connection',  # Replace with your connection ID
    sql="SELECT COUNT(*) FROM my_table WHERE my_condition = TRUE",
    mode='poke',  # 'poke' mode checks the condition at regular intervals
    poke_interval=60,  # Time in seconds between each check
    timeout=600,  # Timeout in seconds after which the task will fail if the condition is not met
    dag=dag,
)

next_task = DummyOperator(
    task_id='next_task',
    dag=dag,
)

start_task >> sql_sensor >> next_task