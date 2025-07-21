from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random
import time

@task
def perform_operation(task_id: str):
    """
    Perform a simple operation for load testing.
    This operation involves sleeping for a random duration and logging a message.
    """
    sleep_duration = random.uniform(0.1, 1.0)  # Random sleep duration between 0.1 and 1.0 seconds
    print(f"Task {task_id} is sleeping for {sleep_duration:.2f} seconds.")
    time.sleep(sleep_duration)
    print(f"Task {task_id} has completed.")

@dag(
    dag_id='dynamic_load_testing_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(seconds=0.1),  # Schedule the DAG to run every 0.1 seconds
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
    description="DAG for dynamically generating tasks to load test Astro deployment performance.",
    tags=['load_testing', 'dynamic_tasks'],
    max_active_runs=1,  # Limit to one active run at a time for controlled testing
)
def dynamic_load_testing_dag(number_of_tasks: int = 14):
    """
    Dynamically generate tasks for load testing Astro deployment performance.
    The number of tasks and their behavior can be adjusted via DAG parameters.
    """
    # Generate and execute the specified number of tasks dynamically
    for i in range(number_of_tasks):
        perform_operation.override(task_id=f"perform_operation_{i}")(task_id=f"Task_{i}")

dynamic_load_testing_dag()  # Instantiate the DAG with default parameters