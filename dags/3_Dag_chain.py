from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Define datasets
dataset1 = Dataset("s3://dataset1/output_1.txt")
dataset2 = Dataset("s3://dataset2/output_2.txt")

@dag(
    dag_id="first_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dataset_producer"],
    description="First DAG that defines and updates dataset1."
)
def first_dag():
    """
    First DAG that produces updates to dataset1.
    """
    @task(outlets=[dataset1])
    def update_dataset1():
        """
        Task that updates dataset1.
        """
        print("Updating dataset1...")
        # Simulate dataset update logic here
        return "Dataset1 updated successfully."

    update_dataset1()

first_dag_instance = first_dag()

@dag(
    dag_id="second_dag",
    start_date=datetime(2023, 1, 1),
    schedule=[dataset1],
    catchup=False,
    tags=["dataset_producer"],
    description="Second DAG that runs when dataset1 is updated and produces updates to dataset2."
)
def second_dag():
    """
    Second DAG that consumes dataset1 and produces updates to dataset2.
    """
    @task(outlets=[dataset2])
    def update_dataset2():
        """
        Task that updates dataset2.
        """
        print("Updating dataset2...")
        # Simulate dataset update logic here
        return "Dataset2 updated successfully."

    update_dataset2()

second_dag_instance = second_dag()

@dag(
    dag_id="third_dag",
    start_date=datetime(2023, 1, 1),
    schedule=[dataset2],
    catchup=False,
    tags=["dataset_consumer"],
    description="Third DAG that runs when dataset2 is updated."
)
def third_dag():
    """
    Third DAG that consumes dataset2.
    """
    @task
    def consume_dataset2():
        """
        Task that consumes dataset2.
        """
        print("Consuming dataset2...")
        # Simulate dataset consumption logic here
        return "Dataset2 consumed successfully."

    consume_dataset2()

third_dag_instance = third_dag()