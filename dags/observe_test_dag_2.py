from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# Define the dataset
dataset1 = Dataset("s3://dataset1/output_1.txt")
dataset2 = Dataset("s3://dataset2/output_2.txt")

@dag(
    dag_id="update_dataset_dag_2",
    start_date=datetime(2023, 1, 1),
    schedule=[dataset1],
    catchup=False,
    tags=["dataset", "example"],
    description="A DAG that defines and updates a dataset using the outlets parameter."
)
def update_dataset_dag_2():
    """
    DAG that defines a dataset and updates it using the outlets parameter in one of the tasks.
    """

    @task
    def extract_data() -> str:
        """
        Task to simulate data extraction.
        Returns a string representing the extracted data.
        """
        print("Extracting data...")
        return "extracted_data"

    @task
    def transform_data(data: str) -> str:
        """
        Task to simulate data transformation.
        Takes the extracted data as input and returns transformed data.
        """
        print(f"Transforming data: {data}")
        return f"transformed_{data}"

    @task(outlets=[dataset2])
    def load_data(data: str):
        """
        Task to simulate loading data and updating the dataset.
        Takes the transformed data as input and updates the dataset.
        """
        print(f"Loading data: {data}")
        print(f"Dataset {dataset2.uri} updated.")

    # Define the task dependencies
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)

update_dataset_dag_2()