from datetime import datetime
from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from include.custom_wasb_prefix_sensor import CustomWasbPrefixSensor

CONTAINER_NAME = "airflow-landing-dw-ae-001"
PREFIX = "20"
AZURE_CONNECTION_ID = "azure_blob"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def list_and_delete_blobs(prefix: str):
    """
    1) List all blobs in the container that start with `prefix`.
    2) Print each one.
    3) Delete each blob.
    """
    wasb_hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_ID)
    blobs = wasb_hook.get_blobs_list(container_name=CONTAINER_NAME, prefix=prefix, delimiter='.zip.enc')

    if not blobs:
        print("No blobs found to delete.")
        return

    print(f"Found {len(blobs)} blob(s) with prefix '{prefix}':")
    print(blobs)
    for blob_name in blobs:
        print(f" - {blob_name}")
        wasb_hook.delete_file(container_name=CONTAINER_NAME, blob_name=blob_name)
        print(f"   Deleted: {blob_name}")

with DAG(
    dag_id="monitor_and_delete_files",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/5 * * * *",  # runs every 5 minutes
    catchup=False,
    max_active_runs=1
) as dag:

    # 1) Sensor: Just checks if there's at least one file with the given prefix
    wait_for_files = WasbPrefixSensor(
        task_id="wait_for_files",
        container_name=CONTAINER_NAME,
        prefix=PREFIX,                       # The prefix you're watching
        wasb_conn_id=AZURE_CONNECTION_ID,
        poke_interval=10,                   # checks every 10 seconds
        timeout=60 * 30,                    # 30-minute timeout
        mode="reschedule",                  # frees up the worker while waiting
    )

    # 2) PythonOperator: Lists and deletes any matching blobs
    delete_blobs = PythonOperator(
        task_id="delete_blobs",
        python_callable=list_and_delete_blobs,
        op_kwargs={'prefix': PREFIX}
    )

    wait_for_files >> delete_blobs