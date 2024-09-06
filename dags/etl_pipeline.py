from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def trigger_informatica_job(**kwargs):
    import requests
    # Your Informatica API call here
    response = requests.post("https://informatica.example.com/api/v2/job", ...)
    if response.status_code == 200:
        print("Informatica job triggered successfully.")
    else:
        print(f"Failed to trigger Informatica job: {response.content}")
        response.raise_for_status()

def run_dbt_model(**kwargs):
    import subprocess
    # Run your dbt model here
    subprocess.run(["dbt", "run", "--models", "your_model_name"], check=True)

def load_data_to_snowflake(**kwargs):
    from snowflake.connector import connect
    # Your Snowflake data load logic here
    conn = connect(
        user='your_user',
        password='your_password',
        account='your_account'
    )
    cursor = conn.cursor()
    cursor.execute("COPY INTO your_snowflake_table FROM your_stage;")
    cursor.close()
    conn.close()

def refresh_powerbi_dashboard(**kwargs):
    import requests
    # Your Power BI refresh logic here
    response = requests.post(
        "https://api.powerbi.com/v1.0/myorg/groups/group_id/datasets/dataset_id/refreshes",
        headers={"Authorization": "Bearer YOUR_ACCESS_TOKEN"}
    )
    if response.status_code == 202:
        print("Power BI dashboard refresh triggered successfully.")
    else:
        print(f"Failed to refresh Power BI dashboard: {response.content}")
        response.raise_for_status()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_with_powerbi_refresh',
    default_args=default_args,
    description='A DAG with Informatica, dbt, Snowflake, and Power BI tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

trigger_informatica_job_task = PythonOperator(
    task_id='trigger_informatica_job',
    python_callable=trigger_informatica_job,
    dag=dag,
)

run_dbt_model_task = PythonOperator(
    task_id='run_dbt_model',
    python_callable=run_dbt_model,
    dag=dag,
)

load_data_to_snowflake_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    dag=dag,
)

refresh_powerbi_dashboard_task = PythonOperator(
    task_id='refresh_powerbi_dashboard',
    python_callable=refresh_powerbi_dashboard,
    dag=dag,
)

trigger_informatica_job_task >> run_dbt_model_task >> load_data_to_snowflake_task >> refresh_powerbi_dashboard_task