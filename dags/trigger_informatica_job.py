from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'informatica_job_orchestration',
    default_args=default_args,
    description='A DAG to orchestrate Informatica jobs using REST API',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    trigger_informatica_job = HttpOperator(
        task_id='trigger_informatica_job',
        http_conn_id='informatica_api',  # Define this connection in Airflow UI
        endpoint='/api/v2/jobs/run',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"job_name": "my_informatica_job"}',
        response_check=lambda response: response.json()['status'] == 'success',
    )

    trigger_informatica_job