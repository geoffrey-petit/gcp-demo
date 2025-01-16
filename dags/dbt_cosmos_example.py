import os
import sys
import setproctitle
import ast
from datetime import datetime, timedelta, time
import logging
from pathlib import Path
import glob
import yaml
from airflow import DAG
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from cosmos import DbtTaskGroup, ExecutionConfig, ProjectConfig, ProfileConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode
from cosmos.profiles import SnowflakeEncryptedPrivateKeyPemProfileMapping
from xo_utils.notification_utils.mattermost import send_mattermost_alert


def alert_on_failure(context):
    """
    Send a Mattermost alert when the DAG fails.
    
    :param context: The context of the DAG run.
    """
    channel = "airflow-alerts-prd" if Variable.get(
            "AIRFLOW_ENV", default_var="STG") == "PRD" else "airflow-alerts-stg"
    
    # Get the error message if it exists
    error_message = None
    if 'exception' in context:
        error_message = str(context['exception'])

    send_mattermost_alert(
        channel=channel,
        dag_id=context['dag'].dag_id,
        task_id=context['task_instance'].task_id,
        execution_date=context['execution_date'],
        owner=context['dag'].owner,
        log_url=context['task_instance'].log_url,
        error_message=error_message 
    )

# environment and schema prefix determination..
env = Variable.get("AIRFLOW_ENV", "DEV").upper()
schema_prefix = {"PRD": "prd", "STG": "afs"}.get(env, '{{ var.value.dbt_schema_prefix }}')
target_env = "prd" if env == "PRD" else "stg"

# Profile configuration for Snowflake
profile_config = ProfileConfig(
    profile_name="xo_core",
    target_name=target_env,
    profile_mapping=SnowflakeEncryptedPrivateKeyPemProfileMapping(
        conn_id="snowflake_xo",
        profile_args={"database": f"{target_env}_raw", "schema": schema_prefix, "private_key": '{{ var.value.snowflake_xo_private_key }}'},
    ),
)

# DBT project and manifest paths 
project_path = "/usr/local/airflow/dags/dbt/xo_core"
#manifest_path = Path(project_path, 'prd-run-artifacts' if env == 'PRD' else 'stg-run-artifacts' if env == 'STG' else 'target', 'manifest.json')
manifest_path = Path(project_path, 'target', 'manifest.json')

project_config = ProjectConfig(dbt_project_path=Path(project_path), manifest_path=manifest_path)

# Execution configuration for DBT
execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    execution_mode=ExecutionMode.VIRTUALENV,
)

# Placeholder function for configuration validation
def validate_config(config):
    """Validate the provided configuration."""
    pass  # Implement validation logic as needed

def emit_dataset_conditional(**kwargs):
    externally_triggered = kwargs['dag_run'].external_trigger
    if externally_triggered:
        emit_dataset_param = bool(kwargs['params'].get('emit_dataset'))
        if emit_dataset_param:
            logging.info(f"Emitting to dataset as `emit_dataset` parameter is set to {emit_dataset_param}")
            return
        else:
            logging.info(f"Skipping task as `emit_dataset` parameter is set to {emit_dataset_param}")
            raise AirflowSkipException
    else:
        logging.info(f"This is not a manual run, emitting to dataset...")
        return

# Function to create the DAG based on job configuration
def create_dag(job_config):
    validate_config(job_config)
    default_args = {
        "owner": "DE",
        "depends_on_past": False,
        "start_date": datetime.combine(job_config["start_date"], time()),
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_on_failure,
    }

    render_config = RenderConfig(load_method=LoadMode.DBT_MANIFEST, **job_config.get("render_config", {}), emit_datasets=False)

    dag = DAG(
        job_config['name'],
        default_args=default_args,
        schedule=[Dataset(f"{dataset_name}") for dataset_name in job_config.get('depends_on_dataset')] if 'depends_on_dataset' in job_config else job_config.get("schedule", None),
        catchup=False,
        tags=job_config.get("tags", []),
        params={
            "full_refresh": Param(default=False, type="boolean"),
            "start_time": Param(default=f"{datetime.today() - timedelta(days=7)}", type="string", format="date-time"), # A default is required, but this default is not used
            "end_time": Param(default=f"{datetime.today()}", type="string", format="date-time"), # A default is required, but this default is not used
            "emit_dataset": Param(default=False, type="boolean")
        },
        render_template_as_native_obj=True,
        max_active_runs=1,
        concurrency=8,
    )

    with dag:
        start_task = EmptyOperator(task_id='start') # Start marker

        dbt_tg = DbtTaskGroup(
            project_config=project_config,
            execution_config=execution_config,
            profile_config=profile_config,
            render_config=render_config,
            operator_args={
                "install_deps": True,
                "full_refresh": "{{ params.get('full_refresh') }}",
                "vars": {
                    "start_time": "{{ params.get('start_time') if dag_run.external_trigger else data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}",
                    "end_time": "{{ params.get('end_time') if dag_run.external_trigger else data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}",
                    "backfill": "{{ dag_run.external_trigger }}",
                    "orchestrator": "Airflow",
                    "job_name": job_config['name'],
                    "disable_run_results": True,
                    "disable_dbt_artifacts_autoupload": True,
                    "disable_dbt_columns_autoupload":  True,
                    "disable_dbt_invocation_autoupload": True,
                },
                "dbt_cmd_global_flags": ["--cache-selected-only", "--log-level-file=none"],
            },
        )

        if 'wait_for_previous_dag' in job_config and 'depends_on_dataset' in job_config:
            logging.error(f"'wait_for_previous_dag' and 'depends_on_dataset' are mutually exclusive and cannot both be specified, but were specified for {job_config['name']}")
            raise Exception(f"'wait_for_previous_dag' and 'depends_on_dataset' are mutually exclusive and cannot both be specified, but were specified for {job_config['name']}")

        if 'wait_for_previous_dag' in job_config and isinstance(job_config['wait_for_previous_dag'], list):
            wait_tasks = []
            for previous_dag in job_config['wait_for_previous_dag']:
                wait_task = ExternalTaskSensor(
                    task_id=f"wait_for__{previous_dag}",
                    external_dag_id=previous_dag,
                    timeout=10800, # 180 mins
                    poke_interval=300, # 5 mins
                    check_existence=True,
                )
                wait_tasks.append(wait_task)
            # Chain the start task to each wait_task, then to the dbt task group
            start_task >> wait_tasks >> dbt_tg
        else:
            start_task >> dbt_tg

        emit_dataset_task = PythonOperator(task_id='emit_dataset',
                                python_callable=emit_dataset_conditional,
                                outlets=[Dataset(f"cosmos_dataset__{dag_id}")],
                                trigger_rule=job_config.get('emit_dataset_trigger_rule', 'all_success'),
                            )  # End marker

        if 'trigger_downstream_dag' in job_config and isinstance(job_config['trigger_downstream_dag'], list):
            trigger_tasks = []
            for trigger_dag in job_config['trigger_downstream_dag']:
                trigger_task = TriggerDagRunOperator(
                    task_id=f"trigger__{trigger_dag}",
                    trigger_dag_id=trigger_dag,
                )
                trigger_tasks.append(trigger_task)
            # Chain the tasks
            dbt_tg >> trigger_tasks >> emit_dataset_task 
        else:
            dbt_tg >> emit_dataset_task

    return dag

current_dag_id = None
if(len(sys.argv) > 3 and sys.argv[1] == "tasks"): # check if args are for task
    current_dag_id = sys.argv[3]
else:
    PROCTITLE_SUPERVISOR_PREFIX = "airflow task supervisor: "
    PROCTITLE_TASK_RUNNER_PREFIX = "airflow task runner: "
    proctitle = str(setproctitle.getproctitle())
    if proctitle.startswith(PROCTITLE_SUPERVISOR_PREFIX): # task is forked from celery
        args_string = proctitle[len(PROCTITLE_SUPERVISOR_PREFIX) :]
        args = ast.literal_eval(args_string) 
        if len(args) > 3 and args[1] == "tasks":
            current_dag_id = args[3]
        elif proctitle.startswith(PROCTITLE_TASK_RUNNER_PREFIX): #task is forked from task runner
            args = proctitle[len(PROCTITLE_TASK_RUNNER_PREFIX) :].split(" ")
            if len(args) > 0:
                current_dag_id = args[0]


# Load the YAML configuration and generate DAGs
yml_files_path = 'dags/dbt_cosmos_factory/*dbt_deployment*.yml'
# List all matching YAML files in the directory
yml_files = glob.glob(yml_files_path)
for file_path in yml_files:
    with open(file_path, 'r') as file:
        yaml_content = yaml.safe_load(file)
    for job_config in yaml_content.get("jobs", []):
        dag_id = job_config['name']
        if current_dag_id is not None and current_dag_id != dag_id:
            continue
        else:
            globals()[dag_id] = create_dag(job_config)