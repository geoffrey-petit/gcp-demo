'''
    DAG to Find Total Duration of Tasks per DAG for the specified duration and calculate cost.
    Input params: <days> float, 
                <deployment_cost_in_dollars> float (optional, default is 0)

    This script converts the days into hours and displays the results in seconds
    for converted number of hours. It also calculates the $ cost for each DAG
    based on the percentage usage if deployment_cost_in_dollars is provided....
'''
from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

@dag(
    'task_duration_and_cost', 
    schedule=None, 
    default_args=default_args,
    tags=['example_dag'],
    doc_md=__doc__,
)
def task_duration_and_cost(days: float, deployment_cost_in_dollars: float):

    @task
    def compute_task_run_duration_and_cost(days: float, deployment_cost_in_dollars: float):

        from airflow.models import DagRun
        from datetime import timedelta
        import pytz
        import pandas as pd
        
        hours = days * 24

        # Fetch DAG runs within the specified time window
        dag_runs = DagRun().find(
            execution_start_date=(datetime.now(pytz.utc) - timedelta(hours=hours)), 
            execution_end_date=datetime.now(pytz.utc)
        )

        dag_duration_result = {}
        for dag_run in dag_runs:
            dag_id = dag_run.dag_id
            total_duration = sum(ti.duration or 0 for ti in dag_run.get_task_instances())
            
            if dag_id in dag_duration_result:
                dag_duration_result[dag_id] += total_duration
            else:
                dag_duration_result[dag_id] = total_duration
        
        # Calculate the total duration of all the DAGs
        total_duration_all_dags = sum(dag_duration_result.values())

        # Prepare data for the table with additional cost calculation
        result_data = []
        for dag_id, total_seconds in dag_duration_result.items():
            percentage_usage = (total_seconds / total_duration_all_dags) * 100 if total_duration_all_dags > 0 else 0
            if deployment_cost_in_dollars > 0:
                cost_per_dag = (percentage_usage / 100) * deployment_cost_in_dollars if percentage_usage > 0 else 0
                result_data.append({
                    'DAG-ID': dag_id,
                    'Total_Task_Duration_(Seconds)': f"{total_seconds:.2f}",
                    'Percentage_Usage': f"{percentage_usage:.2f} %",
                    '$_Cost_per_DAG': f"${cost_per_dag:.2f}"
                })
            else:
                result_data.append({
                    'DAG-ID': dag_id,
                    'Total_Task_Duration_(Seconds)': f"{total_seconds:.2f}",
                    'Percentage_Usage': f"{percentage_usage:.2f} %"
                })
        
        # Convert the results into a dataframe and print as a formatted table
        df = pd.DataFrame(result_data)
        table_output = df.to_string(index=False)
        print(f"\n{'*'*17} DAG TASK DURATIONS AND COSTS for the past {hours} hours {'*'*17}:\n\n\n{table_output}\n")

    # Extract the values from Airflow params

    compute_task_run_duration_and_cost(days, deployment_cost_in_dollars)

# The default cost is 0 if no value is provided via the UI
dag = task_duration_and_cost(2,0)