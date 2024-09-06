from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime

with DAG(dag_id='da_prod_data_and_analytics_monthly', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    da_prod_sotday_monthly_task = BashOperator(task_id='da_prod_sotday_monthly', bash_command='echo "Start of the day- my ecsd var: ECSD - SLACK_DA"', owner='ctmuser', doc='Start of the day job for monthly D&A scehdule')
    da_prod_ctm_monthly_vff_status_credit_bau_task = BashOperator(task_id='da_prod_ctm_monthly_vff_status_credit_bau', bash_command='. /home/controlm/analytics-jobs/env/set_env.sh; python3 /home/controlm/analytics-jobs/utility/docker-git-wrapper.py         -p R -r gitlab.com/flybuys_analytics/software_redevelopment/part.git -s         Coalition_Partner_Query/VFF_Status_Credit_BAU.r', owner='ctmuser', doc='Run the Monthly Velocity_status_credit_bau Job')
    da_prod_sotday_monthly_task >> da_prod_ctm_monthly_vff_status_credit_bau_task