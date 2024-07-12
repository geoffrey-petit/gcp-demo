from pendulum import datetime, duration
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.weekday import WeekDay

DAY_ACTIVITY_MAPPING = {
    "monday": {"is_weekday": True, "activity": "guitar lessons"},
    "tuesday": {"is_weekday": True, "activity": "studying"},
    "wednesday": {"is_weekday": True, "activity": "soccer practice"},
    "thursday": {"is_weekday": True, "activity": "contributing to Airflow"},
    "friday": {"is_weekday": True, "activity": "family dinner"},
    "saturday": {"is_weekday": False, "activity": "going to the beach"},
    "sunday": {"is_weekday": False, "activity": "sleeping in"},
}

@task(
    task_id="going_to_the_beach",
    multiple_outputs=True,  
)
def _going_to_the_beach() -> dict[str, str]:
    return {
        "subject": "Beach day!",
        "body": "It's Saturday and I'm heading to the beach.<br>Come join me!",
    }

@task.branch
def get_activity(day_name: str) -> str:
    activity_id = DAY_ACTIVITY_MAPPING[day_name]["activity"].replace(" ", "_")

    if DAY_ACTIVITY_MAPPING[day_name]["is_weekday"]:
        return f"weekday_activities.{activity_id}"

    return f"weekend_activities.{activity_id}"

@task.virtualenv(requirements=["beautifulsoup4==4.11.2"])
def inviting_friends(subject: str, body: str) -> None:
    from bs4 import BeautifulSoup

    print("Inviting friends...")
    html_doc = f"<title>{subject}</title><p>{body}</p>"
    soup = BeautifulSoup(html_doc, "html.parser")
    print(soup.prettify())

@dag(
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    default_args={
        "owner": "DS Team",
        "retries": 2,
        "retry_delay": duration(
            minutes=3
        ),
    },
    default_view="graph",
    catchup=False,
    tags=["example"],
)
def example_dag_advanced():
    # EmptyOperator placeholder for first task
    begin = EmptyOperator(task_id="begin")
    # Last task will only trigger if all upstream tasks have succeeded or been skipped
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    # This task checks which day of the week it is
    check_day_of_week = BranchDayOfWeekOperator(
        task_id="check_day_of_week",
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},  # This checks day of week
        follow_task_ids_if_true="weekend",  # Next task if criteria is met
        follow_task_ids_if_false="weekday",  # Next task if criteria is not met
        use_task_execution_day=True,  # If True, uses task’s execution day to compare with is_today
    )

    weekend = EmptyOperator(task_id="weekend")  # "weekend" placeholder task
    weekday = EmptyOperator(task_id="weekday")  # "weekday" placeholder task

    # Templated value for determining the name of the day of week based on the start date of the DAG Run
    day_name = "{{ dag_run.start_date.strftime('%A').lower() }}"

    # Begin weekday tasks.
    # Tasks within this TaskGroup (weekday tasks) will be grouped together in the Airflow UI
    @task_group
    def weekday_activities():
        # TaskFlow functions can also be reused which is beneficial if you want to use the same callable for
        # multiple tasks and want to use different task attributes.
        # See this tutorial for more information:
        #   https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html#reusing-a-decorated-task
        which_weekday_activity_day = get_activity.override(
            task_id="which_weekday_activity_day"
        )(day_name)

        for day, day_info in DAY_ACTIVITY_MAPPING.items():
            if day_info["is_weekday"]:
                day_of_week = Label(label=day)
                activity = day_info["activity"]

                # This task prints the weekday activity to bash
                do_activity = BashOperator(
                    task_id=activity.replace(" ", "_"),
                    # This is the Bash command to run
                    bash_command=f"echo It's {day.capitalize()} and I'm busy with {activity}.",
                )

                # Declaring task dependencies within the "TaskGroup" via the classic bitshift operator.
                which_weekday_activity_day >> day_of_week >> do_activity

    # Begin weekend tasks
    # Tasks within this TaskGroup will be grouped together in the UI
    @task_group
    def weekend_activities():
        which_weekend_activity_day = get_activity.override(
            task_id="which_weekend_activity_day"
        )(day_name)

        # Labels that will appear in the Graph view of the Airflow UI
        saturday = Label(label="saturday")
        sunday = Label(label="sunday")

        # This task runs the Sunday activity of sleeping for a random interval between 1 and 30 seconds
        sleeping_in = BashOperator(
            task_id="sleeping_in", bash_command="sleep $[ (1 + $RANDOM % 30) ]s"
        )

        going_to_the_beach = _going_to_the_beach()  # Calling the TaskFlow task

        # Because the "_going_to_the_beach()" function has "multiple_outputs" enabled, each dict key is
        # accessible as their own "XCom" key.
        _inviting_friends = inviting_friends(
            subject=going_to_the_beach["subject"], body=going_to_the_beach["body"]
        )

        # Using "chain()" here for list-to-list dependencies which are not supported by the bitshift
        # operator and to simplify the notation for the desired dependency structure.
        chain(
            which_weekend_activity_day,
            [saturday, sunday],
            [going_to_the_beach, sleeping_in],
        )

    # Call the @task_group TaskFlow functions to instantiate them in the DAG
    _weekday_activities = weekday_activities()
    _weekend_activities = weekend_activities()

    # High-level dependencies between tasks
    chain(
        begin,
        check_day_of_week,
        [weekday, weekend],
        [_weekday_activities, _weekend_activities],
        end,
    )

    # Task dependency created by XComArgs:
    # going_to_the_beach >> inviting_friends


example_dag_advanced()
