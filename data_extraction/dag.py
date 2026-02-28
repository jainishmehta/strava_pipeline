i transform it first:
from airflow.decorators import dag, task
from ingestion import Activity
import logging
import os
from datetime import datetime, date

def create_dag(dag_id, schedule, dag_number, default_args):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def strava_dag():
        @task()
        def ingest_activities(*args):
            logging.info(f"Ingesting activities: {args}")
            pipeline_activities = Activity()
            activities = pipeline_activities.get_activities()
            pipeline_activities.transform_activities(activities)
            pipeline_activities.validate_activities(activities)
            pipeline_activities.store_activities(activities=activities)
            return
        ingest_activities()
    return strava_dag()

number_of_dags = os.getenv("DYNAMIC_DAG_NUMBER", default=3)
number_of_dags = int(number_of_dags)

default_args = {"owner": "airflow", "date_executed": date.today()}
schedule = "@weekly"
dag_id = "strava_dag"
dag_number = 1
for i in range(number_of_dags):
    globals()[f"strava_dag_{i}"] = create_dag(dag_id, schedule, dag_number, default_args)