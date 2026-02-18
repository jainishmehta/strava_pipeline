from airflow.decorators import dag, task
from ingestion import Activity
import logging
import os
from datetime import datetime

def create_dag(dag_id, schedule, dag_number, default_args):
    @dag (dag_id, schedule, default_args, catchup=False)
    def strava_dag():
        @task()
        def ingest_activities(*args):
            logging.info(f"Ingesting activities: {args}")
            pipeline_activities = Activity()
            activities = pipeline_activities.store_activities(
            pipeline_activities.get_client())
            return activities
        ingest_activities()
    return strava_dag()

number_of_dags = os.getenv("DYNAMIC_DAG_NUMBER", default=3)
number_of_dags = int(number_of_dags)

default_args = {"owner": "airflow", "start_date": datetime(2023, 7, 1)}
schedule = "@weekly"
dag_id = "strava_dag"
dag_number = 1
for i in range(number_of_dags):
    globals()[f"strava_dag_{i}"] = create_dag(dag_id, schedule, dag_number, default_args)
    dag_number += 1