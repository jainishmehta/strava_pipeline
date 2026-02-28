from stravalib import Client
import sqlite3
import os
from dotenv import load_dotenv
import great_expectations as gx
import pandas as pd
from datetime import datetime

class Activity:
    def __init__(self):
        return None
    def create_db(self, db_path="strava.db"):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(""" CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY,
        name TEXT,
        distance FLOAT,
        moving_time INTEGER,
        elapsed_time INTEGER,
        total_elevation_gain FLOAT,
        type TEXT,
        start_date TEXT,
        timezone TEXT,
        average_speed FLOAT,
        max_speed FLOAT,
        kudos_count INTEGER,
        comment_count INTEGER
        ) """)
        conn.commit()
        return conn

    def get_activities(self, after="2024-01-01", limit=100):
        load_dotenv("/home/jainishmehta/airflow/dags/.env")
        client = Client()
        token_response = client.refresh_access_token(
        client_id=int(os.getenv("STRAVA_CLIENT_ID")),
        client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
        refresh_token=os.getenv("STRAVA_REFRESH_TOKEN")
        )
        self.client = Client(token_response['access_token'])
        activities = list(self.client.get_activities(after=after, limit=limit))
        return activities

    def transform_activities(self, activities):
        activity_types = ["AlpineSki", "BackcountrySki", "Canoeing", "Crossfit", "EBikeRide", "Elliptical", "Golf",
        "Handcycle", "Hike", "IceSkate", "InlineSkate", "Kayaking", "Kitesurf", "NordicSki", "Ride", "RockClimbing",
        "RollerSki", "Rowing", "Run", "Sail", "Skateboard", "Snowboard", "Snowshoe", "Soccer", "StairStepper", "StandUpPaddling", "Surfing", "Swim",
        "Velomobile", "VirtualRide", "VirtualRun", "Walk", "WeightTraining", "Wheelchair", "Windsurf", "Workout", "Yoga"]
        for activity in activities:
            if activity.type.root=='VirtualRide':
                activity.type.root = "Ride"
            elif activity.type.root=='VirtualRun':
                activity.type.root = "Run"
            elif activity.type.root in activity_types:
                continue
            else:
                activity.type.root = "Other"
            if activity.distance:
                activity.distance = float(activity.distance / 1000)
            if activity.moving_time:
                activity.moving_time = int(activity.moving_time / 60)
        if activity.elapsed_time:
            activity.elapsed_time = int(activity.elapsed_time / 60)
        # in km/h
        if activity.average_speed:
            activity.average_speed = float(activity.average_speed * 3.6)
        # in km/h
        if activity.max_speed:
            activity.max_speed = float(activity.max_speed * 3.6)
        return activities

    def validate_activities(self, activities):
        data = [{ "id": a.id,
        "type": str(a.type.root),
        "distance": float(a.distance) if a.distance else None,
        "moving_time": int(a.moving_time) if a.moving_time else None,
        "elapsed_time": int(a.elapsed_time) if a.elapsed_time else None,
        "total_elevation_gain": float(a.total_elevation_gain) if a.total_elevation_gain else None,
        "average_speed": float(a.average_speed) if a.average_speed else None,
        "max_speed": float(a.max_speed) if a.max_speed else None,
        } for a in activities]

        df = pd.DataFrame(data)
        context = gx.get_context()
        ds = context.data_sources.add_pandas("pandas_datasource")
        da = ds.add_dataframe_asset("activities")
        batch_definition = da.add_batch_definition_whole_dataframe("activities_batch_def")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        suite = gx.ExpectationSuite(name="strava_activities_suite")
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="type", value_set=["AlpineSki", "BackcountrySki", "Canoeing", "Crossfit", "EBikeRide", "Elliptical", "Golf",
        "Handcycle", "Hike", "IceSkate", "InlineSkate", "Kayaking", "Kitesurf", "NordicSki", "Ride", "RockClimbing",
        "RollerSki", "Rowing", "Run", "Sail", "Skateboard", "Snowboard", "Snowshoe", "Soccer", "StairStepper", "StandUpPaddling", "Surfing", "Swim",
        "Velomobile", "VirtualRide", "VirtualRun", "Walk", "WeightTraining", "Wheelchair", "Windsurf", "Workout", "Yoga", "Other"]))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="distance", min_value=0, max_value=1000))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="moving_time", min_value=0, max_value=1000))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="elapsed_time", min_value=0, max_value=1000))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_elevation_gain", min_value=0, max_value=9000))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="average_speed", min_value=0, max_value=80))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="max_speed", min_value=0, max_value=80))
        context.suites.add(suite)
        validation_definition = gx.ValidationDefinition(
        data = batch_definition,
        suite = suite,
        name = "strava_activities_validation")
        validation_results = validation_definition.run(batch_parameters={"dataframe": df})
        if validation_results.success:
            print("Validation successful")
        else:
            print("Validation failed")
            print(validation_results.results)
            for result in validation_results.results:
                if not result.success:
                    expectation_type = result.expectation_config.type
                    column = result.expectation_config.kwargs.get("column", "Unknown Column")
                    print(f" -> Failed: {expectation_type} on column '{column}'")
                    print(f"-> Details: {result.results}")
        return validation_results
    
    def store_activities(self, activities, db_path="strava.db"):
        if activities is None:
            activities = self.get_activities(after="2024-01-01", limit=100)
        conn = self.create_db(db_path)
        cursor = conn.cursor()
        for activity in activities:
            cursor.execute(""" INSERT OR REPLACE INTO activities (id, name, distance, moving_time, elapsed_time, total_elevation_gain, type, start_date, timezone, average_speed, max_speed, kudos_count, comment_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (activity.id, activity.name, activity.distance, activity.moving_time, activity.elapsed_time, activity.total_elevation_gain, str(activity.type.root), activity.start_date, activity.timezone, activity.average_speed, activity.max_speed, activity.kudos_count, activity.comment_count))
        conn.commit()
        print(f"Activities stored:")
        conn.close()
        return