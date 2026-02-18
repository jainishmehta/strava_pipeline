from stravalib import Client 
import sqlite3
import os
from dotenv import load_dotenv

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
                start_date_local TEXT,
                timezone TEXT,
                average_speed FLOAT,
                max_speed FLOAT ) """)
        conn.commit()
        return conn

    def store_activities(self, client, db_path="strava.db"):  
        load_dotenv()
        strava_access_token = os.getenv("STRAVA_ACCESS_TOKEN")
        print(f"Token loaded: {strava_access_token is not None}")
        self.client = Client(strava_access_token)

        activities = self.client.get_activities(after="2024-01-01", limit=100)
        conn = self.create_db()
        cursor = conn.cursor()
        for activity in activities:
            cursor.execute(""" INSERT OR REPLACE INTO activities (id, name, distance, moving_time, elapsed_time, total_elevation_gain, type, start_date, start_date_local, timezone, average_speed, max_speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (activity.id, activity.name, activity.distance, activity.moving_time, activity.elapsed_time, activity.total_elevation_gain, str(activity.type.root), activity.start_date, activity.start_date_local, activity.timezone, activity.average_speed, activity.max_speed))
        conn.commit()
        print(f"Activities stored:")
        conn.close()
        return activities