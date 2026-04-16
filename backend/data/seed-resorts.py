"""
Seed the resorts table from resorts.json into PostGIS.
Run from backend/: python data/seed_resorts.py
"""

import json
import os
import psycopg2
from pathlib import Path

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "snowview_db"),
    "user": os.getenv("DB_USER", "snowview"),
    "password": os.getenv("DB_PASSWORD", ""),
}

def seed_resorts():
    json_path = Path(__file__).parent / "resorts.json"
    with open(json_path, "r") as f:
        resorts = json.load(f)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Clear existing data
    cur.execute("DELETE FROM resort_station_links;")
    cur.execute("DELETE FROM resort_conditions;")
    cur.execute("DELETE FROM forecasts;")
    cur.execute("DELETE FROM resorts;")

    insert_sql = """
        INSERT INTO resorts (name, state, base_elevation_ft, summit_elevation_ft, num_lifts, website_url, geom)
        VALUES (%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
    """

    count = 0
    for r in resorts:
        cur.execute(insert_sql, (
            r["name"],
            r["state"],
            r["base_elevation_ft"],
            r["summit_elevation_ft"],
            r["num_lifts"],
            r["website_url"],
            r["lng"],  # longitude first for ST_MakePoint
            r["lat"],
        ))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Seeded {count} resorts into PostGIS.")

if __name__ == "__main__":
    seed_resorts()
