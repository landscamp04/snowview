"""
Build resort ↔ SNOTEL station links using PostGIS spatial queries.
For each resort, find the nearest stations within 50 miles, ranked by
a combined score of distance and elevation similarity.
Links the top 3 most relevant stations per resort.

Run from backend/: python data/link_resort_stations.py
"""

import os
import psycopg2

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "snowview_db"),
    "user": os.getenv("DB_USER", "snowview"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Max distance in miles to consider a station relevant to a resort
MAX_DISTANCE_MILES = 50

# How many stations to link per resort
MAX_LINKS_PER_RESORT = 3

def build_links():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Clear existing links
    cur.execute("DELETE FROM resort_station_links;")

    # For each resort, find nearby SNOTEL stations ranked by relevance.
    # Relevance = inverse of (normalized distance + normalized elevation difference).
    # We use the resort's midpoint elevation (avg of base and summit) for comparison.
    #
    # ST_Distance with geography type gives meters; convert to miles.
    # We filter to stations within MAX_DISTANCE_MILES and same state (mountain weather
    # patterns are regional, so a CO station shouldn't feed a CA resort).

    max_distance_meters = MAX_DISTANCE_MILES * 1609.34

    query = """
        WITH resort_mid_elev AS (
            SELECT
                id AS resort_id,
                name AS resort_name,
                state,
                geom,
                (COALESCE(base_elevation_ft, 0) + COALESCE(summit_elevation_ft, 0)) / 2 AS mid_elevation_ft
            FROM resorts
        ),
        candidates AS (
            SELECT
                r.resort_id,
                r.resort_name,
                s.id AS station_id,
                s.name AS station_name,
                ROUND((ST_Distance(r.geom::geography, s.geom::geography) / 1609.34)::numeric, 2) AS distance_miles,
                ABS(COALESCE(s.elevation_ft, 0) - r.mid_elevation_ft) AS elevation_diff_ft,
                s.elevation_ft AS station_elevation
            FROM resort_mid_elev r
            JOIN snotel_stations s ON s.state = r.state
            WHERE ST_DWithin(r.geom::geography, s.geom::geography, %(max_dist)s)
        ),
        ranked AS (
            SELECT
                *,
                ROUND(
                    (1.0 / (1.0 + distance_miles / 50.0)) * 0.6 +
                    (1.0 / (1.0 + elevation_diff_ft / 3000.0)) * 0.4,
                    4
                ) AS weight,
                ROW_NUMBER() OVER (
                    PARTITION BY resort_id
                    ORDER BY
                        (distance_miles / 50.0) * 0.6 + (elevation_diff_ft / 3000.0) * 0.4 ASC
                ) AS rank
            FROM candidates
        )
        SELECT resort_id, resort_name, station_id, station_name,
               distance_miles, elevation_diff_ft, weight, station_elevation
        FROM ranked
        WHERE rank <= %(max_links)s
        ORDER BY resort_id, rank;
    """

    cur.execute(query, {"max_dist": max_distance_meters, "max_links": MAX_LINKS_PER_RESORT})
    rows = cur.fetchall()

    insert_sql = """
        INSERT INTO resort_station_links (resort_id, station_id, distance_miles, elevation_diff_ft, weight)
        VALUES (%s, %s, %s, %s, %s)
    """

    current_resort = None
    total = 0
    for row in rows:
        resort_id, resort_name, station_id, station_name, dist, elev_diff, weight, station_elev = row

        if resort_name != current_resort:
            current_resort = resort_name
            print(f"\n{resort_name}:")

        print(f"  → {station_name} ({station_elev} ft) — {dist} mi, {elev_diff} ft diff, weight: {weight}")

        cur.execute(insert_sql, (resort_id, station_id, dist, elev_diff, weight))
        total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nCreated {total} resort-station links.")

if __name__ == "__main__":
    build_links()
