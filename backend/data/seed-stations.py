"""
Seed SNOTEL stations from the NRCS AWDB Web Service.
Pulls ALL SNTL stations nationally, then filters to CA, CO, WA.
State is extracted from the station triplet (format: ID:STATE:NETWORK).

Run from backend/: python data/seed_stations.py
"""

import os
import requests
import psycopg2

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "snowview_db"),
    "user": os.getenv("DB_USER", "snowview"),
    "password": os.getenv("DB_PASSWORD", ""),
}

AWDB_BASE = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations"

TARGET_STATES = {"CA", "CO", "WA"}


def fetch_all_snotel_stations() -> list[dict]:
    """Fetch all SNOTEL stations from AWDB, filter to target states."""
    params = {
        "networkCode": "SNTL",
        "returnFields": "stationTriplet,name,latitude,longitude,elevation",
    }

    print("Fetching all SNOTEL stations from NRCS...")
    resp = requests.get(AWDB_BASE, params=params, timeout=60)
    resp.raise_for_status()
    stations = resp.json()
    print(f"  API returned {len(stations)} total SNOTEL stations nationwide")

    results = []
    for s in stations:
        triplet = s.get("stationTriplet", "")

        # Triplet format is ID:STATE:NETWORK, e.g. "784:CA:SNTL"
        parts = triplet.split(":")
        if len(parts) != 3:
            continue
        if parts[2] != "SNTL":
            continue

        state = parts[1]
        if state not in TARGET_STATES:
            continue

        lat = s.get("latitude")
        lng = s.get("longitude")
        elev = s.get("elevation")
        name = s.get("name")

        if not all([lat, lng, name]):
            continue

        results.append({
            "station_triplet": triplet,
            "name": name,
            "latitude": float(lat),
            "longitude": float(lng),
            "elevation_ft": int(float(elev)) if elev else None,
            "state": state,
        })

    return results


def seed_stations():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Clear existing station data
    cur.execute("DELETE FROM resort_station_links;")
    cur.execute("DELETE FROM snow_observations;")
    cur.execute("DELETE FROM snotel_stations;")

    insert_sql = """
        INSERT INTO snotel_stations (station_triplet, name, elevation_ft, state, geom)
        VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ON CONFLICT (station_triplet) DO NOTHING
    """

    stations = fetch_all_snotel_stations()

    for s in stations:
        cur.execute(insert_sql, (
            s["station_triplet"],
            s["name"],
            s["elevation_ft"],
            s["state"],
            s["longitude"],
            s["latitude"],
        ))

    conn.commit()

    # Verification
    cur.execute("SELECT state, COUNT(*) FROM snotel_stations GROUP BY state ORDER BY state;")
    print("\nStations per state:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cur.close()
    conn.close()
    print(f"\nSeeded {len(stations)} SNOTEL stations into PostGIS.")


if __name__ == "__main__":
    seed_stations()
