"""
ETL Job 1: Ingest SNOTEL observations from NRCS AWDB REST API.

Pulls daily snow depth, SWE, precipitation, and temperature data
for all stations in the database. Supports backfill (default 7 days)
and scheduled daily runs.

Run from backend/: python etl/ingest_snotel.py
Optional args: python etl/ingest_snotel.py --days 30
"""

import os
import sys
import argparse
import requests
import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "snowview_db"),
    "user": os.getenv("DB_USER", "snowview"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# NRCS AWDB REST API endpoint for station data
AWDB_DATA_URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"

# SNOTEL elements we want
# SNWD = snow depth (inches)
# WTEQ = snow water equivalent (inches)
# PREC = accumulated precipitation (inches)
# TMAX = max temperature (°F)
# TMIN = min temperature (°F)
ELEMENTS = ["SNWD", "WTEQ", "PREC", "TMAX", "TMIN"]


def get_stations(cur) -> list[dict]:
    """Fetch all SNOTEL stations from the database."""
    cur.execute("""
        SELECT id, station_triplet, name, state
        FROM snotel_stations
        ORDER BY state, name
    """)
    return [
        {"id": row[0], "triplet": row[1], "name": row[2], "state": row[3]}
        for row in cur.fetchall()
    ]


def fetch_observations(station_triplet: str, begin_date: str, end_date: str) -> list[dict]:
    """
    Fetch daily observations for a single station from AWDB REST API.
    Returns a list of dicts with date and element values.

    API response format:
    [{ "stationTriplet": "...", "data": [{ "stationElement": {...}, "values": [...] }] }]
    """
    params = {
        "stationTriplets": station_triplet,
        "elements": ",".join(ELEMENTS),
        "beginDate": begin_date,
        "endDate": end_date,
        "duration": "DAILY",
    }

    try:
        resp = requests.get(AWDB_DATA_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"    ERROR fetching data: {e}")
        return []

    if not data:
        return []

    # Response is a list with one item per station triplet requested
    # Each item has "stationTriplet" and "data" (list of element records)
    station_data = data[0] if isinstance(data, list) and len(data) > 0 else {}
    element_records = station_data.get("data", [])

    if not element_records:
        return []

    # Parse into a date-keyed structure
    date_map = {}

    for element_record in element_records:
        element_code = element_record.get("stationElement", {}).get("elementCode", "")
        values = element_record.get("values", [])

        for v in values:
            date_str = v.get("date", "")
            value = v.get("value")

            if not date_str:
                continue

            # Clean date string (might include time)
            obs_date = date_str.split(" ")[0] if " " in date_str else date_str

            if obs_date not in date_map:
                date_map[obs_date] = {}

            date_map[obs_date][element_code] = float(value) if value is not None else None

    # Convert to list of observation records
    observations = []
    for date_str, elements in date_map.items():
        observations.append({
            "obs_date": date_str,
            "snow_depth_in": elements.get("SNWD"),
            "swe_in": elements.get("WTEQ"),
            "precip_accum_in": elements.get("PREC"),
            "temp_max_f": elements.get("TMAX"),
            "temp_min_f": elements.get("TMIN"),
        })

    return observations


def log_etl_run(cur, status: str, records: int, error: str = None, run_id: int = None):
    """Update or create an ETL run log entry."""
    if run_id:
        cur.execute("""
            UPDATE etl_runs
            SET completed_at = NOW(), status = %s, records_processed = %s, error_message = %s
            WHERE id = %s
        """, (status, records, error, run_id))
    else:
        cur.execute("""
            INSERT INTO etl_runs (job_name, status)
            VALUES ('ingest_snotel', 'running')
            RETURNING id
        """)
        return cur.fetchone()[0]


def ingest_snotel(days_back: int = 7):
    """Main ingestion function."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Start ETL run log
    run_id = log_etl_run(cur, "running", 0)
    conn.commit()

    end_date = datetime.now().strftime("%Y-%m-%d")
    begin_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"SnowView ETL — SNOTEL Ingestion")
    print(f"Date range: {begin_date} to {end_date} ({days_back} days)")
    print(f"{'='*60}")

    stations = get_stations(cur)
    print(f"Found {len(stations)} stations to process\n")

    upsert_sql = """
        INSERT INTO snow_observations
            (station_id, obs_date, snow_depth_in, swe_in, precip_accum_in, temp_max_f, temp_min_f)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, obs_date)
        DO UPDATE SET
            snow_depth_in = EXCLUDED.snow_depth_in,
            swe_in = EXCLUDED.swe_in,
            precip_accum_in = EXCLUDED.precip_accum_in,
            temp_max_f = EXCLUDED.temp_max_f,
            temp_min_f = EXCLUDED.temp_min_f
    """

    total_records = 0
    total_errors = 0
    current_state = None

    for i, station in enumerate(stations):
        # Print state header
        if station["state"] != current_state:
            current_state = station["state"]
            print(f"\n--- {current_state} ---")

        observations = fetch_observations(station["triplet"], begin_date, end_date)

        if not observations:
            print(f"  [{i+1}/{len(stations)}] {station['name']}: no data")
            total_errors += 1
            continue

        station_records = 0
        for obs in observations:
            try:
                cur.execute(upsert_sql, (
                    station["id"],
                    obs["obs_date"],
                    obs["snow_depth_in"],
                    obs["swe_in"],
                    obs["precip_accum_in"],
                    obs["temp_max_f"],
                    obs["temp_min_f"],
                ))
                station_records += 1
            except Exception as e:
                print(f"    ERROR inserting {obs['obs_date']}: {e}")
                conn.rollback()
                continue

        total_records += station_records
        print(f"  [{i+1}/{len(stations)}] {station['name']}: {station_records} observations")

        # Commit every 10 stations to avoid huge transactions
        if (i + 1) % 10 == 0:
            conn.commit()

    # Final commit
    conn.commit()

    # Log completion
    log_etl_run(cur, "success", total_records, run_id=run_id)
    conn.commit()

    print(f"\n{'='*60}")
    print(f"Ingestion complete:")
    print(f"  Stations processed: {len(stations)}")
    print(f"  Stations with no data: {total_errors}")
    print(f"  Total observations upserted: {total_records}")
    print(f"  Date range: {begin_date} to {end_date}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest SNOTEL observations")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back (default: 7)")
    args = parser.parse_args()

    ingest_snotel(days_back=args.days)
