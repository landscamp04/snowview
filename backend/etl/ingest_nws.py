"""
ETL Job 2: Ingest NWS forecasts for each resort.

Calls api.weather.gov to get gridded forecasts, extracts
snowfall projections, temperature, and wind speed per day.

Run from backend/: python etl/ingest_nws.py
"""

import os
import time
import requests
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "snowview_db"),
    "user": os.getenv("DB_USER", "snowview"),
    "password": os.getenv("DB_PASSWORD", ""),
}

NWS_BASE = "https://api.weather.gov"

# NWS requires a User-Agent header
HEADERS = {
    "User-Agent": "(SnowView, contact@snowview.app)",
    "Accept": "application/geo+json",
}


def get_resorts(cur) -> list[dict]:
    """Fetch all resorts with their coordinates."""
    cur.execute("""
        SELECT id, name, state, ST_Y(geom) AS lat, ST_X(geom) AS lng
        FROM resorts
        ORDER BY state, name
    """)
    return [
        {"id": row[0], "name": row[1], "state": row[2], "lat": row[3], "lng": row[4]}
        for row in cur.fetchall()
    ]


def get_grid_info(lat: float, lng: float) -> dict | None:
    """Get the NWS grid point for a lat/lng."""
    url = f"{NWS_BASE}/points/{lat:.4f},{lng:.4f}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        props = resp.json().get("properties", {})
        return {
            "office": props.get("gridId"),
            "grid_x": props.get("gridX"),
            "grid_y": props.get("gridY"),
            "forecast_url": props.get("forecastGridData"),
        }
    except requests.RequestException as e:
        print(f"    ERROR getting grid info: {e}")
        return None


def fetch_forecast(forecast_url: str) -> list[dict]:
    """
    Fetch detailed grid forecast data from NWS.
    Returns daily forecast records with snowfall, temp, wind.
    """
    try:
        resp = requests.get(forecast_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        props = resp.json().get("properties", {})
    except requests.RequestException as e:
        print(f"    ERROR fetching forecast: {e}")
        return []

    # Extract relevant forecast elements
    snowfall = props.get("snowfallAmount", {}).get("values", [])
    temp_max = props.get("maxTemperature", {}).get("values", [])
    temp_min = props.get("minTemperature", {}).get("values", [])
    wind_speed = props.get("windSpeed", {}).get("values", [])

    # Build a date-keyed map from each element
    date_map = {}

    def extract_daily(values, key, converter=None):
        """Parse NWS time-series values into the date map."""
        for v in values:
            raw_date = v.get("validTime", "").split("T")[0]
            val = v.get("value")
            if raw_date and val is not None:
                if converter:
                    val = converter(val)
                if raw_date not in date_map:
                    date_map[raw_date] = {}
                # Keep the first value for each date (highest resolution)
                if key not in date_map[raw_date]:
                    date_map[raw_date][key] = val

    # NWS snowfall is in mm, convert to inches
    extract_daily(snowfall, "snowfall_in", lambda x: round(x / 25.4, 2))

    # NWS temps are in Celsius, convert to Fahrenheit
    extract_daily(temp_max, "temp_high_f", lambda x: round(x * 9 / 5 + 32, 1))
    extract_daily(temp_min, "temp_low_f", lambda x: round(x * 9 / 5 + 32, 1))

    # NWS wind speed is in km/h, convert to mph
    extract_daily(wind_speed, "wind_mph", lambda x: round(x * 0.621371, 1))

    # Convert to list, limit to next 7 days
    forecasts = []
    for date_str, elements in sorted(date_map.items())[:7]:
        forecasts.append({
            "forecast_date": date_str,
            "projected_snowfall_in": elements.get("snowfall_in", 0),
            "temp_high_f": elements.get("temp_high_f"),
            "temp_low_f": elements.get("temp_low_f"),
            "wind_speed_mph": elements.get("wind_mph"),
        })

    return forecasts


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
            VALUES ('ingest_nws', 'running')
            RETURNING id
        """)
        return cur.fetchone()[0]


def ingest_nws():
    """Main NWS forecast ingestion."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Start ETL run log
    run_id = log_etl_run(cur, "running", 0)
    conn.commit()

    print(f"SnowView ETL — NWS Forecast Ingestion")
    print(f"{'='*60}")

    resorts = get_resorts(cur)
    print(f"Found {len(resorts)} resorts to process\n")

    upsert_sql = """
        INSERT INTO forecasts
            (resort_id, forecast_date, projected_snowfall_in, temp_high_f, temp_low_f, wind_speed_mph)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (resort_id, forecast_date)
        DO UPDATE SET
            projected_snowfall_in = EXCLUDED.projected_snowfall_in,
            temp_high_f = EXCLUDED.temp_high_f,
            temp_low_f = EXCLUDED.temp_low_f,
            wind_speed_mph = EXCLUDED.wind_speed_mph,
            fetched_at = NOW()
    """

    total_records = 0
    total_errors = 0
    current_state = None

    for i, resort in enumerate(resorts):
        if resort["state"] != current_state:
            current_state = resort["state"]
            print(f"\n--- {current_state} ---")

        # Step 1: Get grid point for this resort
        grid = get_grid_info(resort["lat"], resort["lng"])
        if not grid or not grid.get("forecast_url"):
            print(f"  [{i+1}/{len(resorts)}] {resort['name']}: no grid info")
            total_errors += 1
            time.sleep(0.5)
            continue

        # Step 2: Fetch forecast data
        forecasts = fetch_forecast(grid["forecast_url"])
        if not forecasts:
            print(f"  [{i+1}/{len(resorts)}] {resort['name']}: no forecast data")
            total_errors += 1
            time.sleep(0.5)
            continue

        # Step 3: Upsert into database
        resort_records = 0
        for fc in forecasts:
            try:
                cur.execute(upsert_sql, (
                    resort["id"],
                    fc["forecast_date"],
                    fc["projected_snowfall_in"],
                    fc["temp_high_f"],
                    fc["temp_low_f"],
                    fc["wind_speed_mph"],
                ))
                resort_records += 1
            except Exception as e:
                print(f"    ERROR inserting forecast: {e}")
                conn.rollback()
                continue

        total_records += resort_records
        print(f"  [{i+1}/{len(resorts)}] {resort['name']}: {resort_records} forecast days")

        conn.commit()

        # Rate limit — NWS asks for polite usage
        time.sleep(0.5)

    # Log completion
    log_etl_run(cur, "success", total_records, run_id=run_id)
    conn.commit()

    print(f"\n{'='*60}")
    print(f"Forecast ingestion complete:")
    print(f"  Resorts processed: {len(resorts)}")
    print(f"  Resorts with errors: {total_errors}")
    print(f"  Total forecast records upserted: {total_records}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    ingest_nws()
