"""
ETL Job 3: Compute resort condition scores.

For each resort:
1. Pull recent observations from linked SNOTEL stations (weighted)
2. Compute rolling metrics (48h snowfall, 7d snowfall, snowpack trend)
3. Pull latest forecast data
4. Compute composite condition score (0-100)
5. Generate plain-language score explanation

Run from backend/: python etl/compute_conditions.py
"""

import os
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "snowview_db"),
    "user": os.getenv("DB_USER", "snowview"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Scoring weights
WEIGHT_SNOWFALL_48H = 0.30
WEIGHT_SNOW_DEPTH = 0.25
WEIGHT_FORECAST_72H = 0.20
WEIGHT_SNOWPACK_TREND = 0.15
WEIGHT_TEMPERATURE = 0.10


def get_resorts_with_stations(cur) -> list[dict]:
    """Fetch all resorts with their linked station info."""
    cur.execute("""
        SELECT
            r.id,
            r.name,
            r.state,
            r.base_elevation_ft,
            r.summit_elevation_ft,
            json_agg(json_build_object(
                'station_id', l.station_id,
                'weight', l.weight,
                'station_name', s.name
            )) AS stations
        FROM resorts r
        JOIN resort_station_links l ON l.resort_id = r.id
        JOIN snotel_stations s ON s.id = l.station_id
        GROUP BY r.id, r.name, r.state, r.base_elevation_ft, r.summit_elevation_ft
        ORDER BY r.state, r.name
    """)

    results = []
    for row in cur.fetchall():
        results.append({
            "id": row[0],
            "name": row[1],
            "state": row[2],
            "base_elevation_ft": row[3],
            "summit_elevation_ft": row[4],
            "stations": row[5],
        })
    return results


def get_weighted_observations(cur, stations: list[dict]) -> dict:
    """
    Pull recent observations for linked stations and compute
    weighted averages for key metrics.
    """
    station_ids = [s["station_id"] for s in stations]
    weights = {s["station_id"]: s["weight"] for s in stations}

    # Get latest observation date to anchor our calculations
    cur.execute("""
        SELECT MAX(obs_date)
        FROM snow_observations
        WHERE station_id = ANY(%s)
    """, (station_ids,))
    latest_date_row = cur.fetchone()
    if not latest_date_row or not latest_date_row[0]:
        return None

    latest_date = latest_date_row[0]

    # Current snow depth and SWE (latest day, weighted average)
    cur.execute("""
        SELECT
            o.station_id,
            o.snow_depth_in,
            o.swe_in,
            o.temp_max_f,
            o.temp_min_f
        FROM snow_observations o
        WHERE o.station_id = ANY(%s) AND o.obs_date = %s
    """, (station_ids, latest_date))

    total_weight = 0
    weighted_depth = 0
    weighted_swe = 0
    weighted_temp_max = 0
    weighted_temp_min = 0
    temp_count = 0

    for row in cur.fetchall():
        sid, depth, swe, tmax, tmin = row
        w = weights.get(sid, 1.0)

        if depth is not None:
            weighted_depth += depth * w
            total_weight += w
        if swe is not None:
            weighted_swe += swe * w

        if tmax is not None and tmin is not None:
            weighted_temp_max += tmax * w
            weighted_temp_min += tmin * w
            temp_count += w

    if total_weight == 0:
        return None

    current_depth = round(weighted_depth / total_weight, 1)
    current_swe = round(weighted_swe / total_weight, 1)
    temp_avg = round(((weighted_temp_max + weighted_temp_min) / (2 * temp_count)), 1) if temp_count > 0 else None

    # 48-hour snowfall: difference in snow depth over last 2 days
    cur.execute("""
        SELECT o.station_id, o.obs_date, o.snow_depth_in
        FROM snow_observations o
        WHERE o.station_id = ANY(%s)
          AND o.obs_date >= %s - INTERVAL '2 days'
          AND o.obs_date <= %s
        ORDER BY o.station_id, o.obs_date
    """, (station_ids, latest_date, latest_date))

    station_48h = {}
    for row in cur.fetchall():
        sid, date, depth = row
        if depth is None:
            continue
        if sid not in station_48h:
            station_48h[sid] = {}
        station_48h[sid][str(date)] = depth

    snowfall_48h = 0
    w_sum = 0
    for sid, dates in station_48h.items():
        sorted_dates = sorted(dates.keys())
        if len(sorted_dates) >= 2:
            diff = max(0, dates[sorted_dates[-1]] - dates[sorted_dates[0]])
            w = weights.get(sid, 1.0)
            snowfall_48h += diff * w
            w_sum += w

    snowfall_48h = round(snowfall_48h / w_sum, 1) if w_sum > 0 else 0

    # 7-day snowfall: difference in snow depth over last 7 days
    cur.execute("""
        SELECT o.station_id, o.obs_date, o.snow_depth_in
        FROM snow_observations o
        WHERE o.station_id = ANY(%s)
          AND o.obs_date >= %s - INTERVAL '7 days'
          AND o.obs_date <= %s
        ORDER BY o.station_id, o.obs_date
    """, (station_ids, latest_date, latest_date))

    station_7d = {}
    for row in cur.fetchall():
        sid, date, depth = row
        if depth is None:
            continue
        if sid not in station_7d:
            station_7d[sid] = {}
        station_7d[sid][str(date)] = depth

    snowfall_7d = 0
    w_sum = 0
    for sid, dates in station_7d.items():
        sorted_dates = sorted(dates.keys())
        if len(sorted_dates) >= 2:
            diff = max(0, dates[sorted_dates[-1]] - dates[sorted_dates[0]])
            w = weights.get(sid, 1.0)
            snowfall_7d += diff * w
            w_sum += w

    snowfall_7d = round(snowfall_7d / w_sum, 1) if w_sum > 0 else 0

    # Snowpack trend: compare 7-day avg SWE to 14-day avg SWE
    cur.execute("""
        SELECT
            AVG(CASE WHEN o.obs_date >= %s - INTERVAL '7 days' THEN o.swe_in END) AS swe_7d,
            AVG(CASE WHEN o.obs_date >= %s - INTERVAL '14 days'
                      AND o.obs_date < %s - INTERVAL '7 days' THEN o.swe_in END) AS swe_14d
        FROM snow_observations o
        WHERE o.station_id = ANY(%s)
          AND o.obs_date >= %s - INTERVAL '14 days'
          AND o.obs_date <= %s
    """, (latest_date, latest_date, latest_date, station_ids, latest_date, latest_date))

    trend_row = cur.fetchone()
    swe_7d = trend_row[0] if trend_row else None
    swe_14d = trend_row[1] if trend_row else None

    if swe_7d is not None and swe_14d is not None and swe_14d > 0:
        change_pct = ((swe_7d - swe_14d) / swe_14d) * 100
        if change_pct > 5:
            trend = "rising"
        elif change_pct < -5:
            trend = "declining"
        else:
            trend = "stable"
    else:
        trend = "stable"

    return {
        "current_snow_depth_in": current_depth,
        "swe_in": current_swe,
        "snowfall_48h_in": snowfall_48h,
        "snowfall_7d_in": snowfall_7d,
        "snowpack_trend": trend,
        "temp_avg_f": temp_avg,
        "latest_date": latest_date,
    }


def get_forecast_outlook(cur, resort_id: int) -> dict:
    """Get 72-hour forecast snowfall total for a resort."""
    cur.execute("""
        SELECT
            COALESCE(SUM(projected_snowfall_in), 0) AS snowfall_72h,
            AVG(temp_high_f) AS avg_high,
            AVG(temp_low_f) AS avg_low
        FROM forecasts
        WHERE resort_id = %s
          AND forecast_date >= CURRENT_DATE
          AND forecast_date < CURRENT_DATE + INTERVAL '3 days'
    """, (resort_id,))

    row = cur.fetchone()
    if row:
        return {
            "forecast_snowfall_72h_in": round(row[0], 1) if row[0] else 0,
            "forecast_avg_high": round(row[1], 1) if row[1] else None,
            "forecast_avg_low": round(row[2], 1) if row[2] else None,
        }
    return {"forecast_snowfall_72h_in": 0, "forecast_avg_high": None, "forecast_avg_low": None}


def compute_score(obs: dict, forecast: dict) -> tuple[int, str]:
    """
    Compute composite condition score (0-100) and explanation.

    Factors:
    - Recent snowfall 48h (30%) — fresh powder is king
    - Snow depth / SWE (25%) — base matters
    - Forecast 72h outlook (20%) — incoming snow is exciting
    - Snowpack trend (15%) — rising = good momentum
    - Temperature (10%) — colder preserves quality
    """
    factors = []

    # Factor 1: 48h snowfall (0-100)
    # 0 inches = 0, 6+ inches = 80, 12+ inches = 95, 24+ inches = 100
    sf48 = obs["snowfall_48h_in"]
    if sf48 >= 24:
        score_48h = 100
    elif sf48 >= 12:
        score_48h = 85 + (sf48 - 12) / 12 * 15
    elif sf48 >= 6:
        score_48h = 60 + (sf48 - 6) / 6 * 25
    elif sf48 > 0:
        score_48h = sf48 / 6 * 60
    else:
        score_48h = 0

    if sf48 >= 12:
        factors.append(f"{sf48}\" of fresh snow in 48 hours")
    elif sf48 >= 6:
        factors.append(f"{sf48}\" of new snow in 48 hours")

    # Factor 2: Snow depth (0-100)
    # 0 = 0, 24" = 40, 48" = 65, 96"+ = 90, 150"+ = 100
    depth = obs["current_snow_depth_in"]
    if depth >= 150:
        score_depth = 100
    elif depth >= 96:
        score_depth = 90 + (depth - 96) / 54 * 10
    elif depth >= 48:
        score_depth = 65 + (depth - 48) / 48 * 25
    elif depth >= 24:
        score_depth = 40 + (depth - 24) / 24 * 25
    elif depth > 0:
        score_depth = depth / 24 * 40
    else:
        score_depth = 0

    if depth >= 48:
        factors.append(f"strong {depth}\" base depth")
    elif depth < 12 and depth > 0:
        factors.append(f"thin {depth}\" base")
    elif depth == 0:
        factors.append("no snow on ground")

    # Factor 3: 72h forecast (0-100)
    fc72 = forecast["forecast_snowfall_72h_in"]
    if fc72 >= 18:
        score_forecast = 100
    elif fc72 >= 12:
        score_forecast = 80 + (fc72 - 12) / 6 * 20
    elif fc72 >= 6:
        score_forecast = 50 + (fc72 - 6) / 6 * 30
    elif fc72 > 0:
        score_forecast = fc72 / 6 * 50
    else:
        score_forecast = 0

    if fc72 >= 6:
        factors.append(f"{fc72}\" forecasted in next 72 hours")
    elif fc72 > 0:
        factors.append(f"light snow ({fc72}\") in the forecast")

    # Factor 4: Snowpack trend (0-100)
    trend = obs["snowpack_trend"]
    if trend == "rising":
        score_trend = 85
        factors.append("snowpack trending upward")
    elif trend == "stable":
        score_trend = 50
    else:  # declining
        score_trend = 15
        factors.append("snowpack declining")

    # Factor 5: Temperature (0-100)
    # Colder = better for snow quality
    # Below 25°F = great (90+), 25-32 = good (60-90), 32-40 = ok (30-60), above 40 = poor
    temp = obs["temp_avg_f"]
    if temp is not None:
        if temp <= 20:
            score_temp = 100
        elif temp <= 25:
            score_temp = 90 + (25 - temp) / 5 * 10
        elif temp <= 32:
            score_temp = 60 + (32 - temp) / 7 * 30
        elif temp <= 40:
            score_temp = 30 + (40 - temp) / 8 * 30
        else:
            score_temp = max(0, 30 - (temp - 40) / 10 * 30)

        if temp > 40:
            factors.append(f"warm temps ({temp}°F) affecting snow quality")
    else:
        score_temp = 50

    # Composite score
    composite = (
        score_48h * WEIGHT_SNOWFALL_48H +
        score_depth * WEIGHT_SNOW_DEPTH +
        score_forecast * WEIGHT_FORECAST_72H +
        score_trend * WEIGHT_SNOWPACK_TREND +
        score_temp * WEIGHT_TEMPERATURE
    )
    composite = max(0, min(100, round(composite)))

    # Build explanation
    if not factors:
        if composite >= 50:
            explanation = "Moderate conditions with stable snowpack"
        else:
            explanation = "Below average conditions currently"
    else:
        explanation = "Driven by " + ", ".join(factors[:3])

    return composite, explanation


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
            VALUES ('compute_conditions', 'running')
            RETURNING id
        """)
        return cur.fetchone()[0]


def compute_conditions():
    """Main condition computation."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    run_id = log_etl_run(cur, "running", 0)
    conn.commit()

    print("SnowView ETL — Condition Score Computation")
    print(f"{'='*60}")

    resorts = get_resorts_with_stations(cur)
    print(f"Processing {len(resorts)} resorts\n")

    upsert_sql = """
        INSERT INTO resort_conditions
            (resort_id, computed_date, current_snow_depth_in, snowfall_48h_in,
             snowfall_7d_in, swe_in, snowpack_trend, forecast_snowfall_72h_in,
             temp_avg_f, condition_score, score_explanation)
        VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (resort_id, computed_date)
        DO UPDATE SET
            current_snow_depth_in = EXCLUDED.current_snow_depth_in,
            snowfall_48h_in = EXCLUDED.snowfall_48h_in,
            snowfall_7d_in = EXCLUDED.snowfall_7d_in,
            swe_in = EXCLUDED.swe_in,
            snowpack_trend = EXCLUDED.snowpack_trend,
            forecast_snowfall_72h_in = EXCLUDED.forecast_snowfall_72h_in,
            temp_avg_f = EXCLUDED.temp_avg_f,
            condition_score = EXCLUDED.condition_score,
            score_explanation = EXCLUDED.score_explanation,
            updated_at = NOW()
    """

    total = 0
    current_state = None

    for resort in resorts:
        if resort["state"] != current_state:
            current_state = resort["state"]
            print(f"\n--- {current_state} ---")

        # Get weighted observations from linked stations
        obs = get_weighted_observations(cur, resort["stations"])
        if not obs:
            print(f"  {resort['name']}: NO DATA — skipping")
            continue

        # Get forecast outlook
        forecast = get_forecast_outlook(cur, resort["id"])

        # Compute score
        score, explanation = compute_score(obs, forecast)

        # Upsert
        cur.execute(upsert_sql, (
            resort["id"],
            obs["current_snow_depth_in"],
            obs["snowfall_48h_in"],
            obs["snowfall_7d_in"],
            obs["swe_in"],
            obs["snowpack_trend"],
            forecast["forecast_snowfall_72h_in"],
            obs["temp_avg_f"],
            score,
            explanation,
        ))

        total += 1
        print(f"  {resort['name']}: score {score}/100 — {explanation}")

    conn.commit()

    log_etl_run(cur, "success", total, run_id=run_id)
    conn.commit()

    print(f"\n{'='*60}")
    print(f"Computed conditions for {total} resorts")

    cur.close()
    conn.close()


if __name__ == "__main__":
    compute_conditions()
