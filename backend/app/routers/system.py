"""
System endpoints — health check, ETL status, data freshness.
"""

from fastapi import APIRouter
from app.db.database import get_db

router = APIRouter()


@router.get("/status")
async def system_status():
    """System health check and data freshness info."""
    with get_db() as conn:
        cur = conn.cursor()

        # Data counts
        cur.execute("SELECT COUNT(*) FROM resorts")
        resort_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM snotel_stations")
        station_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM snow_observations")
        observation_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM resort_conditions")
        condition_count = cur.fetchone()[0]

        # Latest data timestamps
        cur.execute("SELECT MAX(obs_date) FROM snow_observations")
        latest_observation = cur.fetchone()[0]

        cur.execute("SELECT MAX(computed_date) FROM resort_conditions")
        latest_conditions = cur.fetchone()[0]

        # Latest ETL runs
        cur.execute("""
            SELECT job_name, started_at, completed_at, status, records_processed
            FROM etl_runs
            ORDER BY started_at DESC
            LIMIT 5
        """)
        recent_runs = [
            {
                "job": r[0],
                "started": str(r[1]) if r[1] else None,
                "completed": str(r[2]) if r[2] else None,
                "status": r[3],
                "records": r[4],
            }
            for r in cur.fetchall()
        ]

        # Score distribution
        cur.execute("""
            SELECT
                COUNT(CASE WHEN condition_score >= 70 THEN 1 END) AS excellent,
                COUNT(CASE WHEN condition_score >= 40 AND condition_score < 70 THEN 1 END) AS good,
                COUNT(CASE WHEN condition_score < 40 THEN 1 END) AS fair
            FROM resort_conditions c
            WHERE c.computed_date = (SELECT MAX(computed_date) FROM resort_conditions)
        """)
        dist = cur.fetchone()

        return {
            "status": "healthy",
            "data": {
                "resorts": resort_count,
                "stations": station_count,
                "observations": observation_count,
                "conditions": condition_count,
            },
            "freshness": {
                "latest_observation": str(latest_observation) if latest_observation else None,
                "latest_conditions": str(latest_conditions) if latest_conditions else None,
            },
            "score_distribution": {
                "excellent_70_plus": dist[0] if dist else 0,
                "good_40_to_70": dist[1] if dist else 0,
                "fair_below_40": dist[2] if dist else 0,
            },
            "recent_etl_runs": recent_runs,
        }