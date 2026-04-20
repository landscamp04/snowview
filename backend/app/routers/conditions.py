"""
Conditions endpoints — compare resorts and view history.
"""

from fastapi import APIRouter, Query, HTTPException
from app.db.database import get_db

router = APIRouter()


@router.get("/compare")
async def compare_resorts(
    ids: str = Query(..., description="Comma-separated resort IDs (e.g. 1,2,3)"),
):
    """Side-by-side comparison of multiple resorts."""
    try:
        resort_ids = [int(x.strip()) for x in ids.split(",")]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid resort IDs — use comma-separated integers")

    if len(resort_ids) < 2 or len(resort_ids) > 5:
        raise HTTPException(status_code=400, detail="Provide 2-5 resort IDs to compare")

    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                r.id, r.name, r.state,
                r.base_elevation_ft, r.summit_elevation_ft,
                ST_Y(r.geom) AS lat, ST_X(r.geom) AS lng,
                c.condition_score, c.current_snow_depth_in,
                c.snowfall_48h_in, c.snowfall_7d_in, c.swe_in,
                c.snowpack_trend, c.forecast_snowfall_72h_in,
                c.temp_avg_f, c.score_explanation
            FROM resorts r
            LEFT JOIN resort_conditions c ON c.resort_id = r.id
                AND c.computed_date = (
                    SELECT MAX(computed_date) FROM resort_conditions WHERE resort_id = r.id
                )
            WHERE r.id = ANY(%s)
            ORDER BY COALESCE(c.condition_score, 0) DESC
        """, (resort_ids,))

        rows = cur.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="No resorts found for the given IDs")

        resorts = [
            {
                "id": r[0], "name": r[1], "state": r[2],
                "base_elevation_ft": r[3], "summit_elevation_ft": r[4],
                "lat": r[5], "lng": r[6],
                "condition_score": r[7], "current_snow_depth_in": r[8],
                "snowfall_48h_in": r[9], "snowfall_7d_in": r[10],
                "swe_in": r[11], "snowpack_trend": r[12],
                "forecast_snowfall_72h_in": r[13], "temp_avg_f": r[14],
                "score_explanation": r[15],
            }
            for r in rows
        ]

        # Determine category winners
        scored = [r for r in resorts if r["condition_score"] is not None]
        winners = {}
        if scored:
            winners = {
                "overall": max(scored, key=lambda r: r["condition_score"])["name"],
                "most_snow_48h": max(scored, key=lambda r: r["snowfall_48h_in"] or 0)["name"],
                "deepest_base": max(scored, key=lambda r: r["current_snow_depth_in"] or 0)["name"],
                "best_forecast": max(scored, key=lambda r: r["forecast_snowfall_72h_in"] or 0)["name"],
            }

        return {
            "resorts": resorts,
            "winners": winners,
        }


@router.get("/history/{resort_id}")
async def resort_history(
    resort_id: int,
    days: int = Query(30, ge=7, le=90, description="Number of days of history"),
):
    """Get daily snow observation history for a resort's linked stations (weighted)."""
    with get_db() as conn:
        cur = conn.cursor()

        # Verify resort exists
        cur.execute("SELECT name FROM resorts WHERE id = %s", (resort_id,))
        resort = cur.fetchone()
        if not resort:
            raise HTTPException(status_code=404, detail="Resort not found")

        # Get weighted daily observations from linked stations
        cur.execute("""
            WITH weighted_obs AS (
                SELECT
                    o.obs_date,
                    SUM(o.snow_depth_in * l.weight) / SUM(l.weight) AS snow_depth_in,
                    SUM(o.swe_in * l.weight) / SUM(l.weight) AS swe_in,
                    SUM(o.temp_max_f * l.weight) / SUM(l.weight) AS temp_max_f,
                    SUM(o.temp_min_f * l.weight) / SUM(l.weight) AS temp_min_f
                FROM snow_observations o
                JOIN resort_station_links l ON l.station_id = o.station_id
                WHERE l.resort_id = %s
                  AND o.obs_date >= CURRENT_DATE - (%s || ' days')::INTERVAL
                GROUP BY o.obs_date
            )
            SELECT
                obs_date,
                ROUND(snow_depth_in::numeric, 1) AS snow_depth_in,
                ROUND(swe_in::numeric, 1) AS swe_in,
                ROUND(temp_max_f::numeric, 1) AS temp_max_f,
                ROUND(temp_min_f::numeric, 1) AS temp_min_f
            FROM weighted_obs
            ORDER BY obs_date
        """, (resort_id, days))

        rows = cur.fetchall()

        # Also get condition score history
        cur.execute("""
            SELECT computed_date, condition_score
            FROM resort_conditions
            WHERE resort_id = %s
              AND computed_date >= CURRENT_DATE - (%s || ' days')::INTERVAL
            ORDER BY computed_date
        """, (resort_id, days))

        scores = [{"date": str(r[0]), "score": r[1]} for r in cur.fetchall()]

        return {
            "resort_name": resort[0],
            "resort_id": resort_id,
            "days": days,
            "observations": [
                {
                    "date": str(r[0]),
                    "snow_depth_in": float(r[1]) if r[1] else None,
                    "swe_in": float(r[2]) if r[2] else None,
                    "temp_max_f": float(r[3]) if r[3] else None,
                    "temp_min_f": float(r[4]) if r[4] else None,
                }
                for r in rows
            ],
            "condition_scores": scores,
        }