"""
Resort endpoints — list, detail, nearby, and top resorts.
"""

from fastapi import APIRouter, Query, HTTPException
from app.db.database import get_db

router = APIRouter()


@router.get("")
async def list_resorts(
    state: str | None = Query(None, description="Filter by state (CA, CO, WA)"),
    min_score: int | None = Query(None, ge=0, le=100, description="Minimum condition score"),
):
    """List all resorts with latest condition scores."""
    with get_db() as conn:
        cur = conn.cursor()

        query = """
            SELECT
                r.id, r.name, r.state,
                r.base_elevation_ft, r.summit_elevation_ft,
                r.num_lifts, r.website_url,
                ST_Y(r.geom) AS lat, ST_X(r.geom) AS lng,
                c.condition_score, c.current_snow_depth_in,
                c.snowfall_48h_in, c.snowfall_7d_in,
                c.snowpack_trend, c.score_explanation,
                c.computed_date
            FROM resorts r
            LEFT JOIN resort_conditions c ON c.resort_id = r.id
                AND c.computed_date = (
                    SELECT MAX(computed_date) FROM resort_conditions WHERE resort_id = r.id
                )
            WHERE 1=1
        """
        params = []

        if state:
            query += " AND r.state = %s"
            params.append(state.upper())

        if min_score is not None:
            query += " AND c.condition_score >= %s"
            params.append(min_score)

        query += " ORDER BY COALESCE(c.condition_score, 0) DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        return [
            {
                "id": r[0], "name": r[1], "state": r[2],
                "base_elevation_ft": r[3], "summit_elevation_ft": r[4],
                "num_lifts": r[5], "website_url": r[6],
                "lat": r[7], "lng": r[8],
                "condition_score": r[9], "current_snow_depth_in": r[10],
                "snowfall_48h_in": r[11], "snowfall_7d_in": r[12],
                "snowpack_trend": r[13], "score_explanation": r[14],
                "computed_date": str(r[15]) if r[15] else None,
            }
            for r in rows
        ]


@router.get("/top")
async def top_resorts(
    state: str | None = Query(None, description="Filter by state"),
    limit: int = Query(5, ge=1, le=20, description="Number of results"),
):
    """Get resorts with the best current conditions."""
    with get_db() as conn:
        cur = conn.cursor()

        query = """
            SELECT
                r.id, r.name, r.state,
                r.base_elevation_ft, r.summit_elevation_ft,
                ST_Y(r.geom) AS lat, ST_X(r.geom) AS lng,
                c.condition_score, c.current_snow_depth_in,
                c.snowfall_48h_in, c.score_explanation
            FROM resorts r
            JOIN resort_conditions c ON c.resort_id = r.id
                AND c.computed_date = (
                    SELECT MAX(computed_date) FROM resort_conditions WHERE resort_id = r.id
                )
            WHERE c.condition_score IS NOT NULL
        """
        params = []

        if state:
            query += " AND r.state = %s"
            params.append(state.upper())

        query += " ORDER BY c.condition_score DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        return [
            {
                "id": r[0], "name": r[1], "state": r[2],
                "base_elevation_ft": r[3], "summit_elevation_ft": r[4],
                "lat": r[5], "lng": r[6],
                "condition_score": r[7], "current_snow_depth_in": r[8],
                "snowfall_48h_in": r[9], "score_explanation": r[10],
            }
            for r in rows
        ]


@router.get("/nearby")
async def nearby_resorts(
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius_miles: float = Query(100, description="Search radius in miles"),
):
    """Find resorts within a radius of a point (PostGIS spatial query)."""
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                r.id, r.name, r.state,
                r.base_elevation_ft, r.summit_elevation_ft,
                r.website_url,
                ST_Y(r.geom) AS lat, ST_X(r.geom) AS lng,
                ROUND((ST_Distance(
                    r.geom::geography,
                    ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography
                ) / 1609.34)::numeric, 1) AS distance_miles,
                c.condition_score, c.current_snow_depth_in,
                c.snowfall_48h_in, c.score_explanation
            FROM resorts r
            LEFT JOIN resort_conditions c ON c.resort_id = r.id
                AND c.computed_date = (
                    SELECT MAX(computed_date) FROM resort_conditions WHERE resort_id = r.id
                )
            WHERE ST_DWithin(
                r.geom::geography,
                ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography,
                %(radius)s * 1609.34
            )
            ORDER BY ST_Distance(r.geom::geography, ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography)
        """, {"lat": lat, "lng": lng, "radius": radius_miles})

        rows = cur.fetchall()

        return [
            {
                "id": r[0], "name": r[1], "state": r[2],
                "base_elevation_ft": r[3], "summit_elevation_ft": r[4],
                "website_url": r[5],
                "lat": r[6], "lng": r[7],
                "distance_miles": float(r[8]),
                "condition_score": r[9], "current_snow_depth_in": r[10],
                "snowfall_48h_in": r[11], "score_explanation": r[12],
            }
            for r in rows
        ]


@router.get("/{resort_id}")
async def get_resort(resort_id: int):
    """Get full detail for a single resort including conditions and linked stations."""
    with get_db() as conn:
        cur = conn.cursor()

        # Resort + conditions
        cur.execute("""
            SELECT
                r.id, r.name, r.state,
                r.base_elevation_ft, r.summit_elevation_ft,
                r.num_lifts, r.website_url,
                ST_Y(r.geom) AS lat, ST_X(r.geom) AS lng,
                c.condition_score, c.current_snow_depth_in,
                c.snowfall_48h_in, c.snowfall_7d_in, c.swe_in,
                c.snowpack_trend, c.forecast_snowfall_72h_in,
                c.temp_avg_f, c.score_explanation, c.computed_date
            FROM resorts r
            LEFT JOIN resort_conditions c ON c.resort_id = r.id
                AND c.computed_date = (
                    SELECT MAX(computed_date) FROM resort_conditions WHERE resort_id = r.id
                )
            WHERE r.id = %s
        """, (resort_id,))

        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Resort not found")

        # Linked stations
        cur.execute("""
            SELECT s.name, s.elevation_ft, l.distance_miles, l.weight
            FROM resort_station_links l
            JOIN snotel_stations s ON s.id = l.station_id
            WHERE l.resort_id = %s
            ORDER BY l.weight DESC
        """, (resort_id,))

        stations = [
            {
                "name": s[0], "elevation_ft": s[1],
                "distance_miles": float(s[2]), "weight": float(s[3]),
            }
            for s in cur.fetchall()
        ]

        # Forecast
        cur.execute("""
            SELECT forecast_date, projected_snowfall_in, temp_high_f, temp_low_f, wind_speed_mph
            FROM forecasts
            WHERE resort_id = %s AND forecast_date >= CURRENT_DATE
            ORDER BY forecast_date
            LIMIT 7
        """, (resort_id,))

        forecast = [
            {
                "date": str(f[0]), "snowfall_in": f[1],
                "temp_high_f": f[2], "temp_low_f": f[3], "wind_speed_mph": f[4],
            }
            for f in cur.fetchall()
        ]

        return {
            "id": row[0], "name": row[1], "state": row[2],
            "base_elevation_ft": row[3], "summit_elevation_ft": row[4],
            "num_lifts": row[5], "website_url": row[6],
            "lat": row[7], "lng": row[8],
            "conditions": {
                "condition_score": row[9],
                "current_snow_depth_in": row[10],
                "snowfall_48h_in": row[11],
                "snowfall_7d_in": row[12],
                "swe_in": row[13],
                "snowpack_trend": row[14],
                "forecast_snowfall_72h_in": row[15],
                "temp_avg_f": row[16],
                "score_explanation": row[17],
                "computed_date": str(row[18]) if row[18] else None,
            },
            "stations": stations,
            "forecast": forecast,
        }