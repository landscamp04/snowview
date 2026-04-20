"""
Export endpoints — serve GeoJSON and Shapefile downloads.
"""

import os
import json
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse
from app.db.database import get_db

router = APIRouter()

GEOJSON_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "output", "resort_conditions.geojson"
)


@router.get("/geojson")
async def export_geojson():
    """Download the latest resort conditions as GeoJSON."""
    if os.path.exists(GEOJSON_PATH):
        return FileResponse(
            GEOJSON_PATH,
            media_type="application/geo+json",
            filename="snowview_conditions.geojson",
        )

    # Fallback: generate on the fly from database
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                r.id, r.name, r.state,
                r.base_elevation_ft, r.summit_elevation_ft,
                r.num_lifts, r.website_url,
                ST_X(r.geom) AS lng, ST_Y(r.geom) AS lat,
                c.condition_score, c.current_snow_depth_in,
                c.snowfall_48h_in, c.snowfall_7d_in,
                c.snowpack_trend, c.score_explanation
            FROM resorts r
            LEFT JOIN resort_conditions c ON c.resort_id = r.id
                AND c.computed_date = (
                    SELECT MAX(computed_date) FROM resort_conditions WHERE resort_id = r.id
                )
            ORDER BY r.state, r.name
        """)

        features = []
        for r in cur.fetchall():
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r[7], r[8]]},
                "properties": {
                    "id": r[0], "name": r[1], "state": r[2],
                    "base_elevation_ft": r[3], "summit_elevation_ft": r[4],
                    "num_lifts": r[5], "website_url": r[6],
                    "condition_score": r[9], "current_snow_depth_in": r[10],
                    "snowfall_48h_in": r[11], "snowfall_7d_in": r[12],
                    "snowpack_trend": r[13], "score_explanation": r[14],
                }
            })

        return JSONResponse({
            "type": "FeatureCollection",
            "features": features,
        })