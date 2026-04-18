"""
SnowView ETL Pipeline Orchestrator

Runs all ETL jobs in sequence:
1. Ingest SNOTEL observations
2. Ingest NWS forecasts
3. Compute resort condition scores
4. Export GeoJSON

Run from backend/: python etl/run_pipeline.py
Optional: python etl/run_pipeline.py --days 7
"""

import argparse
import time
from datetime import datetime

from ingest_snotel import ingest_snotel
from ingest_nws import ingest_nws
from compute_conditions import compute_conditions
from export_geojson import export_geojson


def run_pipeline(days_back: int = 7):
    start = datetime.now()

    print("=" * 60)
    print("  SnowView ETL Pipeline")
    print(f"  Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Job 1: SNOTEL
    print("\n\n>>> JOB 1: SNOTEL Ingestion\n")
    t = time.time()
    ingest_snotel(days_back=days_back)
    print(f"\n  Completed in {time.time() - t:.1f}s")

    # Job 2: NWS Forecasts
    print("\n\n>>> JOB 2: NWS Forecast Ingestion\n")
    t = time.time()
    ingest_nws()
    print(f"\n  Completed in {time.time() - t:.1f}s")

    # Job 3: Compute Conditions
    print("\n\n>>> JOB 3: Condition Score Computation\n")
    t = time.time()
    compute_conditions()
    print(f"\n  Completed in {time.time() - t:.1f}s")

    # Job 4: Export GeoJSON
    print("\n\n>>> JOB 4: GeoJSON Export\n")
    t = time.time()
    export_geojson()
    print(f"\n  Completed in {time.time() - t:.1f}s")

    end = datetime.now()
    duration = (end - start).total_seconds()

    print("\n" + "=" * 60)
    print(f"  Pipeline complete!")
    print(f"  Total time: {duration:.1f}s ({duration/60:.1f} minutes)")
    print(f"  Finished: {end.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full SnowView ETL pipeline")
    parser.add_argument("--days", type=int, default=7, help="Days to look back for SNOTEL (default: 7)")
    args = parser.parse_args()

    run_pipeline(days_back=args.days)
