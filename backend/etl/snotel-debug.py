"""
Debug script to inspect raw SNOTEL API responses.
Run from backend/: python etl/debug_snotel.py
"""

import requests
import json

AWDB_DATA_URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"

# Test with a known good station — Virginia Lakes Ridge in CA
STATION = "846:CA:SNTL"

params = {
    "stationTriplets": STATION,
    "elements": "SNWD,WTEQ,PREC,TMAX,TMIN",
    "beginDate": "2026-04-09",
    "endDate": "2026-04-16",
    "duration": "DAILY",
}

print(f"Fetching data for {STATION}")
print(f"URL: {AWDB_DATA_URL}")
print(f"Params: {json.dumps(params, indent=2)}")
print("=" * 60)

resp = requests.get(AWDB_DATA_URL, params=params, timeout=30)
print(f"Status: {resp.status_code}")
print(f"Content-Type: {resp.headers.get('Content-Type')}")
print(f"\nRaw response (first 3000 chars):")
print(resp.text[:3000])

print(f"\n{'='*60}")
print("Parsed JSON structure:")
try:
    data = resp.json()
    print(f"Type: {type(data)}")
    if isinstance(data, list):
        print(f"Length: {len(data)}")
        for i, item in enumerate(data[:2]):
            print(f"\nItem [{i}] keys: {list(item.keys()) if isinstance(item, dict) else type(item)}")
            print(json.dumps(item, indent=2, default=str)[:1000])
    elif isinstance(data, dict):
        print(f"Keys: {list(data.keys())}")
        print(json.dumps(data, indent=2, default=str)[:2000])
except Exception as e:
    print(f"JSON parse error: {e}")
