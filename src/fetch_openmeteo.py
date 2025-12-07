import json
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.exceptions import RequestException

BASE_DIR = Path(__file__).resolve().parents[1]
REGION_POINTS: List[Dict[str, Any]] = json.loads(
    (BASE_DIR / "data" / "region_points_admin1.json").read_text(encoding="utf-8")
)

def fetch_daily_for_point(pt: Dict[str, Any]) -> List[Dict[str, Any]]:
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": pt["lat"],
        "longitude": pt["lon"],
        "daily": "temperature_2m_max,temperature_2m_min,wind_speed_10m_max,precipitation_sum",
        "timezone": "Europe/Berlin",
        "past_days": 90,
        "forecast_days": 1,
    }

    try:
        resp = requests.get(base_url, params=params, timeout=60)
        resp.raise_for_status()
    except RequestException as error:
        print(f"[fetch_openmeteo] Skipping {pt.get('region_code')} / {pt.get('city')} due to error: {error}")
        return []

    data = resp.json() 

    dates = data["daily"]["time"]
    tmax  = data["daily"]["temperature_2m_max"]
    tmin  = data["daily"]["temperature_2m_min"]
    wind  = data["daily"]["wind_speed_10m_max"]
    rain  = data["daily"]["precipitation_sum"]

    rows: list[dict] = [] 

    for d, hi, lo, w, p in zip(dates, tmax, tmin, wind, rain):
        rows.append({
            "date": d,
            "country": pt["country"],
            "region_id": pt["region_id"],
            "region_code": pt["region_code"],
            "city": pt["city"],
            "tmax_c": hi,
            "tmin_c": lo,
            "wind_max_kmh": w,
            "rain_mm": p,
        })

    return rows 

def main() -> None:
    all_rows: list[dict] = []
    for pt in REGION_POINTS:
        all_rows.extend(fetch_daily_for_point(pt))

    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "daily_region_raw.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(all_rows)} rows to {out_path}")

if __name__ == "__main__":
    main()
