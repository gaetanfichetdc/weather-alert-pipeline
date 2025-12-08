import json
import time
from pathlib import Path
from typing import Any, Dict, List
from datetime import date, timedelta

import requests
from requests.exceptions import RequestException

BASE_DIR = Path(__file__).resolve().parents[1]
REGION_POINTS: List[Dict[str, Any]] = json.loads(
    (BASE_DIR / "data" / "region_points_admin1.json").read_text(encoding="utf-8")
)

# Number of days of history to fetch per location using the Open‑Meteo archive API.
HISTORY_DAYS = 180

# Archive API is historical only; we fetch up to yesterday to avoid mixing in
# partial current‑day data.
_end = date.today() - timedelta(days=1)
_start = _end - timedelta(days=HISTORY_DAYS - 1)
START_DATE = _start.isoformat()
END_DATE = _end.isoformat()


def fetch_daily_for_point(pt: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Use historical archive API instead of the forecast endpoint so we can
    # pull a longer continuous history.
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": pt["lat"],
        "longitude": pt["lon"],
        "daily": (
            "temperature_2m_max,temperature_2m_min,"
            "wind_speed_10m_max,precipitation_sum,"
            "snowfall_sum"
        ),
        "timezone": "Europe/Berlin",
        "start_date": START_DATE,
        "end_date": END_DATE,
    }

    # Basic rate limiting: small sleep between calls plus a simple retry on 429.
    for attempt in range(2):
        try:
            # Global throttle to avoid hitting Open‑Meteo per‑second limits.
            time.sleep(0.75)
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            break
        except RequestException as error:
            status = getattr(getattr(error, "response", None), "status_code", None)
            if status == 429 and attempt == 0:
                # Too many requests – back off and retry once.
                print(
                    f"[fetch_openmeteo] 429 for {pt.get('region_code')} / {pt.get('city')}, "
                    "sleeping and retrying once...",
                )
                time.sleep(3.0)
                continue

            print(f"[fetch_openmeteo] Skipping {pt.get('region_code')} / {pt.get('city')} due to error: {error}")
            return []

    data = resp.json() 

    dates = data["daily"]["time"]
    tmax  = data["daily"]["temperature_2m_max"]
    tmin  = data["daily"]["temperature_2m_min"]
    wind  = data["daily"]["wind_speed_10m_max"]
    rain  = data["daily"]["precipitation_sum"]
    snow  = data["daily"].get("snowfall_sum")

    rows: list[dict] = []

    # Align optional snow series by index; if Open‑Meteo does not return
    # snowfall for a location these stay as None.
    for idx, (d, hi, lo, w, p) in enumerate(zip(dates, tmax, tmin, wind, rain)):
        snow_mm = snow[idx] if snow is not None and idx < len(snow) else None
        rows.append(
            {
                "date": d,
                "country": pt["country"],
                "region_id": pt["region_id"],
                "region_code": pt["region_code"],
                "city": pt["city"],
                "tmax_c": hi,
                "tmin_c": lo,
                "wind_max_kmh": w,
                "rain_mm": p,
                "snow_mm": snow_mm,
            },
        )

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
