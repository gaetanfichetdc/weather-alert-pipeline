import json
import time
from pathlib import Path
from typing import Any, Dict, List
from datetime import date, timedelta

import requests
from requests.exceptions import RequestException, ReadTimeout

BASE_DIR = Path(__file__).resolve().parents[1]
REGION_POINTS: List[Dict[str, Any]] = json.loads(
    (BASE_DIR / "data" / "region_points_admin1.json").read_text(encoding="utf-8")
)

# Number of days of history to keep per location using the Open‑Meteo archive API.
HISTORY_DAYS = 180

REQUEST_TIMEOUT_SECONDS = 90


def fetch_daily_for_point(pt: Dict[str, Any], start_date: str, end_date: str) -> List[Dict[str, Any]]:
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
        "start_date": start_date,
        "end_date": end_date,
    }

    # Basic rate limiting: small sleep between calls plus a simple retry on 429.
    for attempt in range(2):
        try:
            # Global throttle to avoid hitting Open‑Meteo per‑second limits.
            time.sleep(0.75)
            resp = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            break
        except ReadTimeout as error:
            if attempt == 0:
                print(
                    f"[fetch_openmeteo] Read timeout for {pt.get('region_code')} / {pt.get('city')}, "
                    "sleeping and retrying once...",
                )
                time.sleep(5.0)
                continue

            print(
                f"[fetch_openmeteo] Skipping {pt.get('region_code')} / {pt.get('city')} "
                f"due to read timeout: {error}"
            )
            return []
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
    out_dir = BASE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "daily_region_raw.json"

    existing_rows: List[Dict[str, Any]] = []
    if out_path.exists():
        try:
            parsed = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                existing_rows = parsed
        except json.JSONDecodeError:
            existing_rows = []

    # Define the desired history window: last HISTORY_DAYS up to yesterday.
    end = date.today() - timedelta(days=1)
    history_start = end - timedelta(days=HISTORY_DAYS - 1)

    # For incremental updates we track, per region_code, the latest date we
    # already have. Newly added regions (for a new country) will not appear
    # here and will get a full HISTORY_DAYS backfill automatically.
    latest_by_region: Dict[str, date] = {}
    for row in existing_rows:
        d = row.get("date")
        code = row.get("region_code")
        if not isinstance(d, str) or not isinstance(code, str):
            continue
        try:
            dt = date.fromisoformat(d)
        except ValueError:
            continue
        # Ignore dates older than our rolling history window; they will be
        # dropped anyway.
        if dt < history_start:
            continue
        prev = latest_by_region.get(code)
        if prev is None or dt > prev:
            latest_by_region[code] = dt

    new_rows: List[Dict[str, Any]] = []

    any_fetch = False
    for pt in REGION_POINTS:
        region_code = pt.get("region_code")
        latest_for_region = latest_by_region.get(region_code) if isinstance(region_code, str) else None

        if latest_for_region is None:
            # New region (e.g. new country just added): backfill the full window.
            start_fetch = history_start
        else:
            # Incremental: start one day after the region's latest date,
            # but never earlier than the start of the rolling window.
            candidate = latest_for_region + timedelta(days=1)
            start_fetch = max(candidate, history_start)

        if start_fetch > end:
            # Nothing new to fetch for this region.
            continue

        any_fetch = True
        start_iso = start_fetch.isoformat()
        end_iso = end.isoformat()
        print(
            f"[fetch_openmeteo] Fetching {start_iso} → {end_iso} for {region_code}",
        )
        new_rows.extend(fetch_daily_for_point(pt, start_iso, end_iso))

    if not any_fetch:
        print(
            "[fetch_openmeteo] No new dates to fetch for any region; "
            f"reusing existing daily_region_raw.json and trimming to the last {HISTORY_DAYS} days.",
        )

    combined = existing_rows + new_rows

    # Trim to the rolling HISTORY_DAYS window.
    cutoff_iso = history_start.isoformat()
    trimmed = [row for row in combined if isinstance(row.get("date"), str) and row["date"] >= cutoff_iso]

    out_path.write_text(json.dumps(trimmed, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {len(trimmed)} rows to {out_path}")

if __name__ == "__main__":
    main()
