import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.exceptions import RequestException, ReadTimeout

BASE_DIR = Path(__file__).resolve().parents[1]
REGION_POINTS: List[Dict[str, Any]] = json.loads(
    (BASE_DIR / "data" / "region_points_admin1.json").read_text(encoding="utf-8")
)

# Number of days of history to keep per location using the Open‑Meteo archive API.
HISTORY_DAYS = 180

# Allow CI or local runs to tweak network behaviour via env vars.
REQUEST_TIMEOUT_SECONDS = int(os.getenv("WEATHER_REQUEST_TIMEOUT_SECONDS", "90"))
ARCHIVE_SLEEP_SECONDS = float(os.getenv("WEATHER_ARCHIVE_SLEEP_SECONDS", "0.75"))
FORECAST_SLEEP_SECONDS = float(os.getenv("WEATHER_FORECAST_SLEEP_SECONDS", "0.75"))

# Optional alternative forecast provider: OpenWeatherMap.
# When OPENWEATHER_API_KEY is set, daily-mode forecast requests will use
# the free 5‑day / 3‑hour forecast API instead of the Open‑Meteo forecast
# endpoint.
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Optional extra logging controlled via WEATHER_DEBUG=1.
WEATHER_DEBUG = os.getenv("WEATHER_DEBUG", "0") == "1"


def fetch_daily_for_point(pt: Dict[str, Any], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    Fetch daily *historical* data for a single point between start_date and
    end_date (inclusive) using the Open‑Meteo archive API.

    This is used for one‑off / manual backfills to build an initial history
    window (e.g. 180 days). It is intentionally separated from the lighter
    "daily" mode used by CI so we don't accidentally keep re‑backfilling
    history on GitHub Actions.
    """
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
            time.sleep(ARCHIVE_SLEEP_SECONDS)
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


def fetch_forecast_for_point(pt: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch a short *forecast* for a single point.

    - If OPENWEATHER_API_KEY is set, use OpenWeatherMap's free 5‑day /
      3‑hour forecast API and aggregate to daily values.
    - Otherwise, fall back to the Open‑Meteo forecast endpoint.

    In both cases we aim to keep just today + tomorrow so the daily CI
    pipeline remains lightweight.
    """
    if OPENWEATHER_API_KEY:
        base_url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
            "lat": pt["lat"],
            "lon": pt["lon"],
            "units": "metric",
            "appid": OPENWEATHER_API_KEY,
        }
        provider = "openweather-forecast"
    else:
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": pt["lat"],
            "longitude": pt["lon"],
            "daily": (
                "temperature_2m_max,temperature_2m_min,"
                "wind_speed_10m_max,precipitation_sum,"
                "snowfall_sum"
            ),
            # We only care about today and tomorrow; Open‑Meteo will always
            # include "today" as the first daily entry.
            "forecast_days": 2,
            "timezone": "Europe/Berlin",
        }
        provider = "open-meteo"

    if WEATHER_DEBUG:
        print(
            "[fetch_openmeteo] forecast: "
            f"provider={provider}, "
            f"region={pt.get('region_code')}, "
            f"city={pt.get('city')}, "
            f"timeout={REQUEST_TIMEOUT_SECONDS}s",
        )

    for attempt in range(2):
        try:
            time.sleep(FORECAST_SLEEP_SECONDS)
            resp = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            break
        except ReadTimeout as error:
            if attempt == 0:
                print(
                    f"[fetch_openmeteo] Read timeout (forecast, {provider}) for "
                    f"{pt.get('region_code')} / {pt.get('city')}, sleeping and retrying once...",
                )
                time.sleep(5.0)
                continue

            print(
                f"[fetch_openmeteo] Skipping forecast (forecast, {provider}) for "
                f"{pt.get('region_code')} / {pt.get('city')} due to read timeout: {error}",
            )
            return []
        except RequestException as error:
            status = getattr(getattr(error, "response", None), "status_code", None)
            if status == 429 and attempt == 0:
                print(
                    f"[fetch_openmeteo] 429 (forecast, {provider}) for "
                    f"{pt.get('region_code')} / {pt.get('city')}, sleeping and retrying once...",
                )
                time.sleep(3.0)
                continue

            if WEATHER_DEBUG:
                print(
                    f"[fetch_openmeteo] RequestException (forecast, {provider}) for "
                    f"{pt.get('region_code')} / {pt.get('city')}: status={status}, error={error}",
                )
            print(
                f"[fetch_openmeteo] Skipping forecast (forecast, {provider}) for "
                f"{pt.get('region_code')} / {pt.get('city')} due to error: {error}",
            )
            return []

    data = resp.json()

    rows: List[Dict[str, Any]] = []

    if OPENWEATHER_API_KEY:
        # OpenWeatherMap 5‑day / 3‑hour forecast: aggregate per calendar day.
        #
        # We keep just today and tomorrow, taking:
        # - max of temp_max
        # - min of temp_min
        # - max wind speed
        # - sum of rain and snow
        forecast_list = data.get("list") or []
        by_date: Dict[str, Dict[str, Any]] = {}

        for entry in forecast_list:
            dt = entry.get("dt")
            if not isinstance(dt, (int, float)):
                continue

            date_iso = datetime.utcfromtimestamp(dt).date().isoformat()
            main = entry.get("main") or {}
            hi = main.get("temp_max")
            lo = main.get("temp_min")
            wind_info = entry.get("wind") or {}
            wind_speed = wind_info.get("speed")

            # Rain/snow volumes are per 3h step; sum them for the day.
            rain_block = entry.get("rain") or {}
            snow_block = entry.get("snow") or {}
            rain_step = rain_block.get("3h") or 0.0
            snow_step = snow_block.get("3h") or 0.0

            # Require temperature and wind to be present.
            if hi is None or lo is None or wind_speed is None:
                continue

            agg = by_date.setdefault(
                date_iso,
                {
                    "tmax_c": hi,
                    "tmin_c": lo,
                    "wind_max_kmh": float(wind_speed) * 3.6,
                    "rain_mm": 0.0,
                    "snow_mm": 0.0,
                },
            )

            agg["tmax_c"] = max(agg["tmax_c"], hi)
            agg["tmin_c"] = min(agg["tmin_c"], lo)
            agg["wind_max_kmh"] = max(agg["wind_max_kmh"], float(wind_speed) * 3.6)
            agg["rain_mm"] += float(rain_step)
            agg["snow_mm"] += float(snow_step)

        for date_iso, agg in by_date.items():
            rows.append(
                {
                    "date": date_iso,
                    "country": pt["country"],
                    "region_id": pt["region_id"],
                    "region_code": pt["region_code"],
                    "city": pt["city"],
                    "tmax_c": agg["tmax_c"],
                    "tmin_c": agg["tmin_c"],
                    "wind_max_kmh": agg["wind_max_kmh"],
                    "rain_mm": agg["rain_mm"],
                    "snow_mm": agg["snow_mm"],
                },
            )
    else:
        dates = data["daily"]["time"]
        tmax = data["daily"]["temperature_2m_max"]
        tmin = data["daily"]["temperature_2m_min"]
        wind = data["daily"]["wind_speed_10m_max"]
        rain = data["daily"]["precipitation_sum"]
        snow = data["daily"].get("snowfall_sum")

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


def run_backfill_mode(out_path: Path) -> None:
    """
    Original "full history" mode: build / refresh a HISTORY_DAYS‑long window
    using the archive API. This is suitable for manual runs on your machine
    but is too heavy for a daily GitHub Actions job.
    """
    existing_rows: List[Dict[str, Any]] = []
    if out_path.exists():
        try:
            parsed = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                existing_rows = parsed
        except json.JSONDecodeError:
            existing_rows = []

    end = date.today() - timedelta(days=1)
    history_start = end - timedelta(days=HISTORY_DAYS - 1)

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
            start_fetch = history_start
        else:
            candidate = latest_for_region + timedelta(days=1)
            start_fetch = max(candidate, history_start)

        if start_fetch > end:
            continue

        any_fetch = True
        start_iso = start_fetch.isoformat()
        end_iso = end.isoformat()
        print(f"[fetch_openmeteo] Fetching {start_iso} → {end_iso} for {region_code}")
        new_rows.extend(fetch_daily_for_point(pt, start_iso, end_iso))

    if not any_fetch:
        print(
            "[fetch_openmeteo] No new dates to fetch for any region; "
            f"reusing existing daily_region_raw.json and trimming to the last {HISTORY_DAYS} days.",
        )

    combined = existing_rows + new_rows

    cutoff_iso = (end - timedelta(days=HISTORY_DAYS - 1)).isoformat()
    trimmed = [row for row in combined if isinstance(row.get("date"), str) and row["date"] >= cutoff_iso]

    out_path.write_text(json.dumps(trimmed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(trimmed)} rows to {out_path}")


def run_daily_mode(out_path: Path) -> None:
    """
    Lightweight daily mode for CI:

    - Assume we already have a good HISTORY_DAYS window committed in the repo.
    - Remove any rows for today / tomorrow from the raw file.
    - For each region point, fetch just today + tomorrow via the forecast API.
    - Append those rows and re‑trim to HISTORY_DAYS.
    """
    existing_rows: List[Dict[str, Any]] = []
    if out_path.exists():
        try:
            parsed = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                existing_rows = parsed
        except json.JSONDecodeError:
            existing_rows = []

    today = date.today()
    tomorrow = today + timedelta(days=1)
    today_iso = today.isoformat()
    tomorrow_iso = tomorrow.isoformat()

    # Keep only rows strictly before today; we'll overwrite today/tomorrow
    # with fresh forecast data.
    base_rows = [
        row
        for row in existing_rows
        if not (isinstance(row.get("date"), str) and row["date"] >= today_iso)
    ]

    new_rows: List[Dict[str, Any]] = []
    for pt in REGION_POINTS:
        rows = fetch_forecast_for_point(pt)
        # Safety: only keep the dates we care about.
        for r in rows:
            d = r.get("date")
            if d == today_iso or d == tomorrow_iso:
                new_rows.append(r)

    combined = base_rows + new_rows

    history_start = today - timedelta(days=HISTORY_DAYS - 1)
    cutoff_iso = history_start.isoformat()
    trimmed = [row for row in combined if isinstance(row.get("date"), str) and row["date"] >= cutoff_iso]

    out_path.write_text(json.dumps(trimmed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[fetch_openmeteo] Daily mode: wrote {len(trimmed)} rows (including today + tomorrow) to {out_path}")


def main() -> None:
    out_dir = BASE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "daily_region_raw.json"

    mode = os.getenv("WEATHER_FETCH_MODE", "daily").lower()

    # If there is no raw file yet (fresh clone), always do a full backfill
    # regardless of mode so we end up with a consistent history window.
    if not out_path.exists():
        print("[fetch_openmeteo] No existing daily_region_raw.json found – running backfill mode once.")
        run_backfill_mode(out_path)
        return

    if mode == "backfill":
        print("[fetch_openmeteo] WEATHER_FETCH_MODE=backfill – running full history mode.")
        run_backfill_mode(out_path)
    else:
        print("[fetch_openmeteo] WEATHER_FETCH_MODE=daily – fetching only today + tomorrow via forecast API.")
        run_daily_mode(out_path)

if __name__ == "__main__":
    main()
