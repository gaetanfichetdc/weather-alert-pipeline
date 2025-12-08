from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
from typing import List, Dict, Any

from .build_region_points_auto import main as build_regions_main
from .fetch_openmeteo import main as fetach_weather_main, HISTORY_DAYS
from .compute_indices import main as compute_indices_main
from .detect_alerts import main as detect_alerts_main
from .export_for_web import main as export_for_web_main


def write_status(started_at: str, completed_at: str) -> None:
    """
    Write a small status JSON summarising the latest pipeline run so the
    website can show when data was last refreshed and how much is available.
    """
    base = Path(__file__).resolve().parents[1]
    regions_path = base / "data" / "regions_daily.json"
    alerts_path = base / "data" / "alerts.json"
    status_path = base / "data" / "pipeline_status.json"

    try:
        regions: List[Dict[str, Any]] = json.loads(regions_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        regions = []

    try:
        alerts: List[Dict[str, Any]] = json.loads(alerts_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        alerts = []

    countries = sorted({row.get("country") for row in regions if row.get("country")})
    region_codes = sorted({row.get("region_code") for row in regions if row.get("region_code")})
    dates = sorted({row.get("date") for row in regions if row.get("date")})

    status = {
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "history_days": HISTORY_DAYS,
        "countries": countries,
        "regions_count": len(region_codes),
        "rows_count": len(regions),
        "alerts_count": len(alerts),
        "data_start_date": dates[0] if dates else None,
        "data_end_date": dates[-1] if dates else None,
    }

    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote pipeline_status.json to {status_path}")

def run_pipeline() -> None:
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"[{started}] Starting weather alert pipeline (regions)...")

    print("1) Building region grid...")
    build_regions_main()

    print("2) Fetching regional weather from Open-Meteo...")
    fetach_weather_main()

    print("3) Computing hazard levels per region/day...")
    compute_indices_main() 

    print("4) Detecting multi-day alerts per region...")
    detect_alerts_main() 

    done = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_status(started_at=started, completed_at=done)

    print("5) Exporting JSON for website...")
    export_for_web_main()

    print(f"[{done}] Pipeline completed.")

if __name__ == "__main__":
    run_pipeline()
