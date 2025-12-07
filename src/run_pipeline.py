from __future__ import annotations

from datetime import datetime, timezone

from .build_region_points_auto import main as build_regions_main 
from .fetch_openmeteo import main as fetach_weather_main 
from .compute_indices import main as compute_indices_main 
from .detect_alerts import main as detect_alerts_main 
from .export_for_web import main as export_for_web_main

def run_pipeline() -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"[{now}] Starting weather alert pipeline (regions)...")

    print("1) Building region grid...")
    build_regions_main()

    print("2) Fetching regional weather from Open-Meteo...")
    fetach_weather_main()

    print("3) Computing hazard levels per region/day...")
    compute_indices_main() 

    print("4) Detecting multi-day alerts per region...")
    detect_alerts_main() 

    print("5) Exporting JSON for website...")
    export_for_web_main()

    done = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{done}] Pipeline completed.")

if __name__ == "__main__":
    run_pipeline()