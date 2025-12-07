from __future__ import annotations

from pathlib import Path 
from datetime import datetime, timedelta, date 
from typing import Any, Dict, List 
import json 

HAZARDS = {
    "heat": {"field": "heat_level", "min_level": 1, "min_duration": 2},
    "cold": {"field": "cold_level", "min_level": 1, "min_duration": 2},
    "wind": {"field": "wind_level", "min_level": 1, "min_duration": 1},
    "rain": {"field": "rain_level", "min_level": 1, "min_duration": 1},
}

DATE_FMT = "%Y-%m-%d"

def parse_date(s: str) -> date:
    return datetime.strptime(s, DATE_FMT).date()

def format_date(d: date) -> str:
    return d.strftime(DATE_FMT)

def summarise_run(rows: List[Dict[str, Any]], hazard: str) -> Dict[str, Any]:
    first = rows[0]
    last = rows[-1]

    levels = [int(r[f"{hazard}_level"]) for r in rows]
    max_level = max(levels)

    if hazard == "heat":
        max_value = max(float(r["tmax_c"]) for r in rows)
        value_field = "max_tmax_c"
    elif hazard == "cold":
        min_value = min(float(r["tmin_c"]) for r in rows)
        return {
            "country": first["country"],
            "region_code": first["region_code"],
            "hazard": hazard,
            "start_date": first["date"],
            "end_date": last["date"],
            "n_days": len(rows),
            "max_level": max_level,
            "min_tmin_c": min_value,
        }
    elif hazard == "wind":
        max_value = max(float(r["wind_max_kmh"]) for r in rows)
        value_field = "max_wind_max_kmh"
    else:  # rain
        max_value = max(float(r["rain_mm"]) for r in rows)
        value_field = "max_rain_mm"

    return {
        "country": first["country"],
        "region_code": first["region_code"],
        "hazard": hazard,
        "start_date": first["date"],
        "end_date": last["date"],
        "n_days": len(rows),
        "max_level": max_level,
        value_field: max_value,
    }

def detect_events_for_country(
        rows: List[Dict[str, Any]],
        hazard: str,
        cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    
    field = cfg["field"]
    min_level = cfg["min_level"]
    min_duration = cfg["min_duration"]

    rows_sorted = sorted(rows, key=lambda r: parse_date(r["date"]))
    events: List[Dict[str, Any]] = []

    current_run: List[Dict[str, Any]] = [] 
    prev_date: date | None = None 

    for row in rows_sorted:
        d = parse_date(row["date"])
        level = int(row[field])

        if level >= min_level:
            if current_run and prev_date is not None and d == prev_date + timedelta(days=1):
                current_run.append(row)
            else:
                if len(current_run) >= min_duration:
                    events.append(summarise_run(current_run, hazard))
                current_run = [row]
        else:
            if len(current_run) >= min_duration:
                events.append(summarise_run(current_run, hazard))
            current_run = []
        
        prev_date = d 


    if len(current_run) >= min_duration:
        events.append(summarise_run(current_run, hazard))
    
    return events

def main() -> None:
    
    base_dir = Path(__file__).resolve().parents[1]
    daily_path = base_dir / "data" / "regions_daily.json"
    alerts_path = base_dir / "data" / "alerts.json"

    rows: List[Dict[str, Any]] = json.loads(daily_path.read_text(encoding="utf-8"))

    by_region: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_region.setdefault(row["region_code"], []).append(row)

    all_alerts: List[Dict[str, Any]] = []
    for region_code, region_rows in by_region.items():
        for hazard, cfg in HAZARDS.items():
            events = detect_events_for_country(region_rows, hazard, cfg)
            all_alerts.extend(events)

    alerts_path.parent.mkdir(parents=True, exist_ok=True)
    alerts_path.write_text(json.dumps(all_alerts, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(all_alerts)} alerts to {alerts_path}")


if __name__ == "__main__":
    main()