from pathlib import Path 
import json 
from typing import Dict, Any, List 
from collections import defaultdict

def classify_levels(row: Dict[str, Any]) -> Dict[str, int]:
    
    tmax = float(row["tmax_c"])
    tmin = float(row["tmin_c"])
    wind = float(row["wind_max_kmh"])
    rain = float(row["rain_mm"])

    if tmax >= 40:
        heat_level = 3
    elif tmax >= 35:
        heat_level = 2
    elif tmax >= 30:
        heat_level = 1
    else:
        heat_level = 0

    if tmin <= -10:
        cold_level = 3
    elif tmin <= -5:
        cold_level = 2
    elif tmin <= 0:
        cold_level = 1
    else:
        cold_level = 0

    if wind >= 90:
        wind_level = 3
    elif wind >= 70:
        wind_level = 2
    elif wind >= 50:
        wind_level = 1
    else:
        wind_level = 0

    if rain >= 60:
        rain_level = 3
    elif rain >= 40:
        rain_level = 2
    elif rain >= 20:
        rain_level = 1
    else:
        rain_level = 0

    return {
        "heat_level": heat_level,
        "cold_level": cold_level,
        "wind_level": wind_level,
        "rain_level": rain_level,
    }

def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    raw_path = base_dir / "data" / "daily_region_raw.json"
    out_path = base_dir / "data" / "regions_daily.json"

    rows: List[Dict[str, Any]] = json.loads(raw_path.read_text(encoding="utf-8"))

    # group by (date, region_code)
    groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (row["date"], row["region_code"])
        groups[key].append(row)

    out_rows: List[Dict[str, Any]] = []
    for (date_str, region_code), grp in groups.items():
        tmax = max(float(r["tmax_c"]) for r in grp)
        tmin = min(float(r["tmin_c"]) for r in grp)
        wind = max(float(r["wind_max_kmh"]) for r in grp)
        rain = sum(float(r["rain_mm"]) for r in grp)  # or max/mean if you prefer

        base = grp[0]
        region_row = {
            "date": date_str,
            "country": base["country"],
            "region_code": region_code,
            "region_id": base["region_id"],
            "tmax_c": tmax,
            "tmin_c": tmin,
            "wind_max_kmh": wind,
            "rain_mm": rain,
        }
        region_row.update(classify_levels(region_row))
        out_rows.append(region_row)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(out_rows)} rows to {out_path}")

if __name__ == "__main__":
    main()
