# weather-alert-pipeline/src/build_region_points_auto.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List
import json

import geonamescache


def build_region_points_for_country(country_code: str, top_n: int = 3) -> List[Dict[str, Any]]:
    """
    Group cities by admin1 (region) and keep the top_n cities by population
    for each region in the given country.
    """
    gc = geonamescache.GeonamesCache()
    cities = gc.get_cities()

    by_region: Dict[str, List[Dict[str, Any]]] = {}

    for city in cities.values():
        if city.get("countrycode") != country_code:
            continue

        admin1 = city.get("admin1code")
        if not admin1:
            continue

        try:
            pop = int(city.get("population", 0) or 0)
        except (TypeError, ValueError):
            pop = 0

        entry = {
            "country": country_code,
            "region_id": admin1,                         # geonames admin1 code ('11', '24', ...)
            "region_code": f"{country_code}-{admin1}",   # placeholder code (e.g. 'FR-11')
            "city": city.get("name"),
            "lat": float(city.get("latitude")),
            "lon": float(city.get("longitude")),
            "population": pop,
        }
        
        by_region.setdefault(admin1, []).append(entry)

    points: List[Dict[str, Any]] = []
    for admin1, rows in by_region.items():
        rows_sorted = sorted(rows, key=lambda r: r["population"], reverse=True)
        points.extend(rows_sorted[:top_n])

    return points

def main() -> None:
    base = Path(__file__).resolve().parents[1]
    out_path = base / "data" / "region_points_admin1.json"

    points: List[Dict[str, Any]] = []
    for cc in ["FR", "ES", "DE", "IT", "PT"]:
        points.extend(build_region_points_for_country(cc, top_n=3))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(points, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {len(points)} region (admin1) points to {out_path}")

if __name__ == "__main__":
    main()