from pathlib import Path 
import json 

def main() -> None:
    base = Path(__file__).resolve().parents[1]

    regions_src = base / "data" / "regions_daily.json"
    alerts_src  = base / "data" / "alerts.json"

    website_root = base.parent / "website"

    out_dir = website_root / "public" / "projects" / "weather-alerts"
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, src in [("regions_daily.json", regions_src), ("alerts.json", alerts_src)]:
        data = json.loads(src.read_text(encoding="utf-8"))
        (out_dir / name).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {name} to {out_dir}")

if __name__ == "__main__":
    main()