import argparse
import json
import logging
import logging.handlers
import os
from datetime import date, datetime
from typing import List

import pandas as pd
from dotenv import load_dotenv

from fetch_api import City, DEFAULT_CITIES, fetch_openmeteo
from fetch_db import load_sqlite
from transform import aggregate_city_daily, join_all, compute_kpis

def configure_logging():
    load_dotenv()
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "app.log")
    handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=int(os.getenv("LOG_MAX_BYTES", "1048576")),
        backupCount=int(os.getenv("LOG_BACKUP_COUNT", "3"))
    )
    fmt = json.dumps({
        "time": "%(asctime)s",
        "level": "%(levelname)s",
        "name": "%(name)s",
        "message": "%(message)s"
    })
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)

def parse_args():
    p = argparse.ArgumentParser(description="EcoAnalytics End-to-End Automation Pipeline")
    p.add_argument("--cities", type=str, default="San Francisco,Los Angeles,Sacramento",
                   help="Comma-separated list of cities")
    p.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    p.add_argument("--db", type=str, default="data/env.db", help="Path to SQLite DB")
    p.add_argument("--output", type=str, default="data/final_enriched.parquet",
                   help="Output parquet path")
    p.add_argument("--offline", action="store_true", help="Use synthetic API data (no network)")
    return p.parse_args()

def main():
    configure_logging()
    log = logging.getLogger("main")
    args = parse_args()
    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    # Build City list from DB station_meta (lat/lon) if present, else use defaults
    db = load_sqlite(args.db)
    meta = db["station_meta"]
    city_objs: List[City] = []
    for city in cities:
        row = meta[meta["city"] == city].head(1)
        if len(row):
            city_objs.append(City(city, float(row["lat"].values[0]), float(row["lon"].values[0])))
        else:
            # fallback to default list if city matches, else just use first default lat/lon
            found = next((c for c in DEFAULT_CITIES if c.name == city), None)
            if found:
                city_objs.append(found)
            else:
                city_objs.append(DEFAULT_CITIES[0])

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    log.info(json.dumps({"event":"start", "cities": cities, "start": args.start, "end": args.end}))

    # 1) External API (or synthetic fallback)
    weather = fetch_openmeteo(city_objs, start, end, offline=args.offline)
    log.info(json.dumps({"event":"weather_rows", "n": len(weather)}))

    # 2) DB -> city daily aggregates
    city_daily = aggregate_city_daily(db)
    # Filter to selected cities/date range
    city_daily = city_daily[city_daily["city"].isin(cities)]
    city_daily = city_daily[(city_daily["date"] >= start) & (city_daily["date"] <= end)]
    log.info(json.dumps({"event":"city_daily_rows", "n": len(city_daily)}))

    # 3) Join + KPIs
    joined = join_all(city_daily, db["energy"], weather)
    enriched = compute_kpis(joined)

    # 4) Save
    out_path = args.output
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    enriched.to_parquet(out_path, index=False)
    log.info(json.dumps({"event":"saved", "path": out_path, "rows": len(enriched)}))

    # Print a small preview
    print(enriched.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
