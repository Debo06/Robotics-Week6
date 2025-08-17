import logging
import sqlite3
from pathlib import Path
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

def load_sqlite(db_path: str) -> dict:
    """
    Load expected tables from SQLite into DataFrames.
    Returns dict with keys: station_meta, air_quality, energy
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")
    con = sqlite3.connect(db_path)
    try:
        station_meta = pd.read_sql_query("SELECT * FROM station_meta", con, parse_dates=[])
        air_quality  = pd.read_sql_query("SELECT * FROM air_quality", con, parse_dates=["date"])
        energy       = pd.read_sql_query("SELECT * FROM energy", con, parse_dates=["date"])
    finally:
        con.close()
    # Ensure date types
    air_quality["date"] = pd.to_datetime(air_quality["date"]).dt.date
    energy["date"]      = pd.to_datetime(energy["date"]).dt.date
    return {"station_meta": station_meta, "air_quality": air_quality, "energy": energy}
