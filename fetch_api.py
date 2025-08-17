import logging
import os
from dataclasses import dataclass
from typing import Iterable, List
import pandas as pd
import numpy as np
import requests
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class City:
    name: str
    lat: float
    lon: float

DEFAULT_CITIES = [
    City("San Francisco", 37.7749, -122.4194),
    City("Los Angeles", 34.0522, -118.2437),
    City("Sacramento", 38.5816, -121.4944),
]

def _timeout() -> int:
    try:
        return int(os.getenv("HTTP_TIMEOUT", "15"))
    except Exception:
        return 15

def _base_url() -> str:
    return os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")

def fetch_openmeteo(cities: Iterable[City], start: date, end: date, offline: bool=False) -> pd.DataFrame:
    """
    Fetch daily weather metrics (temperature, humidity, windspeed) for each city and date.
    Returns a DataFrame with columns: city, date, t2m_mean, rh_mean, wind_mean
    If offline=True, generates synthetic but realistic values.
    """
    days = pd.date_range(start=start, end=end, freq="D")
    records: List[dict] = []
    if offline:
        rng = np.random.default_rng(42)
        for c in cities:
            base = 18 + 10*np.sin(np.linspace(0, 2*np.pi, len(days)))  # gentle seasonal swing
            for i, d in enumerate(days):
                records.append({
                    "city": c.name,
                    "date": d.date().isoformat(),
                    "t2m_mean": round(base[i] + rng.normal(0, 2), 1),
                    "rh_mean": int(np.clip(60 + rng.normal(0, 10), 20, 100)),
                    "wind_mean": round(abs(rng.normal(4.0, 1.5)), 1),
                })
        return pd.DataFrame(records)

    base_url = _base_url()
    session = requests.Session()
    for c in cities:
        params = {
            "latitude": c.lat,
            "longitude": c.lon,
            "daily": "temperature_2m_mean,relative_humidity_2m_mean,windspeed_10m_mean",
            "timezone": "auto",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        }
        try:
            resp = session.get(base_url, params=params, timeout=_timeout())
            resp.raise_for_status()
            js = resp.json()
            daily = js.get("daily", {})
            # Expect arrays aligned by index
            for i, d in enumerate(daily.get("time", [])):
                records.append({
                    "city": c.name,
                    "date": d,
                    "t2m_mean": daily.get("temperature_2m_mean", [None])[i],
                    "rh_mean": daily.get("relative_humidity_2m_mean", [None])[i],
                    "wind_mean": daily.get("windspeed_10m_mean", [None])[i],
                })
        except Exception as e:
            logger.exception("Open-Meteo fetch failed for %s; switching to synthetic for this city.", c.name)
            # Fallback: synthetic for this city
            subset = fetch_openmeteo([c], start, end, offline=True)
            records.extend(subset.to_dict(orient="records"))
    return pd.DataFrame(records)
