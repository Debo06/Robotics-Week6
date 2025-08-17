import logging
from typing import Tuple
import pandas as pd

logger = logging.getLogger(__name__)

AQI_CATEGORIES = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous"),
]

def categorize_aqi(aqi: float) -> str:
    for low, high, label in AQI_CATEGORIES:
        if low <= aqi <= high:
            return label
    return "Out of Range"

def aggregate_city_daily(db: dict) -> pd.DataFrame:
    """
    From station-level air_quality + station_meta, produce city-date aggregates.
    Returns columns: city, date, aqi_mean, co2_mean
    """
    aq = db["air_quality"].copy()
    meta = db["station_meta"][["station_id", "city"]].copy()
    merged = aq.merge(meta, on="station_id", how="left")
    city_daily = (
        merged.groupby(["city", "date"], as_index=False)
        .agg(aqi_mean=("aqi", "mean"), co2_mean=("co2_ppm", "mean"))
    )
    city_daily["aqi_mean"] = city_daily["aqi_mean"].round(1)
    city_daily["co2_mean"] = city_daily["co2_mean"].round(1)
    return city_daily

def compute_kpis(enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Compute KPIs by city ordered by date:
    - 7d rolling average AQI
    - Day-over-day AQI delta
    - 7d rolling CO2 delta (current 7d mean - previous 7d mean)
    - 7d rolling renewable_pct delta
    - AQI category
    """
    df = enriched.sort_values(["city", "date"]).copy()
    df["aqi_7d_avg"] = (
        df.groupby("city")["aqi_mean"]
          .transform(lambda s: s.rolling(7, min_periods=1).mean())
          .round(1)
    )
    df["aqi_dod_delta"] = (
        df.groupby("city")["aqi_mean"].diff().round(1)
    )
    df["co2_7d_avg"] = (
        df.groupby("city")["co2_mean"]
          .transform(lambda s: s.rolling(7, min_periods=1).mean())
          .round(1)
    )
    df["co2_7d_delta"] = df.groupby("city")["co2_7d_avg"].diff().round(1)
    df["renewable_7d_avg"] = (
        df.groupby("city")["renewable_pct"]
          .transform(lambda s: s.rolling(7, min_periods=1).mean())
          .round(1)
    )
    df["renewable_7d_delta"] = df.groupby("city")["renewable_7d_avg"].diff().round(1)
    df["aqi_category"] = df["aqi_mean"].apply(categorize_aqi)
    return df

def join_all(city_daily: pd.DataFrame, energy: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    """
    Join city_daily (AQI/CO2) + energy (renewable_pct) + weather (Open-Meteo).
    city_daily.date is a date; weather.date may be string ISO -> parse here.
    """
    w = weather.copy()
    w["date"] = pd.to_datetime(w["date"]).dt.date
    e = energy.copy()
    e["date"] = pd.to_datetime(e["date"]).dt.date
    base = city_daily.merge(e, on=["city", "date"], how="left")
    out = base.merge(w, on=["city", "date"], how="left")
    return out
