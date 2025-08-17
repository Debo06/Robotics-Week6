import os, random, sqlite3, datetime as dt
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

os.makedirs("data", exist_ok=True)
con = sqlite3.connect("data/env.db")

# -- Cities & stations --
cities = [
    ("San Francisco", 37.7749, -122.4194),
    ("Los Angeles",   34.0522, -118.2437),
    ("Sacramento",    38.5816, -121.4944),
]
stations = []
for city, lat, lon in cities:
    for i in range(3):  # 3 stations per city
        stations.append({
            "station_id": fake.bothify("ST###"),
            "city": city,
            "lat": lat + random.uniform(-0.05, 0.05),
            "lon": lon + random.uniform(-0.05, 0.05),
        })
station_meta = pd.DataFrame(stations).drop_duplicates(subset=["station_id"])
station_meta.to_sql("station_meta", con, if_exists="replace", index=False)

# -- Dates --
end = dt.date.today()
start = end - dt.timedelta(days=45)
dates = pd.date_range(start, end, freq="D")

# -- Air quality readings per station per day --
rows = []
for _, row in station_meta.iterrows():
    for d in dates:
        base_aqi = {
            "San Francisco": 55,
            "Los Angeles": 70,
            "Sacramento": 65,
        }[row["city"]]
        aqi = max(10, int(random.gauss(mu=base_aqi, sigma=15)))
        co2 = round(random.uniform(380, 460), 1)
        rows.append({
            "station_id": row["station_id"],
            "date": d.date().isoformat(),
            "aqi": aqi,
            "co2_ppm": co2,
        })
air_quality = pd.DataFrame(rows)
air_quality.to_sql("air_quality", con, if_exists="replace", index=False)

# -- Energy (renewable %) per city/day --
energy_rows = []
for city, _, _ in cities:
    trend = random.uniform(-0.2, 0.5)
    base = {"San Francisco": 48, "Los Angeles": 36, "Sacramento": 42}[city]
    for i, d in enumerate(dates):
        val = max(5, min(95, base + trend*i + random.uniform(-2, 2)))
        energy_rows.append({"city": city, "date": d.date().isoformat(), "renewable_pct": round(val, 1)})
energy = pd.DataFrame(energy_rows)
energy.to_sql("energy", con, if_exists="replace", index=False)

con.close()
print("✅ synthetic database saved to data/env.db with tables: station_meta, air_quality, energy")
