import pandas as pd
from transform import compute_kpis

def test_compute_kpis_rolling_aqi():
    data = pd.DataFrame({
        "city": ["X"]*10,
        "date": pd.date_range("2025-01-01", periods=10, freq="D").date,
        "aqi_mean": [50,60,70,80,90,100,110,120,130,140],
        "co2_mean": [400,401,402,403,404,405,406,407,408,409],
        "renewable_pct": [30,31,32,33,34,35,36,37,38,39],
        "t2m_mean": [15]*10,
        "rh_mean": [60]*10,
        "wind_mean": [4]*10,
    })
    out = compute_kpis(data)
    # 7-day average at day 7 (index 6): mean of first 7 entries 50..110 => 80.0
    assert round(out.loc[6, "aqi_7d_avg"], 1) == 80.0
    # day-over-day delta at day 3 (index 2): 70 - 60 = 10
    assert round(out.loc[2, "aqi_dod_delta"], 1) == 10.0
