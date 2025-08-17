# EcoAnalytics Solutions — Week 6: End-to-End Automation

**Goal:** Build a production-style Python pipeline that pulls external climate/sustainability metrics, joins with a local database, calculates KPIs, and stores enriched output.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/generate_fake_data.py                # creates data/env.db with realistic tables
python src/main.py --cities "San Francisco,Los Angeles,Sacramento" --start 2025-07-01 --end 2025-08-14
# Offline fallback (no API calls):
python src/main.py --offline --cities "San Francisco,Los Angeles,Sacramento" --start 2025-07-01 --end 2025-08-14
```

**Outputs**
- Transformed file: `data/final_enriched.parquet`
- Logs (JSON): `logs/app.log` (rotates automatically)
- Diagram: `diagrams/workflow.png`
- Slides: `slides/Week6_Slides.pptx` (also `slides/slides.md` with notes)


```

## Notes
- External API used: **Open‑Meteo** (no API key). Weather metrics are joined to local air quality & CO₂ tables.
- If the API is unavailable or you use `--offline`, synthetic but realistic API-like data are generated to keep the pipeline working.
- Tested on Python 3.11+.
