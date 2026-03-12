from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
from datetime import date as dt_date, timedelta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FRED_API_KEY = "9b2b7e3a9562ed8ae4e42db9845281f6"
FINANCEFLOW_API_KEY = "5b9ee5a230d12172b4e22ab5231f964c369470068d71e1a32681f19ca97a24de"

tenors = ["1M","3M","6M","1Y","2Y","5Y","10Y","30Y"]

us_historical_series = {
    "1M": "DGS1MO", "3M": "DGS3MO", "6M": "DGS6MO",
    "1Y": "DGS1", "2Y": "DGS2", "5Y": "DGS5", "10Y": "DGS10", "30Y": "DGS30"
}

ALL_COUNTRIES = ["united_states", "united_kingdom", "germany", "france"]

@app.get("/")
def root():
    return FileResponse("frontend/index.html")

# ---------------- Helpers ----------------

def fetch_us_historical(tenor, start=None, end=None):
    """Return dict date -> tenor -> value"""
    code = us_historical_series[tenor]
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={code}&api_key={FRED_API_KEY}&file_type=json"
    )
    if start and end:
        url += f"&observation_start={start}&observation_end={end}"
    data = requests.get(url).json()
    obs = data.get("observations", [])
    out = {}
    for o in obs:
        if o["value"] != ".":
            out.setdefault(o["date"], {})[tenor] = float(o["value"])
    return out

def fetch_financeflow(country, tenor):
    url = "https://financeflowapi.com/api/v1/bonds-spot"
    params = {"api_key": FINANCEFLOW_API_KEY, "country": country, "type": tenor.lower()}
    resp = requests.get(url, params=params).json()
    try:
        return float(resp["data"][0]["bond_yield"])
    except (KeyError, IndexError, TypeError):
        return None

@app.get("/debug/financeflow")
def debug_financeflow(country: str, tenor: str = "10Y"):
    url = "https://financeflowapi.com/api/v1/bonds-spot"
    params = {"api_key": FINANCEFLOW_API_KEY, "country": country, "type": tenor.lower()}
    return requests.get(url, params=params).json()

def date_range(start_date, end_date):
    start = dt_date.fromisoformat(start_date)
    end = dt_date.fromisoformat(end_date)
    return [(start + timedelta(days=i)).isoformat() for i in range((end-start).days + 1)]

def resolve_countries(country: str, selected_countries: str):
    """Return list of countries to process from either the path param or comma-separated query param."""
    if selected_countries:
        return [c.strip() for c in selected_countries.split(",") if c.strip() in ALL_COUNTRIES]
    if country == "all":
        return ALL_COUNTRIES
    if country in ALL_COUNTRIES:
        return [country]
    return []

# ---------------- Endpoint ----------------

@app.get("/yield-curve/{country}")
def get_yield_curve(
    country: str,
    date: str = Query(None),
    start: str = Query(None),
    end: str = Query(None),
    view: str = Query(None),
    selected_countries: str = Query(None),
):
    country = country.lower()

    if country != "all" and country not in ALL_COUNTRIES and not selected_countries:
        return {"error": "Country not supported"}

    today_str = dt_date.today().isoformat()
    target = resolve_countries(country, selected_countries)

    if not target:
        return {"error": "No valid countries specified"}

    # ---------------- COUNTRY MODE (date range) ----------------
    if view == "country" and start and end:
        all_dates = date_range(start, end)
        non_today = [d for d in all_dates if d != today_str]
        us_cache = {}
        if non_today and "united_states" in target:
            rs, re = min(non_today), max(non_today)
            for t in tenors:
                for d, vals in fetch_us_historical(t, start=rs, end=re).items():
                    us_cache.setdefault(d, {}).update(vals)
        result = []
        for d in all_dates:
            for c in target:
                row = {"date": d, "country": c}
                for t in tenors:
                    if d == today_str:
                        row[t] = fetch_financeflow(c, t)
                    elif c == "united_states":
                        row[t] = us_cache.get(d, {}).get(t)
                    else:
                        row[t] = None
                result.append(row)
        return result

    # ---------------- COUNTRY MODE (single date) ----------------
    if date:
        result = []
        for c in target:
            yields = {}
            for t in tenors:
                if date == today_str:
                    yields[t] = fetch_financeflow(c, t)
                else:
                    if c == "united_states":
                        val = fetch_us_historical(t, start=date, end=date)
                        yields[t] = list(val.values())[0][t] if val else None
                    else:
                        yields[t] = None
            result.append({"date": date, "country": c, **yields})
        return result if len(result) > 1 else result[0]

    # ---------------- TENOR MODE ----------------
    elif start and end:
        all_dates = date_range(start, end)
        non_today_dates = [d for d in all_dates if d != today_str]

        us_cache = {}
        if non_today_dates and "united_states" in target:
            rs, re = min(non_today_dates), max(non_today_dates)
            for t in tenors:
                for d, vals in fetch_us_historical(t, start=rs, end=re).items():
                    us_cache.setdefault(d, {}).update(vals)

        result = []
        for c in target:
            country_data = {}
            for d in all_dates:
                country_data[d] = {}
                for t in tenors:
                    if d == today_str:
                        country_data[d][t] = fetch_financeflow(c, t)
                    elif c == "united_states":
                        country_data[d][t] = us_cache.get(d, {}).get(t)
                    else:
                        country_data[d][t] = None
            result.append({"country": c, "data": country_data})
        return result if len(result) > 1 else result[0]["data"]

    else:
        return {"error": "Must provide either date (country mode) or start & end (tenor mode)"}
