from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
from datetime import date as dt_date, timedelta
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

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

tenors = ["1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"]

ALL_COUNTRIES = ["united_states", "united_kingdom", "germany", "france"]

us_historical_series = {
    "1M": "DGS1MO",
    "3M": "DGS3MO",
    "6M": "DGS6MO",
    "1Y": "DGS1",
    "2Y": "DGS2",
    "5Y": "DGS5",
    "10Y": "DGS10",
    "30Y": "DGS30"
}


TODAY_BLOOMBERG_CACHE = {}   # Bloomberg + FinanceFlow fallback
TODAY_API_CACHE = {}         # FinanceFlow only (toggle forced)


@app.get("/")
def root():
    return FileResponse("frontend/index.html")


@app.get("/debug/scrape")
def debug_scrape():
    """Scrape all three Bloomberg pages in one session and return raw results."""
    result = scrape_bloomberg_batch(["united_states", "united_kingdom", "germany"])
    return result


# ---------------- helpers ----------------


def date_range(start_date, end_date):
    start = dt_date.fromisoformat(start_date)
    end = dt_date.fromisoformat(end_date)
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def resolve_countries(country, selected):
    if selected:
        return [c.strip() for c in selected.split(",") if c.strip() in ALL_COUNTRIES]
    if country == "all":
        return ALL_COUNTRIES
    if country in ALL_COUNTRIES:
        return [country]
    return []


# ---------------- FRED ----------------

def fetch_us_historical(tenor, start=None, end=None):
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
            out.setdefault(o["date"], {})[tenor] = {
                "value": float(o["value"]),
                "source": "FRED API"
            }

    return out


# ---------------- FinanceFlow ----------------

def fetch_financeflow(country, tenor):
    url = "https://financeflowapi.com/api/v1/bonds-spot"

    params = {
        "api_key": FINANCEFLOW_API_KEY,
        "country": country,
        "type": tenor.lower()
    }

    try:
        r = requests.get(url, params=params, timeout=5).json()
        return {
            "value": float(r["data"][0]["bond_yield"]),
            "source": "FinanceFlowAPI"
        }
    except:
        return None


# ---------------- Bloomberg Scrapers ----------------

BLOOMBERG_URLS = {
    "united_states": "https://www.bloomberg.com/markets/rates-bonds/government-bonds/us",
    "united_kingdom": "https://www.bloomberg.com/markets/rates-bonds/government-bonds/uk",
    "germany":        "https://www.bloomberg.com/markets/rates-bonds/government-bonds/germany",
}

BLOOMBERG_MAPPINGS = {
    "united_states": {
        "3M": "3 month", "6M": "6 month", "1Y": "12 month",
        "2Y": "2 year",  "5Y": "5 year",  "10Y": "10 year", "30Y": "30 year",
    },
    "united_kingdom": {
        "2Y": "2 year", "5Y": "5 year", "10Y": "10 year", "30Y": "30 year",
    },
    "germany": {
        "2Y": "2 year", "5Y": "5 year", "10Y": "10 year", "30Y": "30 year",
    },
}

_JS_EXTRACT = """() => {
    const result = {};
    for (const table of document.querySelectorAll('table')) {
        const headers = Array.from(table.querySelectorAll('thead th'))
            .map(th => th.textContent.trim().toLowerCase());
        const yieldCol = headers.findIndex(h => h.includes('yield'));
        if (yieldCol === -1) continue;
        for (const row of table.querySelectorAll('tbody tr')) {
            const cells = Array.from(row.querySelectorAll('td'));
            if (cells.length <= yieldCol) continue;
            const name = cells[0].textContent.trim().toLowerCase();
            const val = parseFloat(
                cells[yieldCol].textContent.trim().replace('%','').replace('+','')
            );
            if (name && !isNaN(val)) result[name] = val;
        }
    }
    return result;
}"""


def scrape_bloomberg_batch(countries):
    """
    Scrape Bloomberg for all requested countries in a single browser session,
    pausing between page loads to avoid bot detection.
    Returns {country: {tenor: value}}.
    """
    import traceback
    results = {c: {} for c in countries}
    targets = [c for c in countries if c in BLOOMBERG_URLS]
    if not targets:
        return results
    try:
        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            page = browser.new_page()
            try:
                for i, country in enumerate(targets):
                    if i > 0:
                        page.wait_for_timeout(3000)
                    page.goto(BLOOMBERG_URLS[country], wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(5000)
                    raw = page.evaluate(_JS_EXTRACT)
                    mapping = BLOOMBERG_MAPPINGS[country]
                    yields = {}
                    for tenor, label in mapping.items():
                        for name, val in raw.items():
                            if label in name:
                                yields[tenor] = val
                                break
                    results[country] = yields
            finally:
                browser.close()
    except Exception:
        print(f"scrape_bloomberg_batch error:\n{traceback.format_exc()}")
    return results


def ensure_bloomberg_cached(countries):
    """Populate TODAY_BLOOMBERG_CACHE for any countries not yet scraped."""
    needed = [c for c in countries if c not in TODAY_BLOOMBERG_CACHE]
    if not needed:
        return
    batch = scrape_bloomberg_batch(needed)
    for country, scraped in batch.items():
        vals = {
            t: {
                "value": scraped.get(t),
                "source": "Bloomberg Rates"
            } if scraped.get(t) is not None else None
            for t in tenors
        }
        if any(v is not None for v in vals.values()):
            TODAY_BLOOMBERG_CACHE[country] = vals


# ---------------- Today's yields (Bloomberg default, FinanceFlow fallback/override) ----------------

def get_today_yields(country, use_api=False):
    """
    use_api=False (default, Bloomberg toggle ON):
        Scrape Bloomberg; fall back to FinanceFlow for any missing tenors.
    use_api=True (FinanceFlow toggle ON):
        Call FinanceFlow directly, skip Bloomberg.
    Results are cached per-source for the lifetime of the server process.
    """
    if use_api:
        if country not in TODAY_API_CACHE:
            TODAY_API_CACHE[country] = {
                t: fetch_financeflow(country, t)
                for t in tenors
            }
        return TODAY_API_CACHE[country]

    if country not in TODAY_BLOOMBERG_CACHE:
        # Single-country fallback (e.g. called outside a batch context)
        ensure_bloomberg_cached([country])

    return TODAY_BLOOMBERG_CACHE.get(country, {})


# ---------------- endpoint ----------------

@app.get("/yield-curve/{country}")
def get_yield_curve(
        country: str,
        date: str = Query(None),
        start: str = Query(None),
        end: str = Query(None),
        view: str = Query(None),
        selected_countries: str = Query(None),
        force_scrape: bool = Query(False)
):
    country = country.lower()

    today_str = dt_date.today().isoformat()

    target = resolve_countries(country, selected_countries)

    if not target:
        return {"error": "No valid countries specified"}

    # Pre-scrape all needed countries in one browser session before any loops
    today_in_range = (
        (date and date == today_str) or
        (start and end and start <= today_str <= end)
    )
    if force_scrape and today_in_range:
        ensure_bloomberg_cached(target)

    # -------- country mode range --------

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

                if d == today_str:
                    today_yields = get_today_yields(c, use_api=not force_scrape)

                for t in tenors:

                    if d == today_str:
                        val = today_yields.get(t)
                        row[t] = val["value"] if val else None
                        row[f"{t}_source"] = val["source"] if val else None
                    elif c == "united_states":
                        val = us_cache.get(d, {}).get(t)
                        row[t] = val["value"] if val else None
                        row[f"{t}_source"] = val["source"] if val else None
                    else:
                        row[t] = None

                result.append(row)

        return result

    # -------- single date --------

    if date:

        result = []

        for c in target:

            yields = {}

            today_yields = get_today_yields(c, use_api=not force_scrape) if date == today_str else None

            for t in tenors:

                if date == today_str:
                    yields[t] = today_yields.get(t)
                else:

                    if c == "united_states":

                        val = fetch_us_historical(t, start=date, end=date)

                        yields[t] = list(val.values())[0][t] if val else None

                    else:

                        yields[t] = None

            result.append({"date": date, "country": c, **yields})

        return result if len(result) > 1 else result[0]

    # -------- tenor mode --------

    if start and end:

        all_dates = date_range(start, end)

        non_today = [d for d in all_dates if d != today_str]

        us_cache = {}

        if non_today and "united_states" in target:

            rs, re = min(non_today), max(non_today)

            for t in tenors:

                for d, vals in fetch_us_historical(t, start=rs, end=re).items():
                    us_cache.setdefault(d, {}).update(vals)

        result = []

        for c in target:

            country_data = {}
            today_yields = None

            for d in all_dates:

                country_data[d] = {}

                if d == today_str and today_yields is None:
                    today_yields = get_today_yields(c, use_api=not force_scrape)

                for t in tenors:

                    if d == today_str:
                        country_data[d][t] = today_yields.get(t)

                    elif c == "united_states":
                        country_data[d][t] = us_cache.get(d, {}).get(t)

                    else:
                        country_data[d][t] = None

            result.append({"country": c, "data": country_data})

        return result if len(result) > 1 else result[0]["data"]

    return {"error": "Must provide either date or start/end"}
