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


@app.get("/debug/scrape/{country}")
def debug_scrape(country: str = "united_states"):
    """Diagnose Bloomberg scraping. country = united_states | united_kingdom | germany"""
    import traceback
    url_map = {
        "united_states": "https://www.bloomberg.com/markets/rates-bonds/government-bonds/us",
        "united_kingdom": "https://www.bloomberg.com/markets/rates-bonds/government-bonds/uk",
        "germany":        "https://www.bloomberg.com/markets/rates-bonds/government-bonds/germany",
    }
    url = url_map.get(country)
    if not url:
        return {"error": "unknown country"}
    try:
        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            page = browser.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(5000)
                title = page.title()
                raw = page.evaluate("""() => {
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
                                cells[yieldCol].textContent.trim()
                                    .replace('%','').replace('+','')
                            );
                            if (name && !isNaN(val)) result[name] = val;
                        }
                    }
                    return result;
                }""")
            finally:
                browser.close()
        return {"title": title, "rows": raw}
    except Exception:
        return {"error": traceback.format_exc()}


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
            out.setdefault(o["date"], {})[tenor] = float(o["value"])

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
        return float(r["data"][0]["bond_yield"])
    except:
        return None


# ---------------- Bloomberg Scrapers ----------------

def parse_bloomberg(url, mapping):
    import traceback
    try:
        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            page = browser.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(5000)

                raw = page.evaluate("""() => {
                    const result = {};
                    for (const table of document.querySelectorAll('table')) {
                        const headers = Array.from(
                            table.querySelectorAll('thead th')
                        ).map(th => th.textContent.trim().toLowerCase());
                        const yieldCol = headers.findIndex(h => h.includes('yield'));
                        if (yieldCol === -1) continue;
                        for (const row of table.querySelectorAll('tbody tr')) {
                            const cells = Array.from(row.querySelectorAll('td'));
                            if (cells.length <= yieldCol) continue;
                            const name = cells[0].textContent.trim().toLowerCase();
                            const val = parseFloat(
                                cells[yieldCol].textContent.trim()
                                    .replace('%', '').replace('+', '')
                            );
                            if (name && !isNaN(val)) result[name] = val;
                        }
                    }
                    return result;
                }""")
            finally:
                browser.close()

        yields = {}
        for tenor, label in mapping.items():
            label_lower = label.lower()
            for name, val in raw.items():
                if label_lower in name:
                    yields[tenor] = val
                    break
        return yields

    except Exception:
        print(f"parse_bloomberg error for {url}:\n{traceback.format_exc()}")
        return {}


def scrape_us():
    mapping = {
        "3M":  "3 month",
        "6M":  "6 month",
        "1Y":  "12 month",
        "2Y":  "2 year",
        "5Y":  "5 year",
        "10Y": "10 year",
        "30Y": "30 year",
    }
    return parse_bloomberg(
        "https://www.bloomberg.com/markets/rates-bonds/government-bonds/us",
        mapping
    )


def scrape_uk():
    mapping = {
        "2Y":  "2 year",
        "5Y":  "5 year",
        "10Y": "10 year",
        "30Y": "30 year",
    }
    return parse_bloomberg(
        "https://www.bloomberg.com/markets/rates-bonds/government-bonds/uk",
        mapping
    )


def scrape_germany():
    mapping = {
        "2Y":  "2 year",
        "5Y":  "5 year",
        "10Y": "10 year",
        "30Y": "30 year",
    }
    return parse_bloomberg(
        "https://www.bloomberg.com/markets/rates-bonds/government-bonds/germany",
        mapping
    )


# ---------------- Bloomberg scrapers by country ----------------

def fetch_scraped_only(country):
    if country == "united_states":
        return scrape_us()
    if country == "united_kingdom":
        return scrape_uk()
    if country == "germany":
        return scrape_germany()
    return {}


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
            TODAY_API_CACHE[country] = {t: fetch_financeflow(country, t) for t in tenors}
        return TODAY_API_CACHE[country]

    if country not in TODAY_BLOOMBERG_CACHE:
        scraped = fetch_scraped_only(country)
        vals = {t: scraped.get(t) for t in tenors}
        if any(v is not None for v in vals.values()):
            TODAY_BLOOMBERG_CACHE[country] = vals
        else:
            return vals  # don't cache failures — retry on next request

    return TODAY_BLOOMBERG_CACHE[country]


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
                        row[t] = today_yields.get(t)
                    elif c == "united_states":
                        row[t] = us_cache.get(d, {}).get(t)
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
