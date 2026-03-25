from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from fastapi import FastAPI, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
import json
import os
import threading
import re
import time
from xml.etree import ElementTree as ET
from datetime import date as dt_date, timedelta, datetime
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth


def _prewarm():
    """Populate both caches for all countries at startup in the background."""
    ensure_bloomberg_cached(ALL_COUNTRIES)
    for country in ALL_COUNTRIES:
        _populate_financeflow_api_cache(country)
    # Mirror what the EOD scheduler does: if today's FinanceFlow historical data
    # is missing (e.g. after a mid-day deploy), fetch and persist it now.
    today_str = dt_date.today().isoformat()
    for country in ALL_COUNTRIES:
        if today_str not in FINANCEFLOW_HISTORICAL_CACHE.get(country, {}):
            try:
                _fetch_and_store_financeflow_eod_country(country)
            except Exception as e:
                print(f"_prewarm: FinanceFlow historical catch-up failed for {country}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=_prewarm, daemon=True).start()
    threading.Thread(target=_financeflow_eod_scheduler, daemon=True).start()
    threading.Thread(target=_bloomberg_background_scheduler, daemon=True).start()
    threading.Thread(target=_new_issues_corps_scheduler, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)

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


TODAY_BLOOMBERG_CACHE = {}   # Bloomberg Data Scraped - default toggle on
TODAY_FINANCEFLOWAPI_CACHE = {}         # FinanceFlow only (toggle forced)
_bloomberg_cache_date = None  # Tracks which calendar date TODAY_BLOOMBERG_CACHE was populated for
_bloomberg_last_scrape_time = {}  # {country: datetime} last scrape attempt, for fallback retry TTL
BLOOMBERG_RETRY_TTL_SECONDS = 1800  # retry fallback-only countries every 30 min
_bloomberg_scrape_rotation = 0     # increments each request-driven background scrape to rotate country order
_bloomberg_bg_rotation = 1         # starts at 1 so first scheduler run follows on from prewarm (which always scrapes [US, UK, GER])
BLOOMBERG_BG_SCHEDULER_INTERVAL = 1 * 60 * 60  # scrape all Bloomberg countries every 1 hour
_financeflow_api_cache_date = None    # Tracks which calendar date the FinanceFlow cache was last refreshed
_financeflow_last_refresh_time = {}   # {country: datetime} last refresh time, for intraday TTL
FINANCEFLOW_RETRY_TTL_SECONDS = 1800  # refresh FinanceFlow data every 30 min intraday

_DATA_DIR = os.environ.get("CACHE_DIR", "/data")
os.makedirs(_DATA_DIR, exist_ok=True)

# On first deploy the volume is empty — seed from any JSON files baked into the image
for _seed_file in ["bbg_last_good_cache.json", "bbg_historical_cache.json", "financeflow_historical_cache.json", "eco_indicators_cache.json"]:
    _dest = os.path.join(_DATA_DIR, _seed_file)
    if not os.path.exists(_dest) and os.path.exists(_seed_file):
        import shutil
        shutil.copy2(_seed_file, _dest)
        print(f"Seeded {_dest} from baked-in {_seed_file}")

BBG_LAST_GOOD_CACHE_FILE = os.path.join(_DATA_DIR, "bbg_last_good_cache.json") #if we dont have live scraped data lets display the last good result from bbg that we stored
BBG_HISTORICAL_CACHE_FILE = os.path.join(_DATA_DIR, "bbg_historical_cache.json") #store each days BBG live scraped data in a cache file so we can use it as a historical data repo display - clever isnt it? useful for uk, germany (us at least has FRED)
FINANCEFLOW_HISTORICAL_CACHE_FILE = os.path.join(_DATA_DIR, "financeflow_historical_cache.json") #store each days FinanceFlowAPI called data in a cache file so we can use it as a historical data repo display for us, uk, ger and fra! clever isnt it?
#there are debug endpoints below for us to validate that we received data

def _load_bbg_last_good_cache():
    if os.path.exists(BBG_LAST_GOOD_CACHE_FILE):
        try:
            with open(BBG_LAST_GOOD_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_bbg_last_good_cache(cache): #if we dont have live scraped data lets extract the last good result from bbg that we stored
    try:
        with open(BBG_LAST_GOOD_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass


def _load_bbg_historical_cache():
    if os.path.exists(BBG_HISTORICAL_CACHE_FILE):
        try:
            with open(BBG_HISTORICAL_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_bbg_historical_cache(cache):
    try:
        with open(BBG_HISTORICAL_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass


def _load_financeflow_historical_cache():
    if os.path.exists(FINANCEFLOW_HISTORICAL_CACHE_FILE):
        try:
            with open(FINANCEFLOW_HISTORICAL_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_financeflow_historical_cache(cache):
    try:
        with open(FINANCEFLOW_HISTORICAL_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass


BLOOMBERG_LAST_GOOD_CACHE = _load_bbg_last_good_cache()  # {country: {tenor: {value, timestamp}}}
BBG_HISTORICAL_YIELDS_CACHE = _load_bbg_historical_cache()   # {country: {date: {tenor: {value, source}}}}
FINANCEFLOW_HISTORICAL_CACHE = _load_financeflow_historical_cache()  # {country: {date: {tenor: {value, source}}}}


def _seed_bbg_historical_from_last_good():
    """
    Seed the historical cache from bloomberg_last_good.json on startup.
    Each entry in last_good carries a timestamp — extract the date from it and
    populate BBG_HISTORICAL_YIELDS_CACHE so we always have the most recently scraped
    data available for historical queries even if historical_yields_cache.json
    was freshly initialised or deleted.
    """
    updated = False
    for country, tenors_data in BLOOMBERG_LAST_GOOD_CACHE.items():
        for tenor, entry in tenors_data.items():
            if "timestamp" not in entry or "value" not in entry:
                continue
            date_str = entry["timestamp"][:10]  # YYYY-MM-DD
            # Don't overwrite if we already have data for this date/tenor
            if not BBG_HISTORICAL_YIELDS_CACHE.get(country, {}).get(date_str, {}).get(tenor):
                BBG_HISTORICAL_YIELDS_CACHE.setdefault(country, {}).setdefault(date_str, {})[tenor] = {
                    "value": entry["value"],
                    "source": "BBG Cache",
                }
                updated = True
    if updated:
        _save_bbg_historical_cache(BBG_HISTORICAL_YIELDS_CACHE)


_seed_bbg_historical_from_last_good()


@app.get("/")
def root():
    return FileResponse("frontend/index.html", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/debug/bbg-scrape")
def debug_scrape():
    """Scrape all three Bloomberg pages in one session and return raw results."""
    result = scrape_bloomberg_batch(["united_states", "united_kingdom", "germany"])
    return result


@app.get("/debug/bloomberg-cache")
def debug_bloomberg_cache():
    """Return TODAY_BLOOMBERG_CACHE: which countries are cached today and what source each tenor came from."""
    summary = {}
    for country, tenor_data in TODAY_BLOOMBERG_CACHE.items():
        summary[country] = {
            t: {"value": v["value"], "source": v["source"]} if v else None
            for t, v in tenor_data.items()
        }
    return {
        "cache_date": _bloomberg_cache_date,
        "today": dt_date.today().isoformat(),
        "data": summary,
    }


@app.get("/debug/bloomberg-last-good")
def debug_bloomberg_last_good():
    """Return the persisted last-good Bloomberg values with their timestamps."""
    return BLOOMBERG_LAST_GOOD_CACHE


@app.get("/debug/financeflow-cache")
def debug_financeflow_cache():
    """Return the in-memory FinanceFlow historical cache."""
    return FINANCEFLOW_HISTORICAL_CACHE


@app.get("/debug/financeflow-live")
def debug_financeflow_live():
    """Fire live FinanceFlow requests for all countries/tenors right now (no caching) to verify the API is responding."""
    results = {}
    with ThreadPoolExecutor(max_workers=len(ALL_COUNTRIES) * len(tenors)) as pool:
        futures = {
            pool.submit(fetch_financeflow, country, t): (country, t)
            for country in ALL_COUNTRIES
            for t in tenors
        }
        for future in as_completed(futures):
            country, t = futures[future]
            result = future.result()
            results.setdefault(country, {})[t] = result
    return results


@app.get("/debug/financeflow-eod-schedule")
def debug_eod_schedule():
    """Show EOD schedule, current UTC time, and whether each country's data for today is already captured."""
    now = datetime.utcnow()
    today = now.date().isoformat()
    schedule = {}
    for country, (h, m) in FINANCEFLOW_EOD_SCHEDULE_UTC.items():
        schedule[country] = {
            "scheduled_utc": f"{h:02d}:{m:02d}",
            "past_scheduled_time": (now.hour, now.minute) >= (h, m),
            "today_captured": today in FINANCEFLOW_HISTORICAL_CACHE.get(country, {}),
            "tenors_captured": list(FINANCEFLOW_HISTORICAL_CACHE.get(country, {}).get(today, {}).keys()),
        }
    return {
        "current_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "today": today,
        "schedule": schedule,
    }


@app.get("/debug/status")
def debug_status():
    """Single health overview: Bloomberg cache state, FinanceFlow EOD coverage, and API cache state."""
    now = datetime.utcnow()
    today = now.date().isoformat()

    bloomberg = {}
    for country in ALL_COUNTRIES:
        tenor_data = TODAY_BLOOMBERG_CACHE.get(country, {})
        live = [t for t, v in tenor_data.items() if v and v.get("source") == "Bloomberg Rates"]
        fallback = [t for t, v in tenor_data.items() if v and v.get("source", "").startswith("Bloomberg (Last")]
        missing = [t for t in tenors if not tenor_data.get(t)]
        bloomberg[country] = {
            "in_today_cache": country in TODAY_BLOOMBERG_CACHE,
            "live_tenors": live,
            "fallback_tenors": fallback,
            "missing_tenors": missing,
        }

    financeflow_eod = {}
    for country, (h, m) in FINANCEFLOW_EOD_SCHEDULE_UTC.items():
        today_data = FINANCEFLOW_HISTORICAL_CACHE.get(country, {}).get(today, {})
        financeflow_eod[country] = {
            "scheduled_utc": f"{h:02d}:{m:02d}",
            "past_scheduled_time": (now.hour, now.minute) >= (h, m),
            "today_captured": bool(today_data),
            "tenors_captured": sorted(today_data.keys()),
            "tenors_missing": [t for t in tenors if t not in today_data],
        }

    return {
        "current_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "today": today,
        "bloomberg_cache_date": _bloomberg_cache_date,
        "bloomberg": bloomberg,
        "financeflow_eod": financeflow_eod,
        "today_api_cache_countries": list(TODAY_FINANCEFLOWAPI_CACHE.keys()),
    }


@app.get("/debug/migrate-cache-to-volume")
def debug_migrate_cache_to_volume():
    """One-time migration: copy cache files from working directory into the persistent volume at _DATA_DIR."""
    import shutil
    results = {}
    for fname in ["bbg_last_good_cache.json", "bbg_historical_cache.json", "financeflow_historical_cache.json"]:
        src = fname  # working directory (old location)
        dst = os.path.join(_DATA_DIR, fname)
        if src == dst:
            results[fname] = "skipped — source and destination are the same"
        elif os.path.exists(src) and not os.path.exists(dst):
            shutil.copy2(src, dst)
            results[fname] = f"copied to {dst}"
        elif os.path.exists(dst):
            results[fname] = f"already exists at {dst}, not overwritten"
        else:
            results[fname] = "source file not found in working directory"
    return results


@app.get("/debug/financeflow-eod")
def debug_financeflow_eod():
    """Manually trigger the FinanceFlow EOD fetch for all countries."""
    def _run_all():
        for country in ALL_COUNTRIES:
            _fetch_and_store_financeflow_eod_country(country)
    threading.Thread(target=_run_all, daemon=True).start()
    return {"status": "triggered", "message": "FinanceFlow EOD fetch started in background for all countries"}


class FinanceFlowCachePatch(BaseModel):
    country: str
    date: str
    tenors: dict  # e.g. {"2Y": 3.78, "5Y": 3.88, "10Y": 4.265, "30Y": 4.89}

@app.post("/debug/financeflow-cache-patch")
def debug_financeflow_cache_patch(patch: FinanceFlowCachePatch):
    """Manually inject tenor values into the FinanceFlow historical cache (writes to volume)."""
    for tenor, value in patch.tenors.items():
        FINANCEFLOW_HISTORICAL_CACHE.setdefault(patch.country, {}).setdefault(patch.date, {})[tenor] = {
            "value": value,
            "source": "FinanceFlow Cache",
        }
    _save_financeflow_historical_cache(FINANCEFLOW_HISTORICAL_CACHE)
    return {"status": "ok", "country": patch.country, "date": patch.date, "tenors": patch.tenors}


@app.get("/debug/patch-us-march-2026")
def debug_patch_us_march_2026():
    """One-off: inject missing US yield data for 2026-03-17 and 2026-03-18."""
    entries = {
        "2026-03-17": {"2Y": 3.68, "5Y": 3.79, "10Y": 4.2, "30Y": 4.85},
        "2026-03-18": {"2Y": 3.78, "5Y": 3.88, "10Y": 4.265, "30Y": 4.89},
    }
    for date, tenors in entries.items():
        for tenor, value in tenors.items():
            FINANCEFLOW_HISTORICAL_CACHE.setdefault("united_states", {}).setdefault(date, {})[tenor] = {
                "value": value,
                "source": "FinanceFlow Cache",
            }
    _save_financeflow_historical_cache(FINANCEFLOW_HISTORICAL_CACHE)
    return {"status": "ok", "patched": entries}


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

def fetch_fred_us_historical(tenor, start=None, end=None):
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

FINANCEFLOW_TENOR_MAP = {"1Y": "12m"}


def fetch_financeflow(country, tenor):
    url = "https://financeflowapi.com/api/v1/bonds-spot"

    params = {
        "api_key": FINANCEFLOW_API_KEY,
        "country": country,
        "type": FINANCEFLOW_TENOR_MAP.get(tenor, tenor.lower())
    }

    try:
        r = requests.get(url, params=params, timeout=5).json()
        return {
            "value": float(r["data"][0]["bond_yield"]),
            "source": "FinanceFlowAPI"
        }
    except:
        return None


def _refresh_financeflow_cache(country):
    """Fetch all tenors for a country from FinanceFlow and update the in-memory cache."""
    with ThreadPoolExecutor(max_workers=len(tenors)) as pool:
        futures = {pool.submit(fetch_financeflow, country, t): t for t in tenors}
        TODAY_FINANCEFLOWAPI_CACHE[country] = {
            futures[f]: f.result() for f in as_completed(futures)
        }


def _populate_financeflow_api_cache(country):
    """Ensure today's FinanceFlow data is cached for the given country.

    Intraday TTL (stale-while-revalidate):
      - No data at all → block until fetched (first startup only).
      - Data exists but TTL expired → return stale immediately, refresh in
        background so the next request gets updated yields.
      - Data exists and within TTL → return immediately, no API calls.
    Timestamp is stamped before spawning the background thread to prevent
    concurrent requests from queuing duplicate fetches.
    """
    global _financeflow_api_cache_date, _financeflow_last_refresh_time
    today = dt_date.today().isoformat()
    if _financeflow_api_cache_date != today:
        _financeflow_api_cache_date = today
        _financeflow_last_refresh_time = {}  # new day — all countries need a refresh
    now = datetime.now()
    last = _financeflow_last_refresh_time.get(country)
    ttl_expired = last is None or (now - last).total_seconds() > FINANCEFLOW_RETRY_TTL_SECONDS
    if not ttl_expired:
        return  # still fresh within TTL
    # Stamp before fetching to prevent concurrent duplicate fetches
    _financeflow_last_refresh_time[country] = now
    if country in TODAY_FINANCEFLOWAPI_CACHE:
        # Stale data available — refresh in background, return immediately
        print(f"_populate_financeflow_api_cache: background refresh for {country}")
        threading.Thread(target=_refresh_financeflow_cache, args=(country,), daemon=True).start()
    else:
        # No data at all — must block
        _refresh_financeflow_cache(country)


# Per-country EOD snapshot times (UTC). UK/GER/FR markets close ~16:00-16:30 UTC,
# US Treasuries close ~22:00 UTC (5pm EST), so we capture US 15 min after close.
FINANCEFLOW_EOD_SCHEDULE_UTC = {
    "united_kingdom": (21,  0),
    "germany":        (21,  5),
    "france":         (21, 10),
    "united_states":  (22, 15),
}


def _fetch_and_store_financeflow_eod_country(country):
    """Fetch all tenors for a single country from FinanceFlow and persist to cache."""
    today_str = dt_date.today().isoformat()
    print(f"_fetch_and_store_financeflow_eod: starting {country} for {today_str}")
    updated = False
    with ThreadPoolExecutor(max_workers=len(tenors)) as pool:
        futures = {pool.submit(fetch_financeflow, country, t): t for t in tenors}
        for future in as_completed(futures):
            tenor = futures[future]
            result = future.result()
            if result is not None:
                FINANCEFLOW_HISTORICAL_CACHE.setdefault(country, {}).setdefault(today_str, {})[tenor] = {
                    "value": result["value"],
                    "source": "FinanceFlow Cache",
                }
                updated = True
    if updated:
        _save_financeflow_historical_cache(FINANCEFLOW_HISTORICAL_CACHE)
    print(f"_fetch_and_store_financeflow_eod: completed {country} for {today_str}, updated={updated}")


def _financeflow_eod_scheduler():
    """Background thread that fires per-country FinanceFlow EOD fetches at their scheduled UTC times.
    Catch-up logic: on startup, if it is already past a country's scheduled time and that country's
    data for today is absent, runs immediately so a server restart never skips a day.
    """
    import time
    last_run_dates = {c: None for c in ALL_COUNTRIES}
    while True:
        now = datetime.utcnow()
        today = now.date().isoformat()
        for country, (sched_hour, sched_min) in FINANCEFLOW_EOD_SCHEDULE_UTC.items():
            already_ran = last_run_dates[country] == today
            if already_ran:
                continue
            past_schedule = (now.hour, now.minute) >= (sched_hour, sched_min)
            if not past_schedule:
                continue
            today_cached = today in FINANCEFLOW_HISTORICAL_CACHE.get(country, {})
            in_window = now.hour == sched_hour and now.minute == sched_min
            if in_window or not today_cached:
                last_run_dates[country] = today
                try:
                    _fetch_and_store_financeflow_eod_country(country)
                except Exception as e:
                    print(f"_financeflow_eod_scheduler error ({country}): {e}")
        time.sleep(30)


# ---------------- Bloomberg Scrapers ----------------

BLOOMBERG_URLS = {
    "united_states": "https://www.bloomberg.com/markets/rates-bonds/government-bonds/us",
    "united_kingdom": "https://www.bloomberg.com/markets/rates-bonds/government-bonds/uk",
    "germany":        "https://www.bloomberg.com/markets/rates-bonds/government-bonds/germany",
}
BLOOMBERG_COUNTRIES = list(BLOOMBERG_URLS.keys())  # fixed rotation order: US, UK, GER

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
            for i, country in enumerate(targets):
                if i > 0:
                    import time; time.sleep(3)
                try:
                    browser = p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-setuid-sandbox"]
                    )
                    page = browser.new_page()
                    try:
                        page.goto(BLOOMBERG_URLS[country], wait_until="domcontentloaded", timeout=60000)
                        page.wait_for_timeout(5000)
                        raw = page.evaluate(_JS_EXTRACT)
                        print(f"scrape_bloomberg_batch: {country} raw={raw}")
                        mapping = BLOOMBERG_MAPPINGS[country]
                        yields = {}
                        for tenor, label in mapping.items():
                            for name, val in raw.items():
                                if label in name:
                                    yields[tenor] = val
                                    break
                        print(f"scrape_bloomberg_batch: {country} yields={yields}")
                        results[country] = yields
                    finally:
                        browser.close()
                except Exception:
                    print(f"scrape_bloomberg_batch: failed for {country}:\n{traceback.format_exc()}")
    except Exception:
        print(f"scrape_bloomberg_batch error:\n{traceback.format_exc()}")
    return results


def _has_live_bloomberg_data(country):
    """Return True if country has at least one live Bloomberg value (not just fallback) today."""
    return any(
        v is not None and v.get("source") == "Bloomberg Rates"
        for v in TODAY_BLOOMBERG_CACHE.get(country, {}).values()
    )


def _process_bloomberg_batch(batch):
    """Merge a scrape_bloomberg_batch result into the caches. Returns (cache_updated, historical_updated)."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_str = dt_date.today().isoformat()
    cache_updated = False
    historical_updated = False
    for country, scraped in batch.items():
        # Persist any fresh values into the last-good cache and historical cache
        for t in tenors:
            v = scraped.get(t)
            if v is not None:
                BLOOMBERG_LAST_GOOD_CACHE.setdefault(country, {})[t] = {
                    "value": v,
                    "timestamp": now,
                }
                cache_updated = True
                # Save to historical cache so non-US countries build up a repository
                BBG_HISTORICAL_YIELDS_CACHE.setdefault(country, {}).setdefault(today_str, {})[t] = {
                    "value": v,
                    "source": "BBG Cache",
                }
                historical_updated = True

        # Build today's cache, falling back to last-good for missing tenors
        vals = {}
        for t in tenors:
            v = scraped.get(t)
            if v is not None:
                vals[t] = {"value": v, "source": "Bloomberg Rates"}
            else:
                last = BLOOMBERG_LAST_GOOD_CACHE.get(country, {}).get(t)
                if last:
                    vals[t] = {
                        "value": last["value"],
                        "source": f"Bloomberg (Last Received: {last['timestamp']})",
                    }
                else:
                    vals[t] = None

        if any(v is not None for v in vals.values()):
            TODAY_BLOOMBERG_CACHE[country] = vals

    return cache_updated, historical_updated


def _do_bloomberg_scrape(countries):
    """Scrape Bloomberg for the given countries and merge results into caches."""
    batch = scrape_bloomberg_batch(countries)
    cu, hu = _process_bloomberg_batch(batch)
    if cu:
        _save_bbg_last_good_cache(BLOOMBERG_LAST_GOOD_CACHE)
    if hu:
        _save_bbg_historical_cache(BBG_HISTORICAL_YIELDS_CACHE)


def _bloomberg_background_scheduler():
    """Background thread that scrapes all Bloomberg countries every 3 hours.
    Rotates country order each run so each country gets the first-position
    advantage on a fixed cycle: [US,UK,GER] → [UK,GER,US] → [GER,US,UK] → repeat.
    Stamps _bloomberg_last_scrape_time before scraping so the request-driven
    retry does not fire a duplicate scrape immediately after the scheduler runs.
    Waits one full interval before the first run since prewarm already scraped at startup.
    """
    import time
    global _bloomberg_bg_rotation
    time.sleep(BLOOMBERG_BG_SCHEDULER_INTERVAL)
    while True:
        n = len(BLOOMBERG_COUNTRIES)
        rotated = BLOOMBERG_COUNTRIES[_bloomberg_bg_rotation % n:] + BLOOMBERG_COUNTRIES[:_bloomberg_bg_rotation % n]
        _bloomberg_bg_rotation += 1
        print(f"_bloomberg_background_scheduler: scraping {rotated}")
        now = datetime.now()
        for c in rotated:
            _bloomberg_last_scrape_time[c] = now
        try:
            _do_bloomberg_scrape(rotated)
        except Exception as e:
            print(f"_bloomberg_background_scheduler error: {e}")
        time.sleep(BLOOMBERG_BG_SCHEDULER_INTERVAL)


def ensure_bloomberg_cached(countries):
    """Populate TODAY_BLOOMBERG_CACHE for any countries not yet scraped.

    Rotates call order across up to len(must_scrape) rounds so each country
    gets a chance to be first in the batch (Bloomberg tends to return data for
    the first caller and block subsequent ones). Short-circuits as soon as
    every country has live data, so performance is unaffected in the happy path.

    Intraday retry (stale-while-revalidate):
      - No cache entry and no last-good data → block until scraped (first ever startup).
      - No cache entry but last-good data exists → pre-populate from last-good,
        then treat as fallback-only and refresh in the background.
      - Fallback-only, TTL expired → return stale data immediately, re-scrape
        in the background so the next request gets fresh data.
      - Live data present → never re-scraped.
    The scrape timestamp is stamped before the scrape starts, so concurrent
    requests for the same country never queue duplicate scrapes.
    """
    global TODAY_BLOOMBERG_CACHE, _bloomberg_cache_date, _bloomberg_last_scrape_time
    today = dt_date.today().isoformat()
    if _bloomberg_cache_date != today:
        _bloomberg_cache_date = today
        _bloomberg_last_scrape_time.clear()
        # Pre-populate from last-good cache so stale data is available immediately.
        # Countries with no last-good entry remain absent and will block (must_scrape).
        new_cache = {}
        for c, tenor_data in BLOOMBERG_LAST_GOOD_CACHE.items():
            vals = {}
            for t in tenors:
                last = tenor_data.get(t)
                if last:
                    vals[t] = {"value": last["value"], "source": f"Bloomberg (Last Received: {last['timestamp']})"}
                else:
                    vals[t] = None
            if any(v is not None for v in vals.values()):
                new_cache[c] = vals
        TODAY_BLOOMBERG_CACHE = new_cache
    now = datetime.now()
    must_scrape = []    # no cache entry — block the request
    background_retry = []  # fallback-only, TTL expired — stale-while-revalidate
    for c in countries:
        if c not in TODAY_BLOOMBERG_CACHE:
            must_scrape.append(c)
        elif not _has_live_bloomberg_data(c):
            last = _bloomberg_last_scrape_time.get(c)
            if last is None or (now - last).total_seconds() > BLOOMBERG_RETRY_TTL_SECONDS:
                background_retry.append(c)
    # Stamp before scraping to prevent concurrent duplicate scrapes
    for c in must_scrape + background_retry:
        _bloomberg_last_scrape_time[c] = now
    # Stale-while-revalidate: return existing fallback data immediately
    if background_retry:
        global _bloomberg_scrape_rotation
        n = len(BLOOMBERG_COUNTRIES)
        rotated_all = BLOOMBERG_COUNTRIES[_bloomberg_scrape_rotation % n:] + BLOOMBERG_COUNTRIES[:_bloomberg_scrape_rotation % n]
        rotated_retry = [c for c in rotated_all if c in background_retry]
        _bloomberg_scrape_rotation += 1
        print(f"ensure_bloomberg_cached: background retry for {rotated_retry}")
        threading.Thread(target=_do_bloomberg_scrape, args=(rotated_retry,), daemon=True).start()
    if not must_scrape:
        return

    cache_updated = False
    historical_updated = False
    n = len(must_scrape)
    for rotation in range(n):
        # Countries still lacking live Bloomberg data, in rotated order
        rotated = must_scrape[rotation:] + must_scrape[:rotation]
        to_scrape = [c for c in rotated if not _has_live_bloomberg_data(c)]
        if not to_scrape:
            break
        print(f"ensure_bloomberg_cached: rotation {rotation + 1}/{n}, scraping {to_scrape}")
        batch = scrape_bloomberg_batch(to_scrape)
        cu, hu = _process_bloomberg_batch(batch)
        cache_updated = cache_updated or cu
        historical_updated = historical_updated or hu

    if cache_updated:
        _save_bbg_last_good_cache(BLOOMBERG_LAST_GOOD_CACHE)
    if historical_updated:
        _save_bbg_historical_cache(BBG_HISTORICAL_YIELDS_CACHE)


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
        _populate_financeflow_api_cache(country)
        return TODAY_FINANCEFLOWAPI_CACHE.get(country, {})

    if country not in TODAY_BLOOMBERG_CACHE:
        # Single-country fallback (e.g. called outside a batch context)
        ensure_bloomberg_cached([country])

    return TODAY_BLOOMBERG_CACHE.get(country, {})


def _get_historical_cache(force_scrape: bool) -> dict:
    """Return the appropriate historical cache based on the active data source toggle.
    Bloomberg mode (force_scrape=True)  → BBG Cache (historical_yields_cache.json)
    FinanceFlow mode (force_scrape=False) → FinanceFlow Cache (financeflow_historical_cache.json)
    """
    return BBG_HISTORICAL_YIELDS_CACHE if force_scrape else FINANCEFLOW_HISTORICAL_CACHE


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

            with ThreadPoolExecutor(max_workers=len(tenors)) as pool:
                for tenor_data in pool.map(partial(fetch_fred_us_historical, start=rs, end=re), tenors):
                    for d, vals in tenor_data.items():
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
                        # FRED had no data for this date — fall back to source-specific historical cache
                        if val is None:
                            val = _get_historical_cache(force_scrape).get(c, {}).get(d, {}).get(t)
                        row[t] = val["value"] if val else None
                        row[f"{t}_source"] = val["source"] if val else None
                    else:
                        val = _get_historical_cache(force_scrape).get(c, {}).get(d, {}).get(t)
                        row[t] = val["value"] if val else None
                        row[f"{t}_source"] = val["source"] if val else None

                result.append(row)

        return result

    # -------- single date --------

    if date:

        result = []

        for c in target:

            yields = {}

            today_yields = get_today_yields(c, use_api=not force_scrape) if date == today_str else None

            if date == today_str:
                yields = {t: today_yields.get(t) for t in tenors}
            elif c == "united_states":
                with ThreadPoolExecutor(max_workers=len(tenors)) as pool:
                    fred_results = pool.map(partial(fetch_fred_us_historical, start=date, end=date), tenors)
                yields = {}
                for t, data in zip(tenors, fred_results):
                    val = list(data.values())[0][t] if data else None
                    # FRED had no data for this date — fall back to source-specific historical cache
                    if val is None:
                        val = _get_historical_cache(force_scrape).get(c, {}).get(date, {}).get(t)
                    yields[t] = val
            else:
                cached = _get_historical_cache(force_scrape).get(c, {}).get(date, {})
                yields = {t: cached.get(t) for t in tenors}

            result.append({"date": date, "country": c, **yields})

        return result if len(result) > 1 else result[0]

    # -------- tenor mode --------

    if start and end:

        all_dates = date_range(start, end)

        non_today = [d for d in all_dates if d != today_str]

        us_cache = {}

        if non_today and "united_states" in target:

            rs, re = min(non_today), max(non_today)

            with ThreadPoolExecutor(max_workers=len(tenors)) as pool:
                for tenor_data in pool.map(partial(fetch_fred_us_historical, start=rs, end=re), tenors):
                    for d, vals in tenor_data.items():
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
                        val = us_cache.get(d, {}).get(t)
                        # FRED had no data for this date — fall back to source-specific historical cache
                        if val is None:
                            val = _get_historical_cache(force_scrape).get(c, {}).get(d, {}).get(t)
                        country_data[d][t] = val

                    else:
                        country_data[d][t] = _get_historical_cache(force_scrape).get(c, {}).get(d, {}).get(t)

            result.append({"country": c, "data": country_data})

        return result if len(result) > 1 else result[0]["data"]

    return {"error": "Must provide either date or start/end"}


# ---------------- Live CoD endpoint ----------------

@app.get("/yield-curve-cod/{country}")
def get_yield_curve_cod(
        country: str,
        selected_countries: str = Query(None),
        force_scrape: bool = Query(False)
):
    """
    Return Change-on-Day data for each tenor:
      - today's yield
      - the most recent prior day's yield (yesterday or last available)
      - CoD in basis points = (today - prev) * 100
    """
    country = country.lower()
    today_str = dt_date.today().isoformat()
    today_date = dt_date.today()

    target = resolve_countries(country, selected_countries)
    if not target:
        return {"error": "No valid countries specified"}

    if force_scrape:
        ensure_bloomberg_cached(target)

    hist_cache = _get_historical_cache(force_scrape)

    results = []
    for c in target:
        today_yields = get_today_yields(c, use_api=not force_scrape)
        country_hist = hist_cache.get(c, {})

        # FRED: check yesterday specifically (US only)
        yesterday = (today_date - timedelta(days=1)).isoformat()
        fred_yesterday = {}  # {tenor: {value, source}}
        if c == "united_states":
            try:
                with ThreadPoolExecutor(max_workers=len(tenors)) as pool:
                    for tenor_data in pool.map(
                        partial(fetch_fred_us_historical, start=yesterday, end=yesterday),
                        tenors
                    ):
                        fred_yesterday.update(tenor_data.get(yesterday, {}))
            except Exception:
                pass

        tenor_results = {}
        prev_dates_used = []

        for t in tenors:
            today_val = today_yields.get(t)
            today_value  = today_val["value"]  if isinstance(today_val, dict) and today_val else None
            today_source = today_val["source"] if isinstance(today_val, dict) and today_val else None

            prev_value    = None
            prev_source   = None
            prev_date_used = None

            # 1. FRED yesterday (US only)
            fv = fred_yesterday.get(t)
            if fv is not None:
                prev_value     = fv["value"]  if isinstance(fv, dict) else float(fv)
                prev_source    = fv["source"] if isinstance(fv, dict) else "FRED API"
                prev_date_used = yesterday

            # 2. Toggle cache — try yesterday first, then walk back for last available
            if prev_value is None:
                for i in range(1, 31):
                    check = (today_date - timedelta(days=i)).isoformat()
                    cv = country_hist.get(check, {}).get(t)
                    if cv is not None:
                        prev_value    = cv["value"]  if isinstance(cv, dict) else float(cv)
                        prev_source   = cv["source"] if isinstance(cv, dict) else (
                            "BBG Cache" if force_scrape else "FinanceFlow Cache"
                        )
                        prev_date_used = check
                        break

            if prev_date_used:
                prev_dates_used.append(prev_date_used)

            cod_bps = (
                round((today_value - prev_value) * 100, 2)
                if today_value is not None and prev_value is not None
                else None
            )

            tenor_results[t] = {
                "today":        round(today_value, 4)       if today_value  is not None else None,
                "today_source": today_source,
                "prev":         round(float(prev_value), 4) if prev_value   is not None else None,
                "prev_source":  prev_source,
                "prev_date_used": prev_date_used,
                "cod_bps":      cod_bps,
            }

        overall_prev_date = max(prev_dates_used) if prev_dates_used else None

        results.append({
            "country":    c,
            "today_date": today_str,
            "prev_date":  overall_prev_date,
            "tenors":     tenor_results,
        })

    return results if len(results) > 1 else results[0]


# ---------------- TreasuryDirect proxy ----------------

@app.get("/ust-auctions")
def get_ust_auctions(
        startDate: str = Query(...),
        endDate: str = Query(...),
        dateFieldName: str = Query(...)
):
    url = (
        f"https://www.treasurydirect.gov/TA_WS/securities/search"
        f"?startDate={startDate}&endDate={endDate}&dateFieldName={dateFieldName}&format=json"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


# ---------------- New Issues (Corps) via SEC EDGAR 424B2 ----------------

_SEC_HEADERS = {
    "User-Agent": "YieldCurveDashboard dhruv_9427@hotmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

NEW_ISSUES_CORPS_CACHE = []
NEW_ISSUES_CORPS_CACHE_DATE = None
_new_issues_lock = threading.Lock()


def _strip_html_tags(html: str) -> str:
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = re.sub(r'&#\d+;', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _parse_bond_terms(text: str) -> dict:
    r = {'ticker': None, 'coupon': None, 'maturity': None,
         'call_date': None, 'cusip': None, 'offering_size': None}

    # CUSIP: exactly 9 alphanumeric chars after the label
    m = re.search(r'CUSIP(?:\s+No\.?|:|\s+Number:?)?\s*([0-9A-Z]{9})\b', text, re.IGNORECASE)
    if m:
        r['cusip'] = m.group(1)

    # Coupon: "X.XXX% ... Notes/Bonds/Debentures"
    m = re.search(
        r'(\d{1,2}\.\d+)\s*%\s*(?:Per Annum\s+)?(?:Fixed[\s-]?Rate\s+)?'
        r'(?:Senior\s+(?:Secured\s+)?|Subordinated\s+|Junior\s+)?'
        r'(?:Notes?|Bonds?|Debentures?)',
        text, re.IGNORECASE)
    if m:
        r['coupon'] = m.group(1) + '%'

    # Maturity year from "due YYYY" or "due Month DD, YYYY"
    m = re.search(
        r'(?:due|matur(?:ing|ity|e)(?:\s+on)?)\s+'
        r'(?:(?:January|February|March|April|May|June|July|August|September|October|November|December'
        r'|Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Oct\.?|Nov\.?|Dec\.?)'
        r'\s+\d{1,2},?\s+)?(\d{4})',
        text, re.IGNORECASE)
    if m:
        r['maturity'] = m.group(1)

    # Offering size — "$X billion / million" then fallback to raw dollar amount
    m = re.search(r'\$\s*([\d,.]+)\s*(billion|million)\b', text, re.IGNORECASE)
    if m:
        amt = float(m.group(1).replace(',', ''))
        r['offering_size'] = f"${amt:.1f}B" if 'billion' in m.group(2).lower() else f"${amt:.0f}M"
    else:
        m = re.search(r'\$([\d,]{10,})\b', text)
        if m:
            val = int(m.group(1).replace(',', ''))
            if val >= 1_000_000_000:
                r['offering_size'] = f"${val / 1e9:.1f}B"
            elif val >= 1_000_000:
                r['offering_size'] = f"${val / 1e6:.0f}M"

    # First call date
    m = re.search(
        r'(?:first\s+)?(?:optional\s+)?(?:call|redeem(?:able)?|redemption)\s+(?:date\s+)?'
        r'(?:on\s+or\s+after\s+)?'
        r'((?:January|February|March|April|May|June|July|August|September|October|November|December'
        r'|Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Oct\.?|Nov\.?|Dec\.?)'
        r'\s+\d{1,2},?\s+\d{4})',
        text, re.IGNORECASE)
    if m:
        r['call_date'] = m.group(1).strip()

    # Ticker from "(NYSE: XXX)" or "(NASDAQ: XXX)"
    m = re.search(r'\((?:NYSE|NASDAQ|Nasdaq|NYSE\s+Arca|NYSEARCA)\s*:\s*([A-Z]{1,5})\)', text)
    if m:
        r['ticker'] = m.group(1)

    return r


def _get_primary_doc_url(index_url: str):
    """Fetch filing index page and return URL of the primary 424B2 HTML document."""
    try:
        resp = requests.get(index_url, headers=_SEC_HEADERS, timeout=15)
        resp.raise_for_status()
        for m in re.finditer(r'href="(/Archives/edgar/data/[^"]+\.htm)"', resp.text, re.IGNORECASE):
            path = m.group(1)
            if '-index' not in path.lower():
                return 'https://www.sec.gov' + path
    except Exception as e:
        print(f"[new-issues] index fetch failed {index_url}: {e}")
    return None


def _fetch_new_issues_corps():
    global NEW_ISSUES_CORPS_CACHE, NEW_ISSUES_CORPS_CACHE_DATE
    with _new_issues_lock:
        today = dt_date.today().isoformat()
        print("[new-issues] Fetching 424B2 Atom feed from SEC EDGAR…")
        try:
            resp = requests.get(
                "https://www.sec.gov/cgi-bin/browse-edgar"
                "?action=getcurrent&type=424B2&count=40&output=atom",
                headers=_SEC_HEADERS, timeout=30
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"[new-issues] Atom feed error: {e}")
            return

        try:
            root = ET.fromstring(resp.content)
        except Exception as e:
            print(f"[new-issues] XML parse error: {e}")
            return

        ns = {'a': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('a:entry', ns)
        print(f"[new-issues] Parsing {len(entries)} filings…")

        results = []
        for entry in entries:
            title_el   = entry.find('a:title', ns)
            link_el    = entry.find('a:link', ns)
            updated_el = entry.find('a:updated', ns)
            if link_el is None:
                continue

            raw_title = (title_el.text or '') if title_el is not None else ''
            m = re.match(r'^424B2\s+-\s+(.+?)\s+\(\d+\)', raw_title, re.IGNORECASE)
            company = m.group(1).strip() if m else raw_title

            index_url  = link_el.get('href', '')
            filed_date = (updated_el.text or '')[:10] if updated_el is not None else ''

            time.sleep(0.12)  # ~8 req/s — within SEC rate limit
            doc_url = _get_primary_doc_url(index_url)
            bond = {'ticker': None, 'coupon': None, 'maturity': None,
                    'call_date': None, 'cusip': None, 'offering_size': None}
            if doc_url:
                try:
                    time.sleep(0.12)
                    dr = requests.get(doc_url, headers=_SEC_HEADERS, timeout=20)
                    dr.raise_for_status()
                    bond = _parse_bond_terms(_strip_html_tags(dr.text))
                except Exception as e:
                    print(f"[new-issues] doc fetch failed {doc_url}: {e}")

            results.append({
                'company':       company,
                'ticker':        bond['ticker'],
                'coupon':        bond['coupon'],
                'maturity':      bond['maturity'],
                'call_date':     bond['call_date'],
                'cusip':         bond['cusip'],
                'offering_size': bond['offering_size'],
                'filed':         filed_date,
                'filing_url':    index_url,
            })

        NEW_ISSUES_CORPS_CACHE = results
        NEW_ISSUES_CORPS_CACHE_DATE = today
        print(f"[new-issues] Done — {len(results)} entries cached for {today}")


def _new_issues_corps_scheduler():
    """Fire once daily at 06:00 UTC (≈ 6 AM UK time in winter, 7 AM in summer)."""
    last_run_date = None
    while True:
        now = datetime.utcnow()
        today = now.date().isoformat()
        if now.hour == 6 and last_run_date != today:
            last_run_date = today
            _fetch_new_issues_corps()
        time.sleep(30)


@app.get("/new-issues-corps")
def get_new_issues_corps():
    if _new_issues_lock.locked():
        return {"status": "loading", "data": [], "date": None}
    if not NEW_ISSUES_CORPS_CACHE:
        return {"status": "empty", "data": [], "date": None}
    return {"status": "ok", "data": NEW_ISSUES_CORPS_CACHE, "date": NEW_ISSUES_CORPS_CACHE_DATE}


@app.post("/new-issues-corps/refresh")
def refresh_new_issues_corps():
    if not _new_issues_lock.locked():
        threading.Thread(target=_fetch_new_issues_corps, daemon=True).start()
    return {"status": "loading"}


# ────────────────────────────────────────────────────────────────
# Economic Indicators
# ────────────────────────────────────────────────────────────────

ECO_INDICATORS_CACHE_FILE = os.path.join(_DATA_DIR, "eco_indicators_cache.json")

ECO_INDICATOR_KEYS = ["headline_cpi", "core_cpi", "manufacturing_pmi", "services_pmi"]
ECO_INDICATOR_API_NAMES = {
    "headline_cpi":      "Inflation Rate",
    "core_cpi":          "Core Inflation Rate",
    "manufacturing_pmi": "Manufacturing PMI",
    "services_pmi":      "Services PMI",
}


def _load_eco_indicators_cache():
    if os.path.exists(ECO_INDICATORS_CACHE_FILE):
        try:
            with open(ECO_INDICATORS_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_eco_indicators_cache(cache):
    try:
        with open(ECO_INDICATORS_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass


ECO_INDICATORS_CACHE = _load_eco_indicators_cache()


def _fetch_eco_indicators_for_country(country):
    """Fetch all economic indicators for a country in one API call and extract the 4 we need."""
    print(f"[eco-indicators] Fetching for {country}")
    url = "https://financeflowapi.com/api/v1/world-indicators"
    params = {"api_key": FINANCEFLOW_API_KEY, "country": country}
    try:
        r = requests.get(url, params=params, timeout=15).json()
        data = r.get("data", [])
        by_name = {d["indicator_name"]: d for d in data}
        result = {}
        for key, api_name in ECO_INDICATOR_API_NAMES.items():
            entry = by_name.get(api_name)
            if entry:
                def _to_float(v):
                    try:
                        return float(v) if v is not None else None
                    except (ValueError, TypeError):
                        return None
                result[key] = {
                    "last":     _to_float(entry.get("last")),
                    "previous": _to_float(entry.get("previous")),
                    "source":   "FinanceFlow",
                }
        if result:
            ECO_INDICATORS_CACHE[country] = result
            # Persist entire cache to file always with "FinanceFlow Cache" source
            # so that after a restart every country correctly shows the cached label,
            # regardless of how many countries were fetched live in this session.
            file_cache = {
                c: {k: {**v, "source": "FinanceFlow Cache"} for k, v in indicators.items()}
                for c, indicators in ECO_INDICATORS_CACHE.items()
            }
            _save_eco_indicators_cache(file_cache)
        print(f"[eco-indicators] Done for {country}: {list(result.keys())}")
        return result
    except Exception as e:
        print(f"[eco-indicators] Error for {country}: {e}")
        return {}


ECO_INDICATOR_COUNTRIES = ALL_COUNTRIES + ["japan", "china", "india", "australia", "brazil"]


@app.get("/economic-indicators")
def get_economic_indicators(country: str = Query("united_states")):
    if country not in ECO_INDICATOR_COUNTRIES:
        return {"error": f"Unknown country: {country}"}
    cached = ECO_INDICATORS_CACHE.get(country)
    if not cached:
        return {"status": "empty", "country": country, "data": {}}
    return {"status": "ok", "country": country, "data": cached}


@app.post("/economic-indicators/refresh")
def refresh_economic_indicators(country: str = Query("united_states")):
    if country not in ECO_INDICATOR_COUNTRIES:
        return {"error": f"Unknown country: {country}"}
    result = _fetch_eco_indicators_for_country(country)
    if not result:
        return {"status": "empty", "country": country, "data": {}}
    return {"status": "ok", "country": country, "data": result}
