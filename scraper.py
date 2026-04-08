#!/usr/bin/env python3
"""
Factory Pocket Pro — Market Data Scraper v4.0
Fetches Forex, Energy, Plastics, Metals, Indices, Steel,
Shipping & Agriculture prices via yfinance.

v4.0 Changes:
  - Exchanger rates: period="5d" for weekend/holiday resilience
  - Retry with exponential backoff on exchanger fetches
  - Sanity bounds per currency to reject aberrant values
  - Optional cross-check against ECB reference rates
  - Audit metadata in JSON output

Designed for GitHub Actions cron (every 2h).
"""

import json
import os
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

try:
    import yfinance as yf
except ImportError:
    print("❌  yfinance not installed — run:  pip install yfinance")
    sys.exit(1)

# ── Ticker Registry ──────────────────────────────────────────
# Ordered: international pairs first for currencies
TICKERS = {
    "indices": {
        "^GSPC":     "S&P 500",
        "^DJI":      "Dow Jones",
        "^IXIC":     "Nasdaq",
        "^STOXX50E": "Euro Stoxx 50",
        "^FTSE":     "FTSE 100",
        "^N225":     "Nikkei 225",
    },
    "currencies": {
        # International majors first
        "EURUSD=X": "EUR / USD",
        "USDJPY=X": "USD / JPY",
        "GBPUSD=X": "GBP / USD",
        "USDCNY=X": "USD / CNY",
        "DX-Y.NYB": "US Dollar Index",
        # Regional
        "USDMAD=X": "USD / MAD",
        "EURMAD=X": "EUR / MAD",
        "GBPMAD=X": "GBP / MAD",
    },
    "energy": {
        "BZ=F":  "Brent Crude",
        "CL=F":  "WTI Crude",
        "NG=F":  "Natural Gas",
        "HO=F":  "Heating Oil",
        "RB=F":  "Gasoline (RBOB)",
    },
    "metals": {
        "GC=F":  "Gold",
        "SI=F":  "Silver",
        "HG=F":  "Copper",
        "ALI=F": "Aluminium",
        "PL=F":  "Platinum",
        "PA=F":  "Palladium",
    },
    "agriculture": {
        "SB=F":  "Sugar #11",
        "KC=F":  "Coffee",
        "CT=F":  "Cotton #2",
        "ZW=F":  "Wheat",
        "ZC=F":  "Corn",
        "ZS=F":  "Soybeans",
    },
}

# ── Currency Exchanger Registry ──────────────────────────────
# Map of "Code" -> ("Ticker", InvertBool)
# InvertBool: True if ticker is Code/USD (e.g. EURUSD=X), False if USD/Code
EXCHANGER_PAIRS = {
    "USD": ("USD", False), # Base
    "EUR": ("EURUSD=X", True),
    "GBP": ("GBPUSD=X", True),
    "JPY": ("USDJPY=X", False),
    "CAD": ("USDCAD=X", False),
    "AUD": ("AUDUSD=X", True),
    "CNY": ("USDCNY=X", False),
    "CHF": ("USDCHF=X", False),
    "HKD": ("USDHKD=X", False),
    "SGD": ("USDSGD=X", False),
    "SEK": ("USDSEK=X", False),
    "KRW": ("USDKRW=X", False),
    "NOK": ("USDNOK=X", False),
    "NZD": ("NZDUSD=X", True),
    "INR": ("USDINR=X", False),
    "MXN": ("USDMXN=X", False),
    "TWD": ("USDTWD=X", False),
    "ZAR": ("USDZAR=X", False),
    "BRL": ("USDBRL=X", False),
    "DKK": ("USDDKK=X", False),
    "PLN": ("USDPLN=X", False),
    "THB": ("USDTHB=X", False),
    "IDR": ("USDIDR=X", False),
    "HUF": ("USDHUF=X", False),
    "CZK": ("USDCZK=X", False),
    "ILS": ("USDILS=X", False),
    "CLP": ("USDCLP=X", False),
    "PHP": ("USDPHP=X", False),
    "AED": ("USDAED=X", False),
    "COP": ("USDCOP=X", False),
    "SAR": ("USDSAR=X", False),
    "MYR": ("USDMYR=X", False),
    "RON": ("USDRON=X", False),
    "MAD": ("USDMAD=X", False),  # Morocco
}

# ── Sanity Bounds (1 USD = ? Currency) ───────────────────────
# (min, max) — generous ranges to catch only truly aberrant values.
# Updated Feb 2026. Ranges give ~±50% margin from recent norms.
RATE_BOUNDS: dict[str, tuple[float, float]] = {
    "EUR": (0.50, 1.50),
    "GBP": (0.40, 1.30),
    "JPY": (70,   250),
    "CAD": (0.90, 2.00),
    "AUD": (0.90, 2.20),
    "CNY": (4.0,  10.0),
    "CHF": (0.50, 1.50),
    "HKD": (5.0,  10.0),
    "SGD": (0.80, 2.00),
    "SEK": (5.0,  16.0),
    "KRW": (700,  2000),
    "NOK": (5.0,  16.0),
    "NZD": (0.90, 2.50),
    "INR": (50,   130),
    "MXN": (10,   30),
    "TWD": (20,   45),
    "ZAR": (10,   30),
    "BRL": (3.0,  8.0),
    "DKK": (4.0,  10.0),
    "PLN": (2.5,  6.0),
    "THB": (20,   50),
    "IDR": (10000, 22000),
    "HUF": (200,  550),
    "CZK": (15,   35),
    "ILS": (2.5,  5.5),
    "CLP": (500,  1400),
    "PHP": (35,   75),
    "AED": (3.0,  4.5),
    "COP": (2500, 6000),
    "SAR": (3.0,  4.5),
    "MYR": (3.0,  7.0),
    "RON": (3.0,  7.0),
    "MAD": (7.0,  14.0),
}

MAX_RETRIES = 3
RETRY_DELAY = 2
OUTPUT_FILE = "market_data.json"

# ── Per-ticker unit map ──────────────────────────────────────
UNIT_MAP = {
    # Indices — points
    "^GSPC": "pts", "^DJI": "pts", "^IXIC": "pts",
    "^STOXX50E": "pts", "^FTSE": "pts", "^N225": "pts",
    # Currencies — dimensionless exchange rates (no unit)
    "EURUSD=X": None, "USDJPY=X": None, "GBPUSD=X": None,
    "USDCNY=X": None, "USDMAD=X": None, "EURMAD=X": None,
    "GBPMAD=X": None, "DX-Y.NYB": "pts",
    # Energy
    "BZ=F": "USD/bbl", "CL=F": "USD/bbl",
    "NG=F": "USD/MMBtu", "HO=F": "USD/gal", "RB=F": "USD/gal",
    # Metals
    "GC=F": "USD/oz", "SI=F": "USD/oz",
    "HG=F": "USD/lb", "ALI=F": "USD/t",
    "PL=F": "USD/oz", "PA=F": "USD/oz",
    # Agriculture
    "SB=F": "¢/lb", "KC=F": "¢/lb", "CT=F": "¢/lb",
    "ZW=F": "¢/bu", "ZC=F": "¢/bu", "ZS=F": "¢/bu",
}

# Currency code for the price (used for legacy "currency" field)
CURRENCY_MAP = {
    "indices": None, "currencies": None,
    "energy": "USD", "metals": "USD",
    "agriculture": None,
}


# ── Helpers ──────────────────────────────────────────────────

def fetch_with_retry(ticker: str, retries: int = MAX_RETRIES) -> dict | None:
    delay = RETRY_DELAY
    for attempt in range(1, retries + 1):
        try:
            data = yf.download(ticker, period="5d", interval="1d",
                               progress=False, timeout=15)
            if data.empty or len(data) < 2:
                print(f"  ⚠  {ticker}: not enough data (rows={len(data)})")
                return None

            if hasattr(data.columns, 'levels') and len(data.columns.levels) > 1:
                data.columns = data.columns.droplevel(1)

            # Drop NaN rows before extracting prices
            close_series = data["Close"].dropna()
            if len(close_series) < 2:
                print(f"  ⚠  {ticker}: not enough non-NaN close prices")
                return None

            latest = float(close_series.iloc[-1])
            prev   = float(close_series.iloc[-2])

            if latest <= 0 or prev <= 0:
                return None

            trend = round(((latest - prev) / prev) * 100, 2)
            return {"price": round(latest, 4), "trend": trend}

        except Exception as exc:
            print(f"  ⚠  {ticker} attempt {attempt}/{retries}: {exc}")
            if attempt < retries:
                time.sleep(delay)
                delay *= 2
    return None


def build_section(category: str, tickers: dict) -> list[dict]:
    items = []
    currency = CURRENCY_MAP.get(category)
    for symbol, label in tickers.items():
        result = fetch_with_retry(symbol)
        if result is None:
            continue
        items.append({
            "ticker":   symbol,
            "name":     label,
            "price":    result["price"],
            "trend":    result["trend"],
            "currency": currency,
            "unit":     UNIT_MAP.get(symbol),
        })
    return items


# ── ECB Cross-Check ──────────────────────────────────────────

ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

def fetch_ecb_reference_rates() -> dict[str, float]:
    """
    Fetches the latest ECB daily reference rates (EUR-based).
    Converts them to USD-based (1 USD = X Currency).
    Returns empty dict on failure (non-blocking).
    """
    try:
        req = urllib.request.Request(ECB_DAILY_URL, headers={"User-Agent": "FPG-Scraper/4.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml_data = resp.read()
        
        root = ET.fromstring(xml_data)
        ns = {"ecb": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
        
        # ECB gives EUR-based rates: 1 EUR = X Currency
        eur_rates: dict[str, float] = {"EUR": 1.0}
        for cube in root.findall(".//ecb:Cube[@currency]", ns):
            code = cube.attrib["currency"]
            rate = float(cube.attrib["rate"])
            eur_rates[code] = rate
        
        # Convert to USD-based: 1 USD = X Currency
        eur_usd = eur_rates.get("USD")
        if not eur_usd or eur_usd <= 0:
            return {}
        
        usd_rates: dict[str, float] = {"USD": 1.0}
        for code, eur_val in eur_rates.items():
            if code == "USD":
                continue
            # 1 USD = (eur_val / eur_usd) Currency
            usd_rates[code] = round(eur_val / eur_usd, 6)
        
        print(f"    🏛  ECB reference: {len(usd_rates)} rates loaded")
        return usd_rates
        
    except Exception as e:
        print(f"    ⚠  ECB cross-check unavailable: {e}")
        return {}


# ── Exchanger Rate Fetching (v4.0) ───────────────────────────

def fetch_exchanger_rates() -> tuple[dict[str, float], dict]:
    """
    Fetches rates for all currencies in EXCHANGER_PAIRS.
    Normalizes everything to: 1 USD = ? Currency
    
    Returns:
        (rates_dict, audit_info_dict)
    """
    print(f"💱  Fetching {len(EXCHANGER_PAIRS)} exchanger rates (v4.0)...")
    rates: dict[str, float] = {"USD": 1.0}
    
    audit = {
        "total_requested": len(EXCHANGER_PAIRS) - 1,  # exclude USD
        "fetched": 0,
        "validated": 0,
        "bounds_rejected": [],
        "fetch_failed": [],
        "cross_check_deviations": {},
        "cross_check_source": "ECB",
    }
    
    for code, (ticker, invert) in EXCHANGER_PAIRS.items():
        if code == "USD":
            continue
        
        # ── Fetch with retry (period=5d for weekend resilience)
        price = None
        delay = RETRY_DELAY
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data = yf.download(ticker, period="5d", interval="1d",
                                   progress=False, timeout=15)
                if data.empty:
                    if attempt < MAX_RETRIES:
                        time.sleep(delay)
                        delay *= 2
                        continue
                    break
                
                # Handle multi-level columns
                if hasattr(data.columns, 'levels') and len(data.columns.levels) > 1:
                    data.columns = data.columns.droplevel(1)
                
                close_series = data["Close"].dropna()
                if len(close_series) < 1:
                    if attempt < MAX_RETRIES:
                        time.sleep(delay)
                        delay *= 2
                        continue
                    break
                price = float(close_series.iloc[-1])
                
                if price <= 0:
                    price = None
                    continue
                break  # success
                
            except Exception as e:
                print(f"  ⚠  {code} ({ticker}) attempt {attempt}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(delay)
                    delay *= 2
        
        if price is None:
            print(f"  ❌  {code}: all retries exhausted for {ticker}")
            audit["fetch_failed"].append(code)
            continue
        
        audit["fetched"] += 1
        
        # ── Compute USD-based rate
        final_rate = (1.0 / price) if invert else price
        final_rate = round(final_rate, 6)
        
        # ── Bounds validation
        if code in RATE_BOUNDS:
            lo, hi = RATE_BOUNDS[code]
            if not (lo <= final_rate <= hi):
                print(f"  🚫  {code}: rate {final_rate} out of bounds [{lo}, {hi}]")
                audit["bounds_rejected"].append({
                    "code": code, "rate": final_rate,
                    "bounds": [lo, hi],
                })
                continue
        
        audit["validated"] += 1
        rates[code] = round(final_rate, 4)
    
    # ── ECB Cross-check
    ecb_rates = fetch_ecb_reference_rates()
    if ecb_rates:
        for code, scraped_rate in rates.items():
            if code == "USD":
                continue
            ecb_rate = ecb_rates.get(code)
            if ecb_rate and ecb_rate > 0:
                deviation_pct = abs(scraped_rate - ecb_rate) / ecb_rate * 100
                if deviation_pct > 2.0:  # flag deviations > 2%
                    audit["cross_check_deviations"][code] = {
                        "scraped": scraped_rate,
                        "ecb": round(ecb_rate, 4),
                        "deviation_pct": round(deviation_pct, 2),
                    }
                    if deviation_pct > 15:
                        print(f"  🚨  {code}: CRITICAL deviation {deviation_pct:.1f}% "
                              f"(scraped={scraped_rate}, ECB={ecb_rate:.4f})")
    
    audit["final_count"] = len(rates)
    print(f"    ✅ {audit['validated']}/{audit['total_requested']} rates validated, "
          f"{len(audit['cross_check_deviations'])} cross-check warnings")
    
    return rates, audit



# ── Main ─────────────────────────────────────────────────────

def main():
    print("🏭  Factory Pocket Pro — Market Scraper v4.0")
    print(f"    {datetime.now(timezone.utc).isoformat()}\n")

    results: dict[str, list] = {}
    total = 0

    for category, tickers in TICKERS.items():
        print(f"📦  Fetching {category} ({len(tickers)} tickers)…")
        section = build_section(category, tickers)
        results[category] = section
        total += len(section)
        print(f"    ✅ {len(section)}/{len(tickers)} OK\n")

    exchanger_rates, exchanger_audit = fetch_exchanger_rates()

    output = {
        "last_update": datetime.now(timezone.utc).isoformat(),
        "totalItems":  total,
        "indices":     results.get("indices", []),
        "currencies":  results.get("currencies", []),
        "energy":      results.get("energy", []),
        "metals":      results.get("metals", []),
        "agriculture": results.get("agriculture", []),
        "rates":       exchanger_rates,
        "exchanger_audit": exchanger_audit,
    }

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILE)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, allow_nan=False)

    print(f"\n📄  Saved {total} items + {len(exchanger_rates)} rates → {out_path}")

    if total == 0:
        print("❌  No data fetched — exiting with error code.")
        sys.exit(1)

    # Fail if too many exchanger rates failed
    if exchanger_audit["validated"] < 10:
        print(f"❌  Only {exchanger_audit['validated']} rates validated — "
              f"minimum 10 required. Exiting with error.")
        sys.exit(1)


if __name__ == "__main__":
    main()
