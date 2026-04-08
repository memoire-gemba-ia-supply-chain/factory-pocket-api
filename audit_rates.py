#!/usr/bin/env python3
"""
Factory Pocket Pro — Exchange Rate Audit Script v1.0

Standalone audit & control script that validates the exchange rates
produced by scraper.py against:
  1. Structural checks (presence, freshness, completeness)
  2. Sanity bounds per currency
  3. Cross-validation against the ECB daily reference rates

Outputs: audit_report.json
Exit code: 0 = PASS, 1 = CRITICAL failure (blocks CI commit)
"""

import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MARKET_DATA = os.path.join(SCRIPT_DIR, "market_data.json")
AUDIT_REPORT = os.path.join(SCRIPT_DIR, "audit_report.json")

# Minimum required rates to pass audit
MIN_RATES = 15

# Max acceptable age (hours) — data must not be older than this
MAX_AGE_HOURS = 6

# Deviation thresholds (vs ECB reference)
WARN_DEVIATION_PCT = 3.0    # > 3% = WARNING
CRITICAL_DEVIATION_PCT = 10.0  # > 10% = CRITICAL

# Expected USD-based rate bounds (same as in scraper.py)
RATE_BOUNDS = {
    "EUR": (0.50, 1.50),   "GBP": (0.40, 1.30),   "JPY": (70, 250),
    "CAD": (0.90, 2.00),   "AUD": (0.90, 2.20),    "CNY": (4.0, 10.0),
    "CHF": (0.50, 1.50),   "HKD": (5.0, 10.0),     "SGD": (0.80, 2.00),
    "SEK": (5.0, 16.0),    "KRW": (700, 2000),      "NOK": (5.0, 16.0),
    "NZD": (0.90, 2.50),   "INR": (50, 130),        "MXN": (10, 30),
    "TWD": (20, 45),       "ZAR": (10, 30),         "BRL": (3.0, 8.0),
    "DKK": (4.0, 10.0),    "PLN": (2.5, 6.0),       "THB": (20, 50),
    "IDR": (10000, 22000), "HUF": (200, 550),       "CZK": (15, 35),
    "ILS": (2.5, 5.5),     "CLP": (500, 1400),      "PHP": (35, 75),
    "AED": (3.0, 4.5),     "COP": (2500, 6000),     "SAR": (3.0, 4.5),
    "MYR": (3.0, 7.0),     "RON": (3.0, 7.0),       "MAD": (7.0, 14.0),
}

# Major currencies that MUST be present
REQUIRED_CURRENCIES = {"EUR", "GBP", "JPY", "CNY", "CHF", "CAD", "MAD"}

ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"


# ── Helpers ──────────────────────────────────────────────────

def load_market_data() -> dict | None:
    if not os.path.exists(MARKET_DATA):
        return None
    with open(MARKET_DATA, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_last_update(date_str: str) -> datetime | None:
    from datetime import datetime
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
    ]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # fallback: try fromisoformat
    try:
        return datetime.fromisoformat(date_str)
    except Exception:
        return None


def fetch_ecb_rates() -> dict[str, float]:
    """Fetch ECB reference rates and convert to USD-based."""
    try:
        req = urllib.request.Request(ECB_DAILY_URL,
                                    headers={"User-Agent": "FPG-Audit/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml_data = resp.read()

        root = ET.fromstring(xml_data)
        ns = {"ecb": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}

        eur_rates = {"EUR": 1.0}
        for cube in root.findall(".//ecb:Cube[@currency]", ns):
            eur_rates[cube.attrib["currency"]] = float(cube.attrib["rate"])

        eur_usd = eur_rates.get("USD", 0)
        if eur_usd <= 0:
            return {}

        usd_rates = {"USD": 1.0}
        for code, val in eur_rates.items():
            if code != "USD":
                usd_rates[code] = round(val / eur_usd, 6)
        return usd_rates
    except Exception as e:
        print(f"  ⚠  ECB fetch failed: {e}")
        return {}


# ── Audit Pipeline ───────────────────────────────────────────

def run_audit() -> dict:
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "PASS",
        "checks": [],
        "summary": {},
        "details_per_currency": {},
    }

    critical = False
    warnings = 0

    def add_check(name: str, status: str, detail: str = ""):
        nonlocal critical, warnings
        report["checks"].append({"check": name, "status": status, "detail": detail})
        if status == "CRITICAL":
            critical = True
        elif status == "WARNING":
            warnings += 1

    # ── 1. File existence
    data = load_market_data()
    if data is None:
        add_check("file_exists", "CRITICAL", f"{MARKET_DATA} not found")
        report["status"] = "CRITICAL"
        return report
    add_check("file_exists", "PASS")

    # ── 2. Freshness
    last_update_str = data.get("last_update", "")
    last_update = parse_last_update(last_update_str)
    if last_update is None:
        add_check("freshness", "CRITICAL", f"Cannot parse last_update: {last_update_str}")
    else:
        now = datetime.now(timezone.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        age_hours = (now - last_update).total_seconds() / 3600
        if age_hours > MAX_AGE_HOURS:
            add_check("freshness", "WARNING",
                       f"Data is {age_hours:.1f}h old (max {MAX_AGE_HOURS}h)")
        else:
            add_check("freshness", "PASS", f"{age_hours:.1f}h old")

    # ── 3. Rates presence
    rates = data.get("rates", {})
    if not rates:
        add_check("rates_present", "CRITICAL", "No 'rates' block in market_data.json")
        report["status"] = "CRITICAL"
        return report
    add_check("rates_present", "PASS", f"{len(rates)} rates found")

    # ── 4. Minimum count
    if len(rates) < MIN_RATES:
        add_check("minimum_count", "CRITICAL",
                   f"Only {len(rates)} rates (min {MIN_RATES})")
    else:
        add_check("minimum_count", "PASS", f"{len(rates)} ≥ {MIN_RATES}")

    # ── 5. USD base = 1.0
    usd_rate = rates.get("USD")
    if usd_rate != 1.0:
        add_check("usd_base", "CRITICAL", f"USD rate = {usd_rate}, expected 1.0")
    else:
        add_check("usd_base", "PASS")

    # ── 6. Required currencies
    missing = REQUIRED_CURRENCIES - set(rates.keys())
    if missing:
        add_check("required_currencies", "WARNING",
                   f"Missing: {', '.join(sorted(missing))}")
    else:
        add_check("required_currencies", "PASS")

    # ── 7. Bounds check per currency
    bounds_violations = []
    for code, rate in rates.items():
        if code == "USD":
            continue
        status_detail = {"rate": rate}
        if code in RATE_BOUNDS:
            lo, hi = RATE_BOUNDS[code]
            if not (lo <= rate <= hi):
                bounds_violations.append(code)
                status_detail["bounds_status"] = "FAIL"
                status_detail["bounds"] = [lo, hi]
            else:
                status_detail["bounds_status"] = "PASS"
        else:
            status_detail["bounds_status"] = "NO_BOUNDS"
        report["details_per_currency"][code] = status_detail

    if bounds_violations:
        add_check("bounds_validation", "WARNING",
                   f"Out of bounds: {', '.join(bounds_violations)}")
    else:
        add_check("bounds_validation", "PASS")

    # ── 8. ECB Cross-validation
    ecb_rates = fetch_ecb_rates()
    if not ecb_rates:
        add_check("ecb_cross_check", "WARNING", "ECB data unavailable — skipped")
    else:
        cross_errors = []
        cross_warnings = []
        for code, rate in rates.items():
            if code == "USD":
                continue
            ecb_rate = ecb_rates.get(code)
            if ecb_rate and ecb_rate > 0:
                dev = abs(rate - ecb_rate) / ecb_rate * 100
                entry = report["details_per_currency"].setdefault(code, {"rate": rate})
                entry["ecb_rate"] = round(ecb_rate, 4)
                entry["ecb_deviation_pct"] = round(dev, 2)
                if dev > CRITICAL_DEVIATION_PCT:
                    cross_errors.append(f"{code}({dev:.1f}%)")
                elif dev > WARN_DEVIATION_PCT:
                    cross_warnings.append(f"{code}({dev:.1f}%)")

        if cross_errors:
            add_check("ecb_cross_check", "CRITICAL",
                       f"Critical deviations: {', '.join(cross_errors)}")
        elif cross_warnings:
            add_check("ecb_cross_check", "WARNING",
                       f"Deviations: {', '.join(cross_warnings)}")
        else:
            add_check("ecb_cross_check", "PASS",
                       f"All rates within {WARN_DEVIATION_PCT}% of ECB")

    # ── 9. Scraper audit metadata (if present)
    scraper_audit = data.get("exchanger_audit", {})
    if scraper_audit:
        failed = scraper_audit.get("fetch_failed", [])
        rejected = scraper_audit.get("bounds_rejected", [])
        if failed:
            add_check("scraper_failures", "WARNING",
                       f"Scraper failed to fetch: {', '.join(failed)}")
        if rejected:
            codes = [r["code"] for r in rejected]
            add_check("scraper_bounds_rejected", "WARNING",
                       f"Scraper rejected: {', '.join(codes)}")
        if not failed and not rejected:
            add_check("scraper_audit", "PASS", "No scraper-level issues")

    # ── Final status
    report["summary"] = {
        "total_rates": len(rates),
        "bounds_violations": len(bounds_violations),
        "warnings": warnings,
        "critical": critical,
    }
    report["status"] = "CRITICAL" if critical else ("WARNING" if warnings > 0 else "PASS")
    return report


# ── Main ─────────────────────────────────────────────────────

def main():
    print("🔍  Factory Pocket Pro — Exchange Rate Audit v1.0")
    print(f"    {datetime.now(timezone.utc).isoformat()}\n")

    report = run_audit()

    # Save report
    with open(AUDIT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n📋  Audit report saved → {AUDIT_REPORT}")

    # Print summary
    s = report["summary"]
    status = report["status"]
    emoji = {"PASS": "✅", "WARNING": "⚠️", "CRITICAL": "❌"}.get(status, "❓")
    print(f"\n{emoji}  AUDIT STATUS: {status}")
    print(f"    Rates: {s['total_rates']}")
    print(f"    Bounds violations: {s['bounds_violations']}")
    print(f"    Warnings: {s['warnings']}")

    for check in report["checks"]:
        icon = {"PASS": "✓", "WARNING": "⚠", "CRITICAL": "✗"}[check["status"]]
        line = f"    [{icon}] {check['check']}"
        if check.get("detail"):
            line += f" — {check['detail']}"
        print(line)

    if status == "CRITICAL":
        print("\n❌  AUDIT FAILED — commit should be blocked.")
        sys.exit(1)
    else:
        print("\n✅  Audit passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
