# Factory Pocket API 📡

Market data backend for **Factory Pocket Pro** iOS app.

## What it does
- Fetches **31 market items** (indices, energy, metals, currencies, agriculture) via yfinance
- Fetches **34 exchange rates** with ECB cross-validation
- Runs every **2 hours** via GitHub Actions
- Outputs `market_data.json` consumed by the iOS app

## Endpoints
| File | URL |
|------|-----|
| Market Data | `https://raw.githubusercontent.com/mouakkid/factory-pocket-api/main/market_data.json` |
| Health Check | `https://raw.githubusercontent.com/mouakkid/factory-pocket-api/main/health.json` |

## Run locally
```bash
pip install -r requirements.txt
python scraper.py
python audit_rates.py
```
