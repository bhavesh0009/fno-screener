# Data Pipeline Documentation

## Overview

This document describes how the stock data pipeline works - from data collection to storage and querying.

---

## Architecture (Hybrid Pipeline)

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│     NSE API     │      │     nselib      │      │     DuckDB      │
│   (Primary)     │ ───► │   (Library)     │ ───► │   (Storage)     │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                                          │
┌─────────────────┐                                       │
│    yfinance     │ ─────── (Backup for adjustments) ─────┘
│   (Adjusted)    │
└─────────────────┘
```

---

## Configuration (`config.py`)

```python
DATA_START_DATE = "01-01-2025"    # Fixed start date (not rolling)
DATA_PERIOD = "1Y"                # Legacy, for index data
REQUEST_DELAY_SECONDS = 0.5       # Rate limiting
DB_PATH = DATA_DIR / "stocks.duckdb"
```

---

## CLI Commands

| Command | Description |
|---------|-------------|
| `python main.py collect-all` | Run full hybrid pipeline |
| `python main.py collect-one <symbol>` | Collect single stock |
| `python main.py stats` | Show database statistics |

---

## Hybrid Pipeline Steps

### `collect-all` Command

```
STEP 1: Fetch from nselib (primary source)
        - Uses from_date="01-01-2025" (fixed date)
        - 5 parallel workers
        - Stores in daily_ohlcv table

STEP 2: Detect mismatches
        - Query: WHERE prev_close != LAG(close)
        - Builds list of affected symbols

STEP 3: Fix with yfinance (backup)
        - For each mismatched symbol
        - Fetch adjusted data from yfinance (SYMBOL.NS)
        - Update close and volume columns

STEP 4: Fetch NIFTY 50 index data
        - For benchmark calculations
```

---

## Database Schema

### Table: `daily_ohlcv`
```sql
CREATE TABLE daily_ohlcv (
    symbol VARCHAR NOT NULL,
    date DATE NOT NULL,
    series VARCHAR,
    open DECIMAL(12,2),
    high DECIMAL(12,2),
    low DECIMAL(12,2),
    close DECIMAL(12,2),      -- Adjusted via yfinance if needed
    prev_close DECIMAL(12,2),
    volume BIGINT,            -- Adjusted via yfinance if needed
    value DECIMAL(18,2),
    vwap DECIMAL(12,2),
    trades INTEGER,
    delivery_volume BIGINT,   -- From nselib only
    delivery_pct DECIMAL(6,2),
    PRIMARY KEY (symbol, date)
)
```

---

## Data Sources

| Source | Data Type | Adjusted? |
|--------|-----------|-----------|
| nselib | OHLCV, delivery % | ❌ Unadjusted |
| yfinance | Close, Volume | ✅ Adjusted |

### yfinance Symbol Format
```python
ticker = yf.Ticker(f"{symbol}.NS")  # e.g., "BSE.NS"
```

---

## Mismatch Detection Logic

```sql
-- Stocks needing adjustment
SELECT DISTINCT symbol
FROM (
    SELECT 
        symbol,
        prev_close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) as actual_prev
    FROM daily_ohlcv
)
WHERE prev_close != actual_prev
```

---

## Known Limitations

1. **Delivery volume**: yfinance doesn't have delivery data - preserved from nselib
2. **OHLC not fully adjusted**: Only `close` and `volume` are updated from yfinance
3. **Rate limiting**: 0.5s delay between API calls
