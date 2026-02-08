"""Main entry point for stock data collection with structured logging."""
import argparse
import sys
from datetime import datetime
import concurrent.futures

from common.config import DB_PATH, DATA_PERIOD, DATA_START_DATE, REQUEST_DELAY_SECONDS
from common.db.schema import init_database, get_connection
from common.logger import get_pipeline_logger
from pipeline.collectors.nse_collector import (
    fetch_fno_stocks,
    fetch_stock_data,
    store_fno_stocks,
    store_ohlcv_data,
    fetch_index_data,
    store_index_data,
    fetch_from_yfinance,
    fetch_index_from_yfinance,
)

# Get module logger
logger = get_pipeline_logger("main")


def fetch_worker(symbol):
    """Worker function to fetch data for a single stock using date range."""
    try:
        df = fetch_stock_data(
            symbol=symbol,
            from_date=DATA_START_DATE,
            delay=REQUEST_DELAY_SECONDS
        )
        return symbol, df
    except Exception as e:
        logger.error("Error in fetch worker", symbol=symbol, error=str(e))
        return symbol, None


def detect_mismatches(conn):
    """
    Detect stocks that likely had corporate actions (splits/bonuses).
    Looks for single-day changes > 30% (drops OR gains) which indicate unadjusted prices.
    Returns list of symbols that need yfinance backup.
    """
    logger.info("Detecting price mismatches")
    result = conn.execute('''
        WITH ordered_data AS (
            SELECT 
                symbol,
                date,
                close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close
            FROM daily_ohlcv
        )
        SELECT DISTINCT symbol
        FROM ordered_data
        WHERE prev_close IS NOT NULL
          AND ABS((close - prev_close) / prev_close) > 0.30
    ''').fetchall()
    
    symbols = [r[0] for r in result]
    logger.info("Mismatches detected", count=len(symbols), symbols=symbols)
    return symbols


def fix_with_yfinance(conn, symbols):
    """
    Fetch adjusted data from yfinance and update all OHLC + volume for mismatched stocks.
    Returns dict with success/failure counts.
    """
    results = {'fixed': [], 'failed': []}
    
    for symbol in symbols:
        logger.info("Fixing with yfinance", symbol=symbol)
        yf_df = fetch_from_yfinance(symbol, start_date="2025-01-01")
        
        if yf_df is None or yf_df.empty:
            results['failed'].append(symbol)
            logger.warning("yfinance fix failed - no data", symbol=symbol)
            continue
        
        # Update all OHLC + volume for each date (yfinance provides adjusted OHLC)
        updated = 0
        for _, row in yf_df.iterrows():
            try:
                conn.execute('''
                    UPDATE daily_ohlcv 
                    SET open = ?, high = ?, low = ?, close = ?, volume = ?
                    WHERE symbol = ? AND date = ?
                ''', [
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume']),
                    symbol,
                    row['date']
                ])
                updated += 1
            except Exception as e:
                pass  # Skip dates not in our DB
        
        conn.commit()  # Ensure changes are persisted
        if updated > 0:
            results['fixed'].append(symbol)
            logger.info("Stock fixed with yfinance", symbol=symbol, records_updated=updated)
        else:
            results['failed'].append(symbol)
            logger.warning("yfinance fix failed - no records updated", symbol=symbol)
    
    return results


def collect_all():
    """Collect data for all F&O stocks using hybrid pipeline."""
    logger.info("=" * 60)
    logger.info("F&O Stock Data Collection (Hybrid Pipeline) Started",
                start_time=datetime.now().isoformat(),
                data_start_date=DATA_START_DATE)
    
    # Initialize database
    logger.info("Initializing database", path=str(DB_PATH))
    conn = init_database(DB_PATH)
    
    # Fetch and store F&O stock list
    fno_df = fetch_fno_stocks()
    if fno_df is None or fno_df.empty:
        logger.error("Could not fetch F&O stock list")
        return 1
    
    stored = store_fno_stocks(conn, fno_df)
    logger.info("F&O stocks stored", count=stored)
    
    # Get list of symbols
    symbols = conn.execute("SELECT symbol FROM fno_stocks ORDER BY symbol").fetchall()
    symbols = [s[0] for s in symbols]
    
    # === STEP 1: Fetch from nselib (primary source) ===
    logger.info("STEP 1: Fetching from nselib", 
                stock_count=len(symbols), 
                concurrent_workers=5)
    
    success_count = 0
    fail_count = 0
    total_records = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {executor.submit(fetch_worker, symbol): symbol for symbol in symbols}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
            symbol = future_to_symbol[future]
            try:
                sym, df = future.result()
                
                if df is not None:
                    records = store_ohlcv_data(conn, sym, df)
                    total_records += records
                    success_count += 1
                else:
                    fail_count += 1
                    
            except Exception as exc:
                logger.error("Worker exception", symbol=symbol, error=str(exc))
                fail_count += 1
    
    logger.info("Step 1 Complete", 
                success=success_count, 
                failed=fail_count, 
                total_records=total_records)
    
    # === STEP 2: Detect mismatches ===
    logger.info("STEP 2: Detecting price mismatches")
    mismatched_symbols = detect_mismatches(conn)
    
    # === STEP 3: Fix with yfinance ===
    if mismatched_symbols:
        logger.info("STEP 3: Fixing with yfinance backup",
                    mismatch_count=len(mismatched_symbols))
        fix_results = fix_with_yfinance(conn, mismatched_symbols)
        logger.info("Step 3 Complete",
                    fixed=fix_results['fixed'],
                    failed=fix_results['failed'])
    else:
        logger.info("STEP 3: No mismatches to fix")
    
    # === STEP 4: Fetch NIFTY 50 index from yfinance (more historical data) ===
    logger.info("STEP 4: Fetching NIFTY 50 index data from yfinance")
    
    # Convert dd-mm-yyyy to YYYY-MM-DD for yfinance
    try:
        start_date_dt = datetime.strptime(DATA_START_DATE, "%d-%m-%Y")
        yf_start_date = start_date_dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"Could not parse DATA_START_DATE: {DATA_START_DATE}, using default 2025-01-01")
        yf_start_date = "2025-01-01"

    nifty_df = fetch_index_from_yfinance("NIFTY 50", start_date=yf_start_date)
    if nifty_df is not None:
        nifty_records = store_index_data(conn, "NIFTY 50", nifty_df)
        logger.info("NIFTY 50 index stored", records=nifty_records)
    else:
        logger.warning("Could not fetch NIFTY 50 data from yfinance")
    
    logger.info("Pipeline completed",
                database=str(DB_PATH),
                finish_time=datetime.now().isoformat())
    
    conn.close()
    return 0


def show_stats():
    """Show database statistics."""
    if not DB_PATH.exists():
        logger.error("Database not found. Run 'collect-all' first.", path=str(DB_PATH))
        return 1
    
    conn = get_connection(DB_PATH)
    
    stock_count = conn.execute("SELECT COUNT(*) FROM fno_stocks").fetchone()[0]
    ohlcv_count = conn.execute("SELECT COUNT(*) FROM daily_ohlcv").fetchone()[0]
    
    date_range = conn.execute("""
        SELECT MIN(date), MAX(date) FROM daily_ohlcv
    """).fetchone()
    
    print("\n" + "=" * 40)
    print("Database Statistics")
    print("=" * 40)
    print(f"F&O Stocks: {stock_count}")
    print(f"OHLCV Records: {ohlcv_count}")
    if date_range[0]:
        print(f"Date Range: {date_range[0]} to {date_range[1]}")
    print(f"Database: {DB_PATH}")
    print("=" * 40 + "\n")
    
    conn.close()
    return 0


def collect_one(symbol: str):
    """Collect data for a single stock."""
    logger.info("Collecting data for single stock", symbol=symbol)
    
    # Initialize database (ensures tables exist)
    conn = init_database(DB_PATH)
    
    # Fetch OHLCV data
    df_ohlcv = fetch_stock_data(symbol, period=DATA_PERIOD)
    if df_ohlcv is not None:
        count = store_ohlcv_data(conn, symbol, df_ohlcv)
        logger.info("Single stock collection complete", symbol=symbol, records=count)
    
    conn.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.main [collect-all|collect-one <symbol>|stats]")
        sys.exit(1)
        
    command = sys.argv[1]
    
    if command == "collect-all":
        collect_all()
    elif command == "collect-one":
        if len(sys.argv) < 3:
            print("Usage: python -m pipeline.main collect-one <symbol>")
            sys.exit(1)
        symbol = sys.argv[2]
        collect_one(symbol)
    elif command == "stats":
        show_stats()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
