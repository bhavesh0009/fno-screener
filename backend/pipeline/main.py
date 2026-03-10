"""Main entry point for stock data collection with structured logging."""
import argparse
import sys
from datetime import datetime, timedelta
import concurrent.futures

from common.config import DATABASE_URL, DATA_PERIOD, DATA_START_DATE, REQUEST_DELAY_SECONDS
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
    fetch_ban_period_stocks,
    store_ban_period_data,
    fetch_lot_sizes,
    update_stock_metadata,
)

# Get module logger
logger = get_pipeline_logger("main")


def fetch_worker(symbol, latest_date=None):
    """Worker function to fetch data for a single stock using date range."""
    try:
        from_date = DATA_START_DATE
        if latest_date:
            next_date = latest_date + timedelta(days=1)
            if next_date > datetime.now().date():
                logger.debug("Stock up to date, skipping", symbol=symbol)
                return symbol, None
            from_date = next_date.strftime("%d-%m-%Y")
            
        df = fetch_stock_data(
            symbol=symbol,
            from_date=from_date,
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
    cur = conn.cursor()
    cur.execute('''
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
    ''')
    result = cur.fetchall()
    
    symbols = [r[0] for r in result]
    logger.info("Mismatches detected", count=len(symbols), symbols=symbols)
    return symbols


def fetch_yf_worker(symbol):
    """Worker function to fetch yfinance data."""
    yf_df = fetch_from_yfinance(symbol, start_date="2025-01-01", delay=0.1)
    return symbol, yf_df


def fix_with_yfinance(conn, symbols):
    """
    Fetch adjusted data from yfinance and update all OHLC + volume for mismatched stocks.
    Runs concurrently.
    Returns dict with success/failure counts.
    """
    results = {'fixed': [], 'failed': []}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_symbol = {executor.submit(fetch_yf_worker, symbol): symbol for symbol in symbols}
        
        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                sym, yf_df = future.result()
                
                if yf_df is None or yf_df.empty:
                    results['failed'].append(symbol)
                    logger.warning("yfinance fix failed - no data", symbol=symbol)
                    continue
                
                # Update all OHLC + volume for each date (yfinance provides adjusted OHLC)
                updated = 0
                cur = conn.cursor()
                for _, row in yf_df.iterrows():
                    try:
                        cur.execute('''
                            UPDATE daily_ohlcv
                            SET open = %s, high = %s, low = %s, close = %s, volume = %s
                            WHERE symbol = %s AND date = %s
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
                
                if updated > 0:
                    results['fixed'].append(symbol)
                    logger.info("Stock fixed with yfinance", symbol=symbol, records_updated=updated)
                else:
                    results['failed'].append(symbol)
                    logger.warning("yfinance fix failed - no records updated", symbol=symbol)
            
            except Exception as e:
                logger.error("Error in yfinance fix worker", symbol=symbol, error=str(e))
                results['failed'].append(symbol)

    conn.commit()  # Ensure changes are persisted
    return results


def collect_all():
    """Collect data for all F&O stocks using hybrid pipeline."""
    logger.info("=" * 60)
    logger.info("F&O Stock Data Collection (Hybrid Pipeline) Started",
                start_time=datetime.now().isoformat(),
                data_start_date=DATA_START_DATE)
    
    # Initialize database
    logger.info("Initializing database")
    conn = init_database(DATABASE_URL)
    
    # Fetch and store F&O stock list with lot sizes
    fno_df = fetch_fno_stocks()
    if fno_df is None or fno_df.empty:
        logger.error("Could not fetch F&O stock list")
        return 1
    
    # Fetch lot sizes from Angel Broking API
    lot_sizes = fetch_lot_sizes()
    if lot_sizes:
        logger.info("Lot sizes fetched", count=len(lot_sizes))
    else:
        logger.warning("Could not fetch lot sizes, will use 0")
    
    stored = store_fno_stocks(conn, fno_df, lot_sizes)
    logger.info("F&O stocks stored", count=stored)
    
    # Get list of symbols
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM fno_stocks ORDER BY symbol")
    symbols = cur.fetchall()
    symbols = [s[0] for s in symbols]
    
    # === STEP 1: Fetch from nselib (primary source) ===
    logger.info("STEP 1: Fetching from nselib", 
                stock_count=len(symbols), 
                concurrent_workers=10)
    
    success_count = 0
    fail_count = 0
    total_records = 0
    
    # Get max dates to perform incremental fetch
    try:
        cur.execute("SELECT symbol, MAX(date) FROM daily_ohlcv GROUP BY symbol")
        max_dates = dict(cur.fetchall())
    except Exception:
        max_dates = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(fetch_worker, symbol, max_dates.get(symbol)): symbol for symbol in symbols}
        
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
    
    # === STEP 4: Fetch Index data (NIFTY 50 and NIFTY BANK) from yfinance ===
    logger.info("STEP 4: Fetching index data from yfinance")
    
    # Convert dd-mm-yyyy to YYYY-MM-DD for yfinance
    try:
        start_date_dt = datetime.strptime(DATA_START_DATE, "%d-%m-%Y")
        yf_start_date = start_date_dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"Could not parse DATA_START_DATE: {DATA_START_DATE}, using default 2025-01-01")
        yf_start_date = "2025-01-01"

    # Fetch NIFTY 50
    nifty_df = fetch_index_from_yfinance("NIFTY 50", start_date=yf_start_date)
    if nifty_df is not None:
        nifty_records = store_index_data(conn, "NIFTY 50", nifty_df)
        logger.info("NIFTY 50 index stored", records=nifty_records)
    else:
        logger.warning("Could not fetch NIFTY 50 data from yfinance")
    
    # Fetch NIFTY BANK
    banknifty_df = fetch_index_from_yfinance("NIFTY BANK", start_date=yf_start_date)
    if banknifty_df is not None:
        banknifty_records = store_index_data(conn, "NIFTY BANK", banknifty_df)
        logger.info("NIFTY BANK index stored", records=banknifty_records)
    else:
        logger.warning("Could not fetch NIFTY BANK data from yfinance")
    
    # === STEP 5: Fetch F&O Ban Period data ===
    logger.info("STEP 5: Fetching F&O ban period data")
    today_str = datetime.now().strftime("%d-%m-%Y")
    banned_stocks = fetch_ban_period_stocks(trade_date=today_str)
    if banned_stocks:
        ban_records = store_ban_period_data(conn, today_str, banned_stocks)
        logger.info("Ban period data stored", records=ban_records, symbols=banned_stocks)
    else:
        logger.info("No stocks in ban period today")
    
    # === STEP 6: Update stock metadata (industry/segment) if null ===
    # This data rarely changes, so only fetch for stocks with null values
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol FROM fno_stocks
        WHERE industry IS NULL OR segment IS NULL OR industry = '' OR segment = ''
        ORDER BY symbol
    """)
    symbols_needing_metadata = cur.fetchall()
    symbols_needing_metadata = [s[0] for s in symbols_needing_metadata]
    
    if symbols_needing_metadata:
        logger.info("STEP 6: Updating stock metadata (industry/segment)", 
                    count=len(symbols_needing_metadata))
        updated = update_stock_metadata(conn, symbols_needing_metadata, delay_seconds=0.5)
        logger.info("Metadata update complete", updated=updated)
    else:
        logger.info("STEP 6: All stocks have metadata, skipping")
    
    logger.info("Pipeline completed",
                finish_time=datetime.now().isoformat())
    
    conn.close()
    return 0


def show_stats():
    """Show database statistics."""
    conn = get_connection(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fno_stocks")
    stock_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM daily_ohlcv")
    ohlcv_count = cur.fetchone()[0]

    cur.execute("SELECT MIN(date), MAX(date) FROM daily_ohlcv")
    date_range = cur.fetchone()
    
    print("\n" + "=" * 40)
    print("Database Statistics")
    print("=" * 40)
    print(f"F&O Stocks: {stock_count}")
    print(f"OHLCV Records: {ohlcv_count}")
    if date_range[0]:
        print(f"Date Range: {date_range[0]} to {date_range[1]}")
    print("=" * 40 + "\n")
    
    conn.close()
    return 0


def collect_one(symbol: str):
    """Collect data for a single stock."""
    logger.info("Collecting data for single stock", symbol=symbol)
    
    # Initialize database (ensures tables exist)
    conn = init_database(DATABASE_URL)
    
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
