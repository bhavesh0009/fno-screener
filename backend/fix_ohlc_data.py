"""One-time script to fix OHLC data for stocks with corporate actions and refresh Nifty data."""
import duckdb
from common.config import DB_PATH
from common.logger import get_pipeline_logger
from pipeline.collectors.nse_collector import (
    fetch_from_yfinance, 
    fetch_index_from_yfinance, 
    store_index_data
)

logger = get_pipeline_logger("fix_ohlc")


def get_stocks_with_ohlc_issues(conn):
    """Find stocks where close is outside high/low range (indicates unadjusted OHLC)."""
    result = conn.execute('''
        SELECT DISTINCT symbol
        FROM daily_ohlcv 
        WHERE close < low OR close > high
    ''').fetchall()
    return [r[0] for r in result]


def fix_stock_ohlc(conn, symbol):
    """Fix OHLC data for a stock using yfinance adjusted data."""
    logger.info("Fixing OHLC", symbol=symbol)
    yf_df = fetch_from_yfinance(symbol, start_date="2025-01-01")
    
    if yf_df is None or yf_df.empty:
        logger.warning("No yfinance data", symbol=symbol)
        return 0
    
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
    
    conn.commit()
    logger.info("Fixed", symbol=symbol, records=updated)
    return updated


def fix_nifty_data(conn):
    """Refresh Nifty 50 data from yfinance (provides more historical data than nselib)."""
    logger.info("Refreshing Nifty 50 data from yfinance")
    nifty_df = fetch_index_from_yfinance("NIFTY 50", start_date="2025-01-01")
    
    if nifty_df is not None:
        records = store_index_data(conn, "NIFTY 50", nifty_df)
        logger.info("Nifty 50 refreshed", records=records)
        return records
    else:
        logger.error("Failed to fetch Nifty 50 from yfinance")
        return 0


def main():
    logger.info("=" * 60)
    logger.info("OHLC Data Fix Script Started")
    
    conn = duckdb.connect(str(DB_PATH))
    
    # Step 1: Fix Nifty 50 data
    logger.info("=== Step 1: Fix Nifty 50 data ===")
    nifty_before = conn.execute("SELECT COUNT(*) FROM index_ohlcv WHERE index_name = 'NIFTY 50'").fetchone()[0]
    fix_nifty_data(conn)
    nifty_after = conn.execute("SELECT COUNT(*) FROM index_ohlcv WHERE index_name = 'NIFTY 50'").fetchone()[0]
    logger.info("Nifty data", before=nifty_before, after=nifty_after)
    
    # Step 2: Find and fix stocks with OHLC issues
    logger.info("=== Step 2: Fix stocks with OHLC issues ===")
    symbols = get_stocks_with_ohlc_issues(conn)
    logger.info("Stocks with OHLC issues", count=len(symbols), symbols=symbols)
    
    fixed = 0
    for symbol in symbols:
        if fix_stock_ohlc(conn, symbol) > 0:
            fixed += 1
    
    logger.info("Fix complete", fixed=fixed, total=len(symbols))
    
    # Step 3: Verify fix
    logger.info("=== Step 3: Verify fix ===")
    remaining = get_stocks_with_ohlc_issues(conn)
    logger.info("Remaining issues", count=len(remaining))
    
    if remaining:
        for sym in remaining[:5]:
            count = conn.execute(f"SELECT COUNT(*) FROM daily_ohlcv WHERE symbol = '{sym}' AND (close < low OR close > high)").fetchone()[0]
            logger.warning("Still has issues", symbol=sym, bad_rows=count)
    
    conn.close()
    logger.info("Script completed")


if __name__ == "__main__":
    main()
