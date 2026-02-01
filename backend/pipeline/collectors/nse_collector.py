"""NSE data collector using nselib with structured logging."""
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import duckdb

from nselib import capital_market

from common.logger import get_pipeline_logger

# Get module logger
logger = get_pipeline_logger("collector")


def fetch_fno_stocks() -> pd.DataFrame:
    """Fetch list of F&O eligible stocks."""
    logger.info("Fetching F&O stock list")
    df = capital_market.fno_equity_list()
    logger.info("F&O stock list fetched", count=len(df))
    return df


def fetch_stock_data(
    symbol: str, 
    period: str = None,
    from_date: str = None,
    to_date: str = None,
    delay: float = 0.5
) -> Optional[pd.DataFrame]:
    """
    Fetch price, volume and delivery data for a stock.
    
    Args:
        symbol: Stock symbol (e.g., 'SBIN')
        period: Data period ('1M', '3M', '6M', '1Y') - used if from_date not provided
        from_date: Start date in dd-mm-yyyy format
        to_date: End date in dd-mm-yyyy format (defaults to today)
        delay: Delay after request (rate limiting)
    
    Returns:
        DataFrame with OHLCV data or None if failed
    """
    try:
        logger.debug("Fetching stock data", symbol=symbol, from_date=from_date)
        
        if from_date:
            # Use date range
            if to_date is None:
                to_date = datetime.now().strftime("%d-%m-%Y")
            df = capital_market.price_volume_and_deliverable_position_data(
                symbol=symbol, 
                from_date=from_date,
                to_date=to_date
            )
        else:
            # Use period (legacy)
            df = capital_market.price_volume_and_deliverable_position_data(
                symbol=symbol, 
                period=period or "1Y"
            )
        
        time.sleep(delay)  # Rate limiting
        
        if df is not None and not df.empty:
            logger.info("Stock data fetched", symbol=symbol, records=len(df))
            return df
        else:
            logger.warning("No data returned", symbol=symbol)
            return None
            
    except Exception as e:
        logger.error("Failed to fetch stock data", symbol=symbol, error=str(e))
        time.sleep(delay)
        return None


def fetch_from_yfinance(
    symbol: str,
    start_date: str = "2025-01-01",
    delay: float = 0.5
) -> Optional[pd.DataFrame]:
    """
    Fetch adjusted price data from yfinance as backup.
    
    Args:
        symbol: Stock symbol (e.g., 'SBIN') - will add .NS suffix
        start_date: Start date in YYYY-MM-DD format
        delay: Delay after request (rate limiting)
    
    Returns:
        DataFrame with adjusted OHLCV data or None if failed
    """
    try:
        import yfinance as yf
        
        ticker_symbol = f"{symbol}.NS"
        logger.info("Fetching from yfinance", symbol=ticker_symbol, start_date=start_date)
        
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(start=start_date)
        
        time.sleep(delay)
        
        if df is not None and not df.empty:
            # Normalize column names to match our schema
            df = df.reset_index()
            df = df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            # Convert timezone-aware datetime to date
            df['date'] = pd.to_datetime(df['date']).dt.date
            logger.info("yfinance data fetched", symbol=ticker_symbol, records=len(df))
            return df
        else:
            logger.warning("No yfinance data", symbol=ticker_symbol)
            return None
            
    except Exception as e:
        logger.error("yfinance fetch failed", symbol=symbol, error=str(e))
        time.sleep(delay)
        return None


def fetch_index_from_yfinance(
    index_name: str = "NIFTY 50",
    start_date: str = "2025-01-01",
    delay: float = 0.5
) -> Optional[pd.DataFrame]:
    """
    Fetch index data from yfinance.
    
    Args:
        index_name: Index name (maps to yfinance ticker)
        start_date: Start date in YYYY-MM-DD format
        delay: Delay after request (rate limiting)
    
    Returns:
        DataFrame with OHLC data formatted for store_index_data, or None if failed
    """
    try:
        import yfinance as yf
        
        # Map index names to yfinance tickers
        ticker_map = {
            'NIFTY 50': '^NSEI',
            'NIFTY BANK': '^NSEBANK',
        }
        
        ticker_symbol = ticker_map.get(index_name, '^NSEI')
        logger.info("Fetching index from yfinance", index=index_name, ticker=ticker_symbol)
        
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(start=start_date)
        
        time.sleep(delay)
        
        if df is not None and not df.empty:
            df = df.reset_index()
            # Normalize to match store_index_data expected format
            df = df.rename(columns={
                'Date': 'TIMESTAMP',
                'Open': 'OPEN_INDEX_VAL',
                'High': 'HIGH_INDEX_VAL',
                'Low': 'LOW_INDEX_VAL',
                'Close': 'CLOSE_INDEX_VAL'
            })
            # Convert timezone-aware datetime to date string format expected by store_index_data
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.strftime('%d-%b-%Y')
            logger.info("yfinance index data fetched", index=index_name, records=len(df))
            return df
        else:
            logger.warning("No yfinance index data", index=index_name)
            return None
            
    except Exception as e:
        logger.error("yfinance index fetch failed", index=index_name, error=str(e))
        time.sleep(delay)
        return None


def store_fno_stocks(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Store F&O stocks list in database."""
    if df is None or df.empty:
        return 0
    
    # Normalize column names
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    
    # Map columns - adjust based on actual nselib output
    insert_data = []
    for _, row in df.iterrows():
        symbol = row.get('symbol', row.get('Symbol', ''))
        company = row.get('company_name', row.get('companyname', row.get('Company Name', '')))
        lot_size = row.get('lot_size', row.get('lotsize', row.get('Lot Size', 0)))
        
        if symbol:
            insert_data.append((str(symbol).strip(), str(company).strip(), int(lot_size) if lot_size else 0))
    
    if insert_data:
        conn.executemany("""
            INSERT OR REPLACE INTO fno_stocks (symbol, company_name, lot_size, last_updated)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, insert_data)
        conn.commit()
        logger.info("F&O stocks stored", count=len(insert_data))
    
    return len(insert_data)


def store_ohlcv_data(
    conn: duckdb.DuckDBPyConnection, 
    symbol: str, 
    df: pd.DataFrame
) -> int:
    """Store OHLCV data for a stock."""
    if df is None or df.empty:
        return 0
    
    # Clean column names - remove BOM and extra quotes, normalize
    df = df.copy()
    df.columns = [c.replace('\ufeff', '').replace('"', '').strip() for c in df.columns]
    
    # Filter to EQ series only to prevent duplicates from BL/T0 series
    if 'Series' in df.columns:
        df = df[df['Series'] == 'EQ']
        if df.empty:
            logger.warning("No EQ series data", symbol=symbol)
            return 0
    
    records = []
    for _, row in df.iterrows():
        try:
            # Parse date
            date_val = row.get('Date', None)
            if date_val is None:
                continue
            
            if isinstance(date_val, str):
                # Try different date formats
                for fmt in ['%d-%b-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d %b %Y']:
                    try:
                        date_val = datetime.strptime(date_val.strip(), fmt).date()
                        break
                    except:
                        continue
            
            # Helper to safely convert to float
            def safe_float(val, default=0.0):
                try:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return default
                    if isinstance(val, str):
                        val = val.replace(',', '').strip()
                        if val == '-':  # Handle cases where value is just a dash
                            return default
                    return float(val)
                except:
                    return default
            
            # Helper to safely convert to int
            def safe_int(val, default=0):
                try:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return default
                    if isinstance(val, str):
                        val = val.replace(',', '').strip()
                        if val == '-':
                            return default
                        return int(float(val)) # Handle "123.0" strings
                    return int(float(val))
                except:
                    return default
            
            record = (
                symbol,
                date_val,
                str(row.get('Series', 'EQ')).strip(),
                safe_float(row.get('OpenPrice')),
                safe_float(row.get('HighPrice')),
                safe_float(row.get('LowPrice')),
                safe_float(row.get('ClosePrice')),
                safe_float(row.get('PrevClose')),
                safe_int(row.get('TotalTradedQuantity')),
                safe_float(row.get('TurnoverInRs')),
                safe_float(row.get('AveragePrice')),
                safe_int(row.get('No.ofTrades')),
                safe_int(row.get('DeliverableQty')),
                safe_float(row.get('%DlyQttoTradedQty')),
            )
            records.append(record)
        except Exception as e:
            logger.warning("Could not parse row", symbol=symbol, error=str(e))
            continue
    
    if records:
        conn.executemany("""
            INSERT OR REPLACE INTO daily_ohlcv 
            (symbol, date, series, open, high, low, close, prev_close, 
             volume, value, vwap, trades, delivery_volume, delivery_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        conn.commit()
        logger.debug("OHLCV data stored", symbol=symbol, records=len(records))
    
    return len(records)


def fetch_index_data(
    index_name: str = "NIFTY 50",
    period: str = "1Y"
) -> Optional[pd.DataFrame]:
    """
    Fetch historical index data.
    
    Args:
        index_name: Index name (e.g., 'NIFTY 50', 'NIFTY BANK')
        period: Data period ('1D', '1W', '1M', '6M', '1Y')
    
    Returns:
        DataFrame with OHLC data or None if failed
    """
    try:
        logger.info("Fetching index data", index=index_name, period=period)
        df = capital_market.index_data(index=index_name, period=period)
        
        if df is not None and not df.empty:
            logger.info("Index data fetched", index=index_name, records=len(df))
            return df
        else:
            logger.warning("No index data", index=index_name)
            return None
            
    except Exception as e:
        logger.error("Failed to fetch index data", index=index_name, error=str(e))
        return None


def store_index_data(
    conn: duckdb.DuckDBPyConnection,
    index_name: str,
    df: pd.DataFrame
) -> int:
    """Store index OHLC data in database."""
    if df is None or df.empty:
        return 0
    
    records = []
    for _, row in df.iterrows():
        try:
            # Parse date
            date_val = row.get('TIMESTAMP', None)
            if date_val is None:
                continue
            
            if isinstance(date_val, str):
                for fmt in ['%d-%b-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d %b %Y']:
                    try:
                        date_val = datetime.strptime(date_val.strip(), fmt).date()
                        break
                    except:
                        continue
            
            # Helper to safely convert to float
            def safe_float(val, default=0.0):
                try:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return default
                    if isinstance(val, str):
                        val = val.replace(',', '').strip()
                    return float(val)
                except:
                    return default
            
            record = (
                index_name,
                date_val,
                safe_float(row.get('OPEN_INDEX_VAL')),
                safe_float(row.get('HIGH_INDEX_VAL')),
                safe_float(row.get('CLOSE_INDEX_VAL')),
                safe_float(row.get('LOW_INDEX_VAL')),
            )
            records.append(record)
        except Exception as e:
            logger.warning("Could not parse index row", index=index_name, error=str(e))
            continue
    
    if records:
        conn.executemany("""
            INSERT OR REPLACE INTO index_ohlcv 
            (index_name, date, open, high, close, low)
            VALUES (?, ?, ?, ?, ?, ?)
        """, records)
        conn.commit()
        logger.info("Index data stored", index=index_name, records=len(records))
    
    return len(records)
