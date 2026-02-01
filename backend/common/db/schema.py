"""DuckDB schema definitions and database utilities."""
import duckdb
from pathlib import Path


def init_database(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB database with schema."""
    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(db_path))
    
    # Create tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fno_stocks (
            symbol VARCHAR PRIMARY KEY,
            company_name VARCHAR,
            lot_size INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_ohlcv (
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            series VARCHAR,
            open DECIMAL(12,2),
            high DECIMAL(12,2),
            low DECIMAL(12,2),
            close DECIMAL(12,2),
            prev_close DECIMAL(12,2),
            volume BIGINT,
            value DECIMAL(18,2),
            vwap DECIMAL(12,2),
            trades INTEGER,
            delivery_volume BIGINT,
            delivery_pct DECIMAL(6,2),
            PRIMARY KEY (symbol, date)
        )
    """)
    
    # Create index for faster queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_ohlcv_date 
        ON daily_ohlcv(date)
    """)
    
    # Index OHLCV table for NIFTY 50 data
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_ohlcv (
            index_name VARCHAR NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(12,2),
            high DECIMAL(12,2),
            close DECIMAL(12,2),
            low DECIMAL(12,2),
            PRIMARY KEY (index_name, date)
        )
    """)
    
    return conn


def get_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Get a connection to the database."""
    return duckdb.connect(str(db_path))
