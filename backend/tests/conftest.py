"""Pytest configuration and fixtures for pipeline tests."""
import pytest
import duckdb
import pandas as pd
from pathlib import Path


@pytest.fixture
def in_memory_db():
    """Create an in-memory DuckDB database with schema."""
    conn = duckdb.connect(":memory:")
    
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
    
    yield conn
    conn.close()


@pytest.fixture
def sample_fno_df():
    """Create sample F&O stocks DataFrame."""
    return pd.DataFrame({
        'symbol': ['SBIN', 'RELIANCE', 'TCS'],
        'company_name': ['State Bank of India', 'Reliance Industries', 'Tata Consultancy Services'],
        'lot_size': [1500, 250, 150]
    })


@pytest.fixture
def sample_ohlcv_df():
    """Create sample OHLCV DataFrame matching nselib output format."""
    return pd.DataFrame({
        'Date': ['01-Jan-2025', '02-Jan-2025', '03-Jan-2025'],
        'Series': ['EQ', 'EQ', 'EQ'],
        'OpenPrice': [100.0, 102.0, 101.0],
        'HighPrice': [105.0, 104.0, 103.0],
        'LowPrice': [99.0, 100.0, 99.5],
        'ClosePrice': [104.0, 101.0, 102.5],
        'PrevClose': [98.0, 104.0, 101.0],
        'TotalTradedQuantity': [1000000, 1200000, 800000],
        'TurnoverInRs': [100000000, 120000000, 80000000],
        'AveragePrice': [102.0, 101.5, 101.0],
        'No.ofTrades': [5000, 6000, 4000],
        'DeliverableQty': [500000, 600000, 400000],
        '%DlyQttoTradedQty': [50.0, 50.0, 50.0]
    })


@pytest.fixture
def sample_index_df():
    """Create sample index DataFrame matching nselib output format."""
    return pd.DataFrame({
        'TIMESTAMP': ['01-Jan-2025', '02-Jan-2025', '03-Jan-2025'],
        'OPEN_INDEX_VAL': [24000.0, 24100.0, 24050.0],
        'HIGH_INDEX_VAL': [24200.0, 24150.0, 24100.0],
        'LOW_INDEX_VAL': [23900.0, 24000.0, 23950.0],
        'CLOSE_INDEX_VAL': [24150.0, 24050.0, 24080.0]
    })
