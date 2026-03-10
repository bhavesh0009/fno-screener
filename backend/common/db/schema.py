"""PostgreSQL schema definitions and database utilities for Supabase."""
import psycopg2


def init_database(dsn: str):
    """Initialize PostgreSQL database with schema. Returns connection."""
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fno_stocks (
            symbol VARCHAR PRIMARY KEY,
            company_name VARCHAR,
            lot_size INTEGER,
            sector VARCHAR,
            industry VARCHAR,
            segment VARCHAR,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_ohlcv (
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            series VARCHAR,
            open NUMERIC(12,2),
            high NUMERIC(12,2),
            low NUMERIC(12,2),
            close NUMERIC(12,2),
            prev_close NUMERIC(12,2),
            volume BIGINT,
            value NUMERIC(18,2),
            vwap NUMERIC(12,2),
            trades INTEGER,
            delivery_volume BIGINT,
            delivery_pct NUMERIC(6,2),
            PRIMARY KEY (symbol, date)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_ohlcv_date
        ON daily_ohlcv(date)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS index_ohlcv (
            index_name VARCHAR NOT NULL,
            date DATE NOT NULL,
            open NUMERIC(12,2),
            high NUMERIC(12,2),
            close NUMERIC(12,2),
            low NUMERIC(12,2),
            PRIMARY KEY (index_name, date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fno_ban_period (
            trade_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (trade_date, symbol)
        )
    """)

    conn.commit()
    return conn


def get_connection(dsn: str):
    """Get a connection to the database."""
    return psycopg2.connect(dsn)
