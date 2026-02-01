"""Tests for data validation and quality checks."""
import pytest
import sys
from pathlib import Path
from datetime import date

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDataQuality:
    """Tests for data quality constraints."""
    
    def test_no_null_close_prices(self, in_memory_db, sample_ohlcv_df):
        """Verify that stored OHLCV data has no null close prices."""
        from pipeline.collectors.nse_collector import store_ohlcv_data
        
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE close IS NULL
        """).fetchone()
        
        assert result[0] == 0, "Found records with null close prices"
    
    def test_no_null_dates(self, in_memory_db, sample_ohlcv_df):
        """Verify that stored OHLCV data has no null dates."""
        from pipeline.collectors.nse_collector import store_ohlcv_data
        
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE date IS NULL
        """).fetchone()
        
        assert result[0] == 0, "Found records with null dates"
    
    def test_dates_in_expected_range(self, in_memory_db, sample_ohlcv_df):
        """Verify dates are within expected range (2025 onwards)."""
        from pipeline.collectors.nse_collector import store_ohlcv_data
        
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE date < DATE '2025-01-01'
        """).fetchone()
        
        assert result[0] == 0, "Found records with dates before 2025"
    
    def test_positive_close_prices(self, in_memory_db, sample_ohlcv_df):
        """Verify close prices are positive."""
        from pipeline.collectors.nse_collector import store_ohlcv_data
        
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE close <= 0
        """).fetchone()
        
        assert result[0] == 0, "Found records with non-positive close prices"
    
    def test_non_negative_volume(self, in_memory_db, sample_ohlcv_df):
        """Verify volumes are non-negative."""
        from pipeline.collectors.nse_collector import store_ohlcv_data
        
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE volume < 0
        """).fetchone()
        
        assert result[0] == 0, "Found records with negative volume"


class TestMismatchDetection:
    """Tests for price mismatch detection (corporate actions)."""
    
    def test_detect_large_price_drop(self, in_memory_db):
        """Test detection of stocks with >30% single-day price drops."""
        # Insert test data with a 50% price drop
        in_memory_db.execute("""
            INSERT INTO daily_ohlcv (symbol, date, series, open, high, low, close, prev_close, volume)
            VALUES 
                ('TEST', '2025-01-01', 'EQ', 100, 105, 98, 100, 95, 1000000),
                ('TEST', '2025-01-02', 'EQ', 50, 55, 48, 50, 100, 1000000)
        """)
        
        # Run mismatch detection query
        result = in_memory_db.execute('''
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
              AND (close - prev_close) / prev_close < -0.30
        ''').fetchall()
        
        symbols = [r[0] for r in result]
        assert 'TEST' in symbols, "Failed to detect 50% price drop"
    
    def test_no_false_positive_normal_drop(self, in_memory_db):
        """Test that normal price drops (<30%) are not flagged."""
        # Insert test data with a 10% price drop (normal)
        in_memory_db.execute("""
            INSERT INTO daily_ohlcv (symbol, date, series, open, high, low, close, prev_close, volume)
            VALUES 
                ('NORMAL', '2025-01-01', 'EQ', 100, 105, 98, 100, 95, 1000000),
                ('NORMAL', '2025-01-02', 'EQ', 90, 95, 88, 90, 100, 1000000)
        """)
        
        result = in_memory_db.execute('''
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
              AND (close - prev_close) / prev_close < -0.30
        ''').fetchall()
        
        symbols = [r[0] for r in result]
        assert 'NORMAL' not in symbols, "10% drop should not be flagged as mismatch"


class TestOHLCConsistency:
    """Tests for OHLC data consistency (close should be within high/low range when properly adjusted)."""
    
    def test_close_within_range_after_adjustment(self, in_memory_db):
        """Verify that properly adjusted data has close within high/low range."""
        # Insert valid adjusted data
        in_memory_db.execute("""
            INSERT INTO daily_ohlcv (symbol, date, series, open, high, low, close, volume)
            VALUES ('VALID', '2025-01-01', 'EQ', 100, 105, 95, 102, 1000000)
        """)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE close > high OR close < low
        """).fetchone()
        
        assert result[0] == 0, "Close should be within high/low range for properly adjusted data"
    
    def test_detect_ohlc_inconsistency(self, in_memory_db):
        """Verify detection of OHLC inconsistencies (indicates corporate action adjustment issues)."""
        # Insert inconsistent data (simulating unadjusted OHLC with adjusted close)
        in_memory_db.execute("""
            INSERT INTO daily_ohlcv (symbol, date, series, open, high, low, close, volume)
            VALUES ('SPLIT', '2025-01-01', 'EQ', 500, 510, 490, 102, 1000000)
        """)
        
        result = in_memory_db.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE close < low OR close > high
        """).fetchone()
        
        assert result[0] == 1, "Should detect OHLC inconsistency when close is outside high/low range"


class TestDelta52WHigh:
    """Tests for Delta 52-week high calculation."""
    
    def test_delta_52w_nonpositive(self, in_memory_db):
        """Delta from 52-week high should always be <= 0 (can't be above the high)."""
        # Insert sample data with increasing prices
        for i in range(10):
            in_memory_db.execute(f"""
                INSERT INTO daily_ohlcv (symbol, date, series, open, high, low, close, volume)
                VALUES ('TEST', DATE '2025-01-01' + INTERVAL '{i} days', 'EQ', 
                        {100 + i}, {105 + i}, {95 + i}, {100 + i}, 1000000)
            """)
        
        result = in_memory_db.execute("""
            WITH stock_data AS (
                SELECT 
                    symbol,
                    date,
                    close,
                    MAX(high) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) as high_52w
                FROM daily_ohlcv
                WHERE symbol = 'TEST'
            )
            SELECT 
                ROUND(((close - high_52w) / high_52w) * 100, 2) as delta
            FROM stock_data
            ORDER BY date DESC
            LIMIT 1
        """).fetchone()
        
        assert result[0] <= 0, "Delta 52W High should always be <= 0"
    
    def test_delta_52w_uses_consistent_data(self, in_memory_db):
        """Verify delta calculation produces sensible results with consistent OHLC data."""
        # Insert data where stock is at its 52-week high
        in_memory_db.execute("""
            INSERT INTO daily_ohlcv (symbol, date, series, open, high, low, close, volume)
            VALUES 
                ('ATHIGH', '2025-01-01', 'EQ', 100, 100, 95, 100, 1000000),
                ('ATHIGH', '2025-01-02', 'EQ', 105, 110, 100, 110, 1000000)
        """)
        
        result = in_memory_db.execute("""
            WITH stock_data AS (
                SELECT 
                    symbol,
                    date,
                    close,
                    MAX(high) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) as high_52w
                FROM daily_ohlcv
                WHERE symbol = 'ATHIGH'
            )
            SELECT 
                ROUND(((close - high_52w) / high_52w) * 100, 2) as delta
            FROM stock_data
            WHERE date = '2025-01-02'
        """).fetchone()
        
        # Close = 110, High_52w = 110, so delta should be 0
        assert result[0] == 0, "Stock at 52-week high should have delta = 0"

