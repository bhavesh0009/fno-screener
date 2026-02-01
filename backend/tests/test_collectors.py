"""Tests for pipeline collectors."""
import pytest
import sys
from pathlib import Path

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.collectors.nse_collector import (
    store_fno_stocks,
    store_ohlcv_data,
    store_index_data,
)


class TestStoreFnoStocks:
    """Tests for store_fno_stocks function."""
    
    def test_store_fno_stocks_basic(self, in_memory_db, sample_fno_df):
        """Test basic F&O stock storage."""
        count = store_fno_stocks(in_memory_db, sample_fno_df)
        
        assert count == 3
        
        # Verify data was stored
        result = in_memory_db.execute("SELECT COUNT(*) FROM fno_stocks").fetchone()
        assert result[0] == 3
    
    def test_store_fno_stocks_upsert(self, in_memory_db, sample_fno_df):
        """Test that re-storing updates existing records (upsert behavior)."""
        # Store once
        store_fno_stocks(in_memory_db, sample_fno_df)
        
        # Store again
        count = store_fno_stocks(in_memory_db, sample_fno_df)
        
        assert count == 3
        # Should still have only 3 records (not 6)
        result = in_memory_db.execute("SELECT COUNT(*) FROM fno_stocks").fetchone()
        assert result[0] == 3
    
    def test_store_fno_stocks_empty(self, in_memory_db):
        """Test handling of empty DataFrame."""
        import pandas as pd
        empty_df = pd.DataFrame()
        
        count = store_fno_stocks(in_memory_db, empty_df)
        assert count == 0
    
    def test_store_fno_stocks_none(self, in_memory_db):
        """Test handling of None input."""
        count = store_fno_stocks(in_memory_db, None)
        assert count == 0


class TestStoreOhlcvData:
    """Tests for store_ohlcv_data function."""
    
    def test_store_ohlcv_basic(self, in_memory_db, sample_ohlcv_df):
        """Test basic OHLCV data storage."""
        count = store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        assert count == 3
        
        # Verify data was stored
        result = in_memory_db.execute(
            "SELECT COUNT(*) FROM daily_ohlcv WHERE symbol = 'SBIN'"
        ).fetchone()
        assert result[0] == 3
    
    def test_store_ohlcv_idempotent(self, in_memory_db, sample_ohlcv_df):
        """Test that re-storing the same data doesn't create duplicates."""
        # Store twice
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        
        # Should still have only 3 records (idempotent)
        result = in_memory_db.execute(
            "SELECT COUNT(*) FROM daily_ohlcv WHERE symbol = 'SBIN'"
        ).fetchone()
        assert result[0] == 3
    
    def test_store_ohlcv_multiple_symbols(self, in_memory_db, sample_ohlcv_df):
        """Test storing data for multiple symbols."""
        store_ohlcv_data(in_memory_db, "SBIN", sample_ohlcv_df)
        store_ohlcv_data(in_memory_db, "RELIANCE", sample_ohlcv_df)
        
        # Should have 6 records (3 per symbol)
        result = in_memory_db.execute("SELECT COUNT(*) FROM daily_ohlcv").fetchone()
        assert result[0] == 6
    
    def test_store_ohlcv_empty(self, in_memory_db):
        """Test handling of empty DataFrame."""
        import pandas as pd
        empty_df = pd.DataFrame()
        
        count = store_ohlcv_data(in_memory_db, "SBIN", empty_df)
        assert count == 0


class TestStoreIndexData:
    """Tests for store_index_data function."""
    
    def test_store_index_basic(self, in_memory_db, sample_index_df):
        """Test basic index data storage."""
        count = store_index_data(in_memory_db, "NIFTY 50", sample_index_df)
        
        assert count == 3
        
        # Verify data was stored
        result = in_memory_db.execute(
            "SELECT COUNT(*) FROM index_ohlcv WHERE index_name = 'NIFTY 50'"
        ).fetchone()
        assert result[0] == 3
    
    def test_store_index_idempotent(self, in_memory_db, sample_index_df):
        """Test that re-storing the same data doesn't create duplicates."""
        store_index_data(in_memory_db, "NIFTY 50", sample_index_df)
        store_index_data(in_memory_db, "NIFTY 50", sample_index_df)
        
        # Should still have only 3 records
        result = in_memory_db.execute(
            "SELECT COUNT(*) FROM index_ohlcv WHERE index_name = 'NIFTY 50'"
        ).fetchone()
        assert result[0] == 3
