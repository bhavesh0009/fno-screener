"""
Stock Screening Logic
Defines the SQL queries and metadata for stock screens.
"""

DEFAULT_COLUMNS = [
    {"key": "symbol", "label": "Symbol", "type": "symbol"},
    {"key": "close", "label": "Price", "type": "currency"},
    {"key": "changePct", "label": "Change %", "type": "percent"},
    {"key": "volumeMult", "label": "Vol Multiple", "type": "multiplier"},
    {"key": "deliveryPct", "label": "Delivery %", "type": "percent"},
    {"key": "date", "label": "Date", "type": "date"},
]

BREAKOUT_COLUMNS = [
    {"key": "symbol", "label": "Symbol", "type": "symbol"},
    {"key": "close", "label": "Price", "type": "currency"},
    {"key": "changePct", "label": "Change %", "type": "percent"},
    {"key": "strength", "label": "Strength", "type": "strength"},
    {"key": "volumeMult", "label": "Vol Multiple", "type": "multiplier"},
    {"key": "deliveryPct", "label": "Delivery %", "type": "percent"},
    {"key": "date", "label": "Date", "type": "date"},
]

SCREENS = {
    "volume-breakout": {
        "id": "volume-breakout",
        "title": "Volume Breakout Pattern",
        "description": "Bullish breakout with high volume confirmation.",
        "columns": DEFAULT_COLUMNS,
        "sql": """
            WITH analysis AS (
                SELECT 
                    symbol, 
                    date, 
                    close, 
                    open, 
                    volume,
                    delivery_pct,
                    -- 20-day High (excluding today)
                    MAX(high) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as max_20_high,
                    -- 50-day Avg Volume
                    AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) as avg_50_vol,
                    -- 10-day Max Volume (excluding today)
                    MAX(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as max_10_vol
                FROM daily_ohlcv
            )
            SELECT 
                symbol,
                date,
                close,
                (close - open) / open * 100 as change_pct, -- approximate daily change
                volume,
                delivery_pct,
                ROUND(volume / avg_50_vol, 2) as volume_mult, -- How many times average volume
                'N/A' as strength
            FROM analysis
            WHERE date = (SELECT MAX(date) FROM daily_ohlcv)
              AND close > max_20_high          -- Condition 1: Price Breakout
              AND volume > (avg_50_vol * 2)    -- Condition 2: Volume Explosion
              AND volume > max_10_vol          -- Condition 3: Local Volume Spike
              AND close > open                 -- Condition 4: Bullish Candle
              AND close > 20                   -- Condition 5: No Penny Stocks
            ORDER BY volume DESC
        """
    },
    "upward-breakout": {
        "id": "upward-breakout",
        "title": "Upward Breakout (20D)",
        "description": "Price breaks above the highest high of the last 20 days.",
        "columns": BREAKOUT_COLUMNS,
        "sql": """
            WITH ohlcv_lag AS (
                SELECT *,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close_calc
                FROM daily_ohlcv
            ),
            tr_calc AS (
                SELECT *,
                       GREATEST(
                           high - low,
                           ABS(high - prev_close_calc),
                           ABS(low - prev_close_calc)
                        ) as tr
                FROM ohlcv_lag
            ),
            indicators AS (
                SELECT *,
                       -- ATR (14)
                       AVG(tr) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr,
                       -- 20 Day High (Excluding today)
                       MAX(high) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as max_20_high,
                       -- 20 Day Avg Volume (Excluding today)
                       AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_20_vol
                FROM tr_calc
            )
            SELECT 
                symbol,
                date,
                close,
                ROUND((close - prev_close) / prev_close * 100, 2) as change_pct,
                volume,
                delivery_pct,
                ROUND(volume / avg_20_vol, 2) as volume_mult,
                CASE 
                    WHEN (close - max_20_high) > atr AND volume > (avg_20_vol * 1.5) THEN 'Full'
                    WHEN (close - max_20_high) > atr THEN 'Partial (Low Volume)'
                    ELSE 'Partial (Small Size)'
                END as strength
            FROM indicators
            WHERE date = (SELECT MAX(date) FROM daily_ohlcv)
              AND close > max_20_high
            ORDER BY strength ASC, volume_mult DESC
        """
    },
    "downward-breakout": {
        "id": "downward-breakout",
        "title": "Downward Breakdown (20D)",
        "description": "Price breaks below the lowest low of the last 20 days.",
        "columns": BREAKOUT_COLUMNS,
        "sql": """
            WITH ohlcv_lag AS (
                SELECT *,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close_calc
                FROM daily_ohlcv
            ),
            tr_calc AS (
                SELECT *,
                       GREATEST(
                           high - low,
                           ABS(high - prev_close_calc),
                           ABS(low - prev_close_calc)
                        ) as tr
                FROM ohlcv_lag
            ),
            indicators AS (
                SELECT *,
                       -- ATR (14)
                       AVG(tr) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr,
                       -- 20 Day Low (Excluding today)
                       MIN(low) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as min_20_low,
                       -- 20 Day Avg Volume (Excluding today)
                       AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_20_vol
                FROM tr_calc
            )
            SELECT 
                symbol,
                date,
                close,
                ROUND((close - prev_close) / prev_close * 100, 2) as change_pct,
                volume,
                delivery_pct,
                ROUND(volume / avg_20_vol, 2) as volume_mult,
                CASE 
                    WHEN (min_20_low - close) > atr AND volume > (avg_20_vol * 1.5) THEN 'Full'
                    WHEN (min_20_low - close) > atr THEN 'Partial (Low Volume)'
                    ELSE 'Partial (Small Size)'
                END as strength
            FROM indicators
            WHERE date = (SELECT MAX(date) FROM daily_ohlcv)
              AND close < min_20_low
            ORDER BY strength ASC, volume_mult DESC
        """
    },
    "volume-breakout-delivery": {
        "id": "volume-breakout-delivery",
        "title": "Volume Breakout (Delivery)",
        "description": "Bullish breakout based on high *delivery* volume confirmation.",
        "columns": DEFAULT_COLUMNS,
        "sql": """
            WITH analysis AS (
                SELECT 
                    symbol, 
                    date, 
                    close, 
                    open, 
                    volume,
                    delivery_pct,
                    delivery_volume,
                    -- 20-day High (excluding today)
                    MAX(high) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as max_20_high,
                    -- 50-day Avg Delivery Volume
                    AVG(delivery_volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING) as avg_50_del_vol,
                    -- 10-day Max Delivery Volume (excluding today)
                    MAX(delivery_volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as max_10_del_vol
                FROM daily_ohlcv
            )
            SELECT 
                symbol,
                date,
                close,
                (close - open) / open * 100 as change_pct, -- approximate daily change
                volume,
                delivery_pct,
                ROUND(delivery_volume / avg_50_del_vol, 2) as volume_mult, -- Multiplier based on Delivery Volume
                'N/A' as strength
            FROM analysis
            WHERE date = (SELECT MAX(date) FROM daily_ohlcv)
              AND close > max_20_high          -- Condition 1: Price Breakout
              AND delivery_volume > (avg_50_del_vol * 2)    -- Condition 2: Delivery Volume Explosion
              AND delivery_volume > max_10_del_vol          -- Condition 3: Local Delivery Volume Spike
              AND close > open                 -- Condition 4: Bullish Candle
              AND close > 20                   -- Condition 5: No Penny Stocks
            ORDER BY volume_mult DESC
        """
    },
    "upward-breakout-delivery": {
        "id": "upward-breakout-delivery",
        "title": "Upward Breakout (20D Delivery)",
        "description": "Price breaks above 20-day high with high *delivery* volume.",
        "columns": BREAKOUT_COLUMNS,
        "sql": """
            WITH ohlcv_lag AS (
                SELECT *,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close_calc
                FROM daily_ohlcv
            ),
            tr_calc AS (
                SELECT *,
                       GREATEST(
                           high - low,
                           ABS(high - prev_close_calc),
                           ABS(low - prev_close_calc)
                        ) as tr
                FROM ohlcv_lag
            ),
            indicators AS (
                SELECT *,
                       -- ATR (14)
                       AVG(tr) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr,
                       -- 20 Day High (Excluding today)
                       MAX(high) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as max_20_high,
                       -- 20 Day Avg Delivery Volume (Excluding today)
                       AVG(delivery_volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_20_del_vol
                FROM tr_calc
            )
            SELECT 
                symbol,
                date,
                close,
                ROUND((close - prev_close) / prev_close * 100, 2) as change_pct,
                volume,
                delivery_pct,
                ROUND(delivery_volume / avg_20_del_vol, 2) as volume_mult,
                CASE 
                    WHEN (close - max_20_high) > atr AND delivery_volume > (avg_20_del_vol * 1.5) THEN 'Full'
                    WHEN (close - max_20_high) > atr THEN 'Partial (Low Volume)'
                    ELSE 'Partial (Small Size)'
                END as strength
            FROM indicators
            WHERE date = (SELECT MAX(date) FROM daily_ohlcv)
              AND close > max_20_high
            ORDER BY strength ASC, volume_mult DESC
        """
    },
    "downward-breakout-delivery": {
        "id": "downward-breakout-delivery",
        "title": "Downward Breakdown (20D Delivery)",
        "description": "Price breaks below 20-day low with high *delivery* volume.",
        "columns": BREAKOUT_COLUMNS,
        "sql": """
            WITH ohlcv_lag AS (
                SELECT *,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close_calc
                FROM daily_ohlcv
            ),
            tr_calc AS (
                SELECT *,
                       GREATEST(
                           high - low,
                           ABS(high - prev_close_calc),
                           ABS(low - prev_close_calc)
                        ) as tr
                FROM ohlcv_lag
            ),
            indicators AS (
                SELECT *,
                       -- ATR (14)
                       AVG(tr) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as atr,
                       -- 20 Day Low (Excluding today)
                       MIN(low) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as min_20_low,
                       -- 20 Day Avg Delivery Volume (Excluding today)
                       AVG(delivery_volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_20_del_vol
                FROM tr_calc
            )
            SELECT 
                symbol,
                date,
                close,
                ROUND((close - prev_close) / prev_close * 100, 2) as change_pct,
                volume,
                delivery_pct,
                ROUND(delivery_volume / avg_20_del_vol, 2) as volume_mult,
                CASE 
                    WHEN (min_20_low - close) > atr AND delivery_volume > (avg_20_del_vol * 1.5) THEN 'Full'
                    WHEN (min_20_low - close) > atr THEN 'Partial (Low Volume)'
                    ELSE 'Partial (Small Size)'
                END as strength
            FROM indicators
            WHERE date = (SELECT MAX(date) FROM daily_ohlcv)
              AND close < min_20_low
            ORDER BY strength ASC, volume_mult DESC
        """
    },
    "relative-weakness": {
        "id": "relative-weakness",
        "title": "Relative Weakness (vs Nifty)",
        "description": "Stocks showing weakness relative to Nifty 50: underperformance ≥1.2%, close in bottom 30% of range, high delivery volume.",
        "columns": [
            {"key": "symbol", "label": "Symbol", "type": "symbol"},
            {"key": "close", "label": "Price", "type": "currency"},
            {"key": "changePct", "label": "Change %", "type": "percent"},
            {"key": "relativeReturn", "label": "Rel. Return", "type": "percent"},
            {"key": "closeLocation", "label": "Close Loc %", "type": "percent"},
            {"key": "deliveryMult", "label": "Del Vol Multiple", "type": "multiplier"},
            {"key": "deliveryPct", "label": "Delivery %", "type": "percent"},
            {"key": "date", "label": "Date", "type": "date"},
        ],
        "sql": """
            WITH latest_date AS (
                SELECT MAX(date) as max_date FROM daily_ohlcv
            ),
            nifty_return AS (
                SELECT 
                    date,
                    (close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date) * 100 as nifty_pct
                FROM index_ohlcv
                WHERE index_name = 'NIFTY 50'
            ),
            stock_analysis AS (
                SELECT 
                    d.symbol, 
                    d.date, 
                    d.close, 
                    d.prev_close,
                    d.high,
                    d.low,
                    d.delivery_volume,
                    d.delivery_pct,
                    -- Stock return %
                    CASE WHEN d.prev_close > 0 
                         THEN (d.close - d.prev_close) / d.prev_close * 100 
                         ELSE 0 END as stock_pct,
                    -- Close location: 0 = low, 1 = high
                    CASE WHEN (d.high - d.low) > 0 
                         THEN (d.close - d.low) / (d.high - d.low) 
                         ELSE 0.5 END as close_loc,
                    -- 20-day avg delivery volume (excluding today)
                    AVG(d.delivery_volume) OVER (
                        PARTITION BY d.symbol 
                        ORDER BY d.date 
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) as avg_20_del_vol
                FROM daily_ohlcv d
            )
            SELECT 
                s.symbol,
                s.close,
                ROUND(s.stock_pct, 2) as change_pct,
                ROUND(s.stock_pct - n.nifty_pct, 2) as relative_return,
                ROUND(s.close_loc * 100, 1) as close_location,
                ROUND(s.delivery_volume / NULLIF(s.avg_20_del_vol, 0), 2) as delivery_mult,
                s.delivery_pct,
                s.date
            FROM stock_analysis s
            CROSS JOIN latest_date ld
            LEFT JOIN nifty_return n ON s.date = n.date
            WHERE s.date = ld.max_date
              -- Condition 1: Relative weakness vs Nifty <= -1.2%
              AND (s.stock_pct - COALESCE(n.nifty_pct, 0)) <= -1.2
              -- Condition 2: Close in bottom 30% of day range
              AND s.close_loc <= 0.30
              -- Condition 3: Delivery volume >= 1.5x 20-day avg
              AND s.delivery_volume >= (s.avg_20_del_vol * 1.5)
              -- Minimum price filter
              AND s.close > 20
            ORDER BY (s.stock_pct - COALESCE(n.nifty_pct, 0)) ASC, delivery_mult DESC
        """
    }
}

def get_screen(screen_id):
    """Get screen detail by ID."""
    return SCREENS.get(screen_id)

def list_screens():
    """List all available screens."""
    return [
        {
            "id": k, 
            "title": v["title"], 
            "description": v["description"]
        } 
        for k, v in SCREENS.items()
    ]
