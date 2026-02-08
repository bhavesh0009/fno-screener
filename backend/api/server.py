"""Flask API server for stock screener data."""
from flask import Flask, jsonify, request
from flask_cors import CORS
import duckdb
from decimal import Decimal
from pathlib import Path

from api.screens import get_screen, list_screens
from common.config import DB_PATH

app = Flask(__name__)
CORS(app)


def get_db():
    """Get database connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


@app.route("/api/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok"})


@app.route("/api/stats")
def get_stats():
    """Get database statistics."""
    conn = get_db()
    
    stock_count = conn.execute("SELECT COUNT(*) FROM fno_stocks").fetchone()[0]
    
    # Get the latest date and count positive stocks on that date
    latest_date = conn.execute("SELECT MAX(date) FROM daily_ohlcv").fetchone()[0]
    
    # Count stocks with positive change on latest date
    positive_count = 0
    if latest_date:
        positive_count = conn.execute("""
            SELECT COUNT(*) FROM daily_ohlcv 
            WHERE date = ? 
              AND prev_close IS NOT NULL 
              AND prev_close > 0
              AND close > prev_close
        """, [latest_date]).fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "stockCount": stock_count,
        "positiveCount": positive_count,
        "lastUpdated": str(latest_date) if latest_date else None
    })


@app.route("/api/screens")
def get_screens_list():
    """List available screens."""
    return jsonify(list_screens())


@app.route("/api/screens/<screen_id>/run")
def run_screen(screen_id):
    """Run a specific screen."""
    screen = get_screen(screen_id)
    if not screen:
        return jsonify({"error": "Screen not found"}), 404
        
    conn = get_db()
    try:
        cursor = conn.execute(screen["sql"])
        results = cursor.fetchall()
        
        # Get SQL column names from cursor description
        sql_columns = [desc[0] for desc in cursor.description]
        
        # Create mapping from SQL column names to screen column keys
        # SQL uses snake_case, screen uses camelCase
        snake_to_camel = {
            "symbol": "symbol",
            "date": "date", 
            "close": "close",
            "change_pct": "changePct",
            "volume": "volume",
            "delivery_pct": "deliveryPct",
            "volume_mult": "volumeMult",
            "delivery_mult": "deliveryMult",
            "strength": "strength",
            "relative_return": "relativeReturn",
            "close_location": "closeLocation",
        }
        
        data = []
        for row in results:
            item = {}
            for i, sql_col in enumerate(sql_columns):
                val = row[i]
                # Convert SQL column name to camelCase key
                key = snake_to_camel.get(sql_col, sql_col)
                
                # Auto-convert based on Python type
                if val is None:
                    item[key] = None
                elif hasattr(val, 'isoformat'):  # date/datetime
                    item[key] = str(val)
                elif isinstance(val, (int, float, Decimal)):
                    item[key] = float(val)
                else:
                    item[key] = val
            data.append(item)
            
        return jsonify({
            "screen": screen["title"],
            "count": len(data),
            "columns": screen.get("columns", []),
            "results": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/stocks")
def get_stocks():
    """Get list of F&O stocks with latest data and enhanced metrics."""
    conn = get_db()
    
    # Get pagination params
    page = request.args.get("page", 1, type=int)
    limit = request.args.get("limit", 50, type=int)
    search = request.args.get("search", "", type=str)
    sort_by = request.args.get("sortBy", "symbol", type=str)
    sort_order = request.args.get("sortOrder", "asc", type=str)
    
    offset = (page - 1) * limit
    
    # Validate sort column and map to proper table alias
    column_mapping = {
        "symbol": "m.symbol",
        "company_name": "s.company_name",
        "close": "m.close",
        "change_pct": "m.change_pct",
        "volume": "m.volume",
        "date": "m.date",
        "delivery_pct": "m.delivery_pct",
        "ytd_pct": "m.ytd_pct",
        "pct_1y": "m.pct_1y",
        "pct_1m": "m.pct_1m",
        "delta_52w_high": "m.delta_52w_high",
        "rs_rank": "m.rs_rank"
    }
    if sort_by not in column_mapping:
        sort_by = "symbol"
    sort_column = column_mapping[sort_by]
    
    if sort_order.lower() not in ["asc", "desc"]:
        sort_order = "asc"
    
    # Build search filter
    search_filter = ""
    if search:
        search_filter = f"AND (s.symbol ILIKE '%{search}%' OR s.company_name ILIKE '%{search}%')"
    
    # Get Nifty 50 1-year return for RS Rank calculation
    nifty_1y_query = """
        WITH nifty_data AS (
            SELECT 
                date,
                close,
                ROW_NUMBER() OVER (ORDER BY date DESC) as rn
            FROM index_ohlcv 
            WHERE index_name = 'NIFTY 50'
        )
        SELECT 
            (SELECT close FROM nifty_data WHERE rn = 1) as current_close,
            (SELECT close FROM nifty_data WHERE rn = 252) as close_1y_ago
    """
    nifty_result = conn.execute(nifty_1y_query).fetchone()
    nifty_1y_return = 0
    if nifty_result and nifty_result[0] and nifty_result[1]:
        nifty_1y_return = ((float(nifty_result[0]) - float(nifty_result[1])) / float(nifty_result[1])) * 100
    
    # Get stocks with enhanced metrics
    query = f"""
        WITH 
        -- Get the latest date in the dataset
        latest_date AS (
            SELECT MAX(date) as max_date FROM daily_ohlcv
        ),
        -- Get YTD start date (first trading day of 2026)
        ytd_start AS (
            SELECT MIN(date) as start_date 
            FROM daily_ohlcv 
            WHERE date >= DATE '2026-01-01'
        ),
        -- Calculate all metrics per stock
        stock_metrics AS (
            SELECT 
                d.symbol,
                d.date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.prev_close,
                d.volume,
                d.value,
                d.delivery_volume,
                d.delivery_pct,
                -- Daily change %
                CASE 
                    WHEN d.prev_close IS NOT NULL AND d.prev_close > 0 
                    THEN ROUND(((d.close - d.prev_close) / d.prev_close) * 100, 2)
                    ELSE NULL 
                END as change_pct,
                -- Row number for latest record
                ROW_NUMBER() OVER (PARTITION BY d.symbol ORDER BY d.date DESC) as rn,
                -- 52-week high (last 252 trading days)
                MAX(d.high) OVER (
                    PARTITION BY d.symbol 
                    ORDER BY d.date 
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) as high_52w,
                -- 20 SMA
                AVG(d.close) OVER (
                    PARTITION BY d.symbol 
                    ORDER BY d.date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as sma_20,
                -- 50 SMA  
                AVG(d.close) OVER (
                    PARTITION BY d.symbol 
                    ORDER BY d.date 
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) as sma_50,
                -- 200 SMA
                AVG(d.close) OVER (
                    PARTITION BY d.symbol 
                    ORDER BY d.date 
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) as sma_200
            FROM daily_ohlcv d
        ),
        -- Get 1Y ago, 1M ago, and YTD start prices
        historical_prices AS (
            SELECT 
                symbol,
                -- 1Y ago (approx 252 trading days)
                MAX(CASE WHEN rn = 252 THEN close END) as close_1y_ago,
                -- 1M ago (approx 21 trading days)
                MAX(CASE WHEN rn = 21 THEN close END) as close_1m_ago
            FROM stock_metrics
            GROUP BY symbol
        ),
        -- Get YTD start price
        ytd_prices AS (
            SELECT 
                d.symbol,
                d.close as ytd_start_close
            FROM daily_ohlcv d
            INNER JOIN ytd_start ys ON d.date = ys.start_date
        ),
        -- Combine all metrics
        final_metrics AS (
            SELECT 
                m.symbol,
                m.date,
                m.open,
                m.high,
                m.low,
                m.close,
                m.prev_close,
                m.change_pct,
                m.volume,
                m.value,
                m.delivery_volume,
                m.delivery_pct,
                -- YTD %
                CASE 
                    WHEN yp.ytd_start_close IS NOT NULL AND yp.ytd_start_close > 0
                    THEN ROUND(((m.close - yp.ytd_start_close) / yp.ytd_start_close) * 100, 2)
                    ELSE NULL
                END as ytd_pct,
                -- 1Y %
                CASE 
                    WHEN hp.close_1y_ago IS NOT NULL AND hp.close_1y_ago > 0
                    THEN ROUND(((m.close - hp.close_1y_ago) / hp.close_1y_ago) * 100, 2)
                    ELSE NULL
                END as pct_1y,
                -- 1M %
                CASE 
                    WHEN hp.close_1m_ago IS NOT NULL AND hp.close_1m_ago > 0
                    THEN ROUND(((m.close - hp.close_1m_ago) / hp.close_1m_ago) * 100, 2)
                    ELSE NULL
                END as pct_1m,
                -- Delta 52w High (always negative or zero)
                CASE 
                    WHEN m.high_52w IS NOT NULL AND m.high_52w > 0
                    THEN ROUND(((m.close - m.high_52w) / m.high_52w) * 100, 2)
                    ELSE NULL
                END as delta_52w_high,
                -- SMA position flags
                m.close > m.sma_20 as above_sma_20,
                m.close > m.sma_50 as above_sma_50,
                m.close > m.sma_200 as above_sma_200,
                -- RS Rank (stock 1Y return - Nifty 1Y return)
                CASE 
                    WHEN hp.close_1y_ago IS NOT NULL AND hp.close_1y_ago > 0
                    THEN ROUND(((m.close - hp.close_1y_ago) / hp.close_1y_ago) * 100 - {nifty_1y_return}, 2)
                    ELSE NULL
                END as rs_rank
            FROM stock_metrics m
            LEFT JOIN historical_prices hp ON m.symbol = hp.symbol
            LEFT JOIN ytd_prices yp ON m.symbol = yp.symbol
            WHERE m.rn = 1
        )
        SELECT 
            s.symbol,
            s.company_name,
            s.lot_size,
            m.date,
            m.open,
            m.high,
            m.low,
            m.close,
            m.prev_close,
            m.change_pct,
            m.volume,
            m.value,
            m.delivery_volume,
            m.delivery_pct,
            m.ytd_pct,
            m.pct_1y,
            m.pct_1m,
            m.delta_52w_high,
            m.above_sma_20,
            m.above_sma_50,
            m.above_sma_200,
            m.rs_rank
        FROM fno_stocks s
        INNER JOIN final_metrics m ON s.symbol = m.symbol
        WHERE 1=1 {search_filter}
        ORDER BY {sort_column} {sort_order}
        LIMIT {limit} OFFSET {offset}
    """
    
    result = conn.execute(query).fetchall()
    columns = ["symbol", "companyName", "lotSize", "date", "open", "high", "low", 
               "close", "prevClose", "changePct", "volume", "value", 
               "deliveryVolume", "deliveryPct", "ytdPct", "pct1Y", "pct1M",
               "delta52wHigh", "aboveSma20", "aboveSma50", "aboveSma200", "rsRank"]
    
    stocks = []
    for row in result:
        stock = dict(zip(columns, row))
        # Convert date to string
        if stock["date"]:
            stock["date"] = str(stock["date"])
        
        # Ensure numeric fields are floats, not strings or decimals
        numeric_fields = ["open", "high", "low", "close", "prevClose", "changePct", 
                          "volume", "value", "deliveryVolume", "deliveryPct",
                          "ytdPct", "pct1Y", "pct1M", "delta52wHigh", "rsRank"]
        for field in numeric_fields:
            if stock.get(field) is not None:
                try:
                    stock[field] = float(stock[field])
                except (ValueError, TypeError):
                    pass # Keep as is if conversion fails
        
        # Convert boolean fields
        for field in ["aboveSma20", "aboveSma50", "aboveSma200"]:
            if stock.get(field) is not None:
                stock[field] = bool(stock[field])

        stocks.append(stock)
    
    # Get sparkline data for all stocks in current page
    symbols = [s["symbol"] for s in stocks]
    if symbols:
        # Create a parameterized query for sparklines (last 252 days of closing prices)
        symbols_str = ", ".join([f"'{s}'" for s in symbols])
        sparkline_query = f"""
            WITH ranked AS (
                SELECT 
                    symbol,
                    date,
                    close,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
                FROM daily_ohlcv
                WHERE symbol IN ({symbols_str})
            )
            SELECT 
                symbol,
                LIST(close ORDER BY rn DESC) as prices
            FROM ranked
            WHERE rn <= 252
            GROUP BY symbol
        """
        sparkline_result = conn.execute(sparkline_query).fetchall()
        sparkline_map = {row[0]: [float(p) for p in row[1]] for row in sparkline_result}
        
        # Add sparklines to stock data
        for stock in stocks:
            stock["sparkline"] = sparkline_map.get(stock["symbol"], [])
    
    # Get total count for pagination
    count_query = f"""
        SELECT COUNT(*) FROM fno_stocks s
        WHERE 1=1 {search_filter}
    """
    total = conn.execute(count_query).fetchone()[0]
    
    conn.close()
    
    return jsonify({
        "stocks": stocks,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "totalPages": (total + limit - 1) // limit
        }
    })


@app.route("/api/stocks/<symbol>")
def get_stock_detail(symbol):
    """Get detailed data for a specific stock."""
    conn = get_db()
    
    # Get stock info
    stock_info = conn.execute("""
        SELECT symbol, company_name, lot_size, last_updated
        FROM fno_stocks WHERE symbol = ?
    """, [symbol.upper()]).fetchone()
    
    if not stock_info:
        conn.close()
        return jsonify({"error": "Stock not found"}), 404
    
    # Get historical data (last 30 days by default)
    days = request.args.get("days", 30, type=int)
    
    history = conn.execute(f"""
        SELECT date, open, high, low, close, prev_close, volume, value, 
               delivery_volume, delivery_pct,
               ROUND(((close - prev_close) / prev_close) * 100, 2) as change_pct
        FROM daily_ohlcv 
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT {days}
    """, [symbol.upper()]).fetchall()
    
    conn.close()
    
    columns = ["date", "open", "high", "low", "close", "prevClose", "volume", 
               "value", "deliveryVolume", "deliveryPct", "changePct"]
    
    history_data = []
    for row in history:
        record = dict(zip(columns, row))
        record["date"] = str(record["date"])
        history_data.append(record)
    
    return jsonify({
        "symbol": stock_info[0],
        "companyName": stock_info[1],
        "lotSize": stock_info[2],
        "lastUpdated": str(stock_info[3]) if stock_info[3] else None,
        "history": history_data
    })


if __name__ == "__main__":
    print(f"Starting API server...")
    print(f"Database: {DB_PATH}")
    app.run(host="0.0.0.0", port=5001, debug=True)
