"""
Proof of Concept: Explore fno_security_in_ban_period from nselib
This script explores the data structure returned by the function.
"""

from nselib import derivatives
from datetime import datetime, timedelta
import json

def explore_ban_period_data():
    """Fetch and explore the ban period data structure."""
    
    print("=" * 60)
    print("Exploring fno_security_in_ban_period from nselib")
    print("=" * 60)
    
    # Try multiple dates to understand the data pattern
    dates_to_try = []
    for i in range(30):  # Try last 30 days
        d = datetime.now() - timedelta(days=i)
        dates_to_try.append(d.strftime('%d-%m-%Y'))
    
    results = {}
    
    print("\n1. Fetching data for the last 30 days...")
    print("-" * 60)
    
    for trade_date in dates_to_try:
        try:
            data = derivatives.fno_security_in_ban_period(trade_date=trade_date)
            if data is not None and len(data) > 0:
                results[trade_date] = data
                print(f"   {trade_date}: {len(data)} stocks in ban - {data}")
            else:
                print(f"   {trade_date}: No stocks in ban (or no data available)")
        except Exception as e:
            print(f"   {trade_date}: Error - {e}")
    
    print("\n" + "=" * 60)
    print("2. Summary of Ban Period Data")
    print("=" * 60)
    
    print(f"\n   Dates with stocks in ban: {len(results)}")
    
    if results:
        # Collect all unique stocks that were ever in ban
        all_banned_stocks = set()
        for date, stocks in results.items():
            all_banned_stocks.update(stocks)
        
        print(f"   Unique stocks found in ban period: {sorted(all_banned_stocks)}")
        
        # Show frequency of each stock in ban
        stock_counts = {}
        for date, stocks in results.items():
            for stock in stocks:
                stock_counts[stock] = stock_counts.get(stock, 0) + 1
        
        print(f"\n   Stock occurrence counts:")
        for stock, count in sorted(stock_counts.items(), key=lambda x: -x[1]):
            print(f"      {stock}: {count} days")
    
    print("\n" + "=" * 60)
    print("3. Data Structure Analysis")
    print("=" * 60)
    
    if results:
        sample_data = list(results.values())[0]
        print(f"\n   Return type: {type(sample_data)}")
        print(f"   Element type: {type(sample_data[0]) if sample_data else 'N/A'}")
        print(f"   Sample value: {sample_data}")
    else:
        print("\n   No data collected to analyze")
    
    print("\n" + "=" * 60)
    print("4. Potential Table Schema")
    print("=" * 60)
    
    print("""
   Based on the data structure, here's a potential table schema:
   
   CREATE TABLE fno_ban_period (
       id SERIAL PRIMARY KEY,
       trade_date DATE NOT NULL,
       symbol VARCHAR(50) NOT NULL,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       UNIQUE(trade_date, symbol)
   );
   
   -- Or for historical tracking with entry/exit dates:
   CREATE TABLE fno_ban_history (
       id SERIAL PRIMARY KEY,
       symbol VARCHAR(50) NOT NULL,
       ban_start_date DATE NOT NULL,
       ban_end_date DATE,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   """)
    
    print("=" * 60)
    print("Exploration complete!")
    print("=" * 60)
    
    return results


if __name__ == "__main__":
    data = explore_ban_period_data()
