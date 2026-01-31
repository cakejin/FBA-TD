#!/usr/bin/env python3
"""
Check for data gaps in the orderbook table.
This helps identify when the data collection was interrupted.
"""
import requests
import json
from datetime import datetime, timedelta

QUESTDB_URL = "http://localhost:9000/exec"

def query_questdb(sql):
    """Execute a query against QuestDB"""
    try:
        response = requests.get(QUESTDB_URL, params={"query": sql}, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Query error ({response.status_code}): {response.text[:100]}")
            return None
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def main():
    print("=" * 70)
    print("ORDERBOOK DATA ANALYSIS")
    print("=" * 70)
    
    # 1. Total row count
    result = query_questdb("SELECT COUNT(*) as total_rows FROM orderbook")
    if result and 'dataset' in result and result['dataset']:
        total_rows = result['dataset'][0][0]
        print(f"\n✓ Total rows in database: {total_rows:,}")
    else:
        print("✗ Failed to query database")
        return
    
    # 2. Time range
    result = query_questdb("SELECT MIN(ts) as min_time, MAX(ts) as max_time FROM orderbook")
    if result and 'dataset' in result and result['dataset']:
        min_time = result['dataset'][0][0]
        max_time = result['dataset'][0][1]
        print(f"\n✓ Data time range:")
        print(f"  Start: {min_time}")
        print(f"  End:   {max_time}")
        
        # Calculate duration
        try:
            start = datetime.fromisoformat(min_time.replace('Z', '+00:00'))
            end = datetime.fromisoformat(max_time.replace('Z', '+00:00'))
            duration = (end - start).total_seconds()
            hours = duration / 3600
            print(f"  Duration: {hours:.2f} hours ({duration:.0f} seconds)")
        except:
            pass
    
    # 3. Rows per symbol
    print(f"\n✓ Rows per symbol (top 20):")
    result = query_questdb("""
        SELECT symbol, COUNT(*) as count 
        FROM orderbook 
        GROUP BY symbol 
        ORDER BY count DESC 
        LIMIT 20
    """)
    if result and 'dataset' in result:
        for row in result['dataset']:
            print(f"  {row[0]}: {row[1]:,} rows")
    
    # 4. Ingestion rate
    print(f"\n✓ Recent ingestion rate (last 5 minutes):")
    result = query_questdb("""
        SELECT ts, COUNT(*) as row_count
        FROM orderbook 
        WHERE ts > now() - 300000000l
        GROUP BY ts
        ORDER BY ts DESC
        LIMIT 5
    """)
    if result and 'dataset' in result:
        for row in result['dataset']:
            print(f"  {row[0]}: {row[1]:,} rows")
    
    # 5. Last update time
    print(f"\n✓ Last update time:")
    result = query_questdb("SELECT MAX(ts) as last_update FROM orderbook")
    if result and 'dataset' in result and result['dataset']:
        last_update = result['dataset'][0][0]
        print(f"  {last_update}")
        
        # Time since last update
        try:
            last = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
            now = datetime.now(last.tzinfo)
            elapsed = (now - last).total_seconds()
            if elapsed < 60:
                print(f"  Status: ✓ ACTIVE (data {elapsed:.0f}s old)")
            elif elapsed < 300:
                print(f"  Status: ⚠ SLOW ({elapsed:.0f}s since last update)")
            else:
                print(f"  Status: ✗ INACTIVE ({elapsed:.0f}s since last update)")
        except:
            pass
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
