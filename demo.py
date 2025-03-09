#!/usr/bin/env python3
import os
import sys
import time
import asyncio
import pandas as pd

# Add app to path
sys.path.append('/app')

from services.psql_helpers import read_table_async
from models.db import Transcript

async def monitor_transcript_table(refresh_rate: float = 1.0, limit: int = 10):
    """Monitor the Transcript table with periodic refresh and pretty printing.
    
    Args:
        refresh_rate: Seconds between refreshes
        limit: Maximum rows to display
    """
    while True:
        # Clear screen - works on Unix/Mac/Windows
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Get current time for display
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"Table: transcript | Updated: {current_time}")
        print("-" * 80)
        
        # Fetch data
        df = await read_table_async(Transcript)
        
        # Apply filters
        if df.empty:
            print("No data found in transcript table")
        else:
            # Select specific columns
            columns = ['start_timestamp', 'speaker', 'text_content']
            df = df[columns]
            
            # Sort by start_timestamp
            df = df.sort_values(by='start_timestamp', ascending=True)
            
            # Limit rows
            if limit > 0:
                df = df.tail(limit)
            
            # Display with good formatting
            pd.set_option('display.max_colwidth', 100)
            pd.set_option('display.width', 1000)
            print(df)
        
        # Wait before next refresh
        await asyncio.sleep(refresh_rate)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_transcript_table())
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    except Exception as e:
        print(f"Error: {e}")