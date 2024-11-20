from indexing.meetings_monitor import MeetingsMonitor
import asyncio
import logging
from datetime import datetime, timezone



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def monitor_loop():
    monitor = MeetingsMonitor()
    
    while True:
        try:
            await monitor.sync_meetings()
            await monitor.sync_meetings_queue(last_days=60)
            await monitor.print_queue_status()
            await asyncio.sleep(5)  # Check every 30 seconds
        except KeyboardInterrupt:
            logging.info("Monitor shutdown requested")
            break
        except Exception as e:
            logging.error(f"Monitor error: {e}")
            await asyncio.sleep(5)  # Brief pause on error

if __name__ == "__main__":
    asyncio.run(monitor_loop()) 
    
#conda activate langchain; python run_monitor.py