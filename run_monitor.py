from indexing.meetings_monitor import MeetingsMonitor
import asyncio
import logging
from datetime import datetime, timezone



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def monitor_loop():
    monitor = MeetingsMonitor(test_user_id="ef7c085b-fdb5-4c94-b7b6-a61a3d04c210")
    
    while True:
       # try:
        await monitor.sync_meetings(overlap_minutes=5*60)
        await monitor.sync_meetings_queue(last_days=30)
        await monitor.print_queue_status()
        await asyncio.sleep(15)  # Check every 30 seconds
        # except KeyboardInterrupt:
        #     logging.info("Monitor shutdown requested")
        #     break
        # except Exception as e:
        #     logging.error(f"Monitor error: {e}")
        #     await asyncio.sleep(60)  # Brief pause on error

if __name__ == "__main__":
    asyncio.run(monitor_loop()) 
    
#conda activate langchain; python run_monitor.py