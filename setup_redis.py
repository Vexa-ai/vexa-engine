from indexing.meetings_monitor import MeetingsMonitor

from datetime import datetime, timezone

 

if __name__ == "__main__":
   monitor = MeetingsMonitor()
   monitor.flush_queues()
