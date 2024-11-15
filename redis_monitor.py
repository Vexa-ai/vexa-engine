from redis import Redis
import time
import os
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

class RedisMonitor:
    def __init__(self):
        self.redis = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
        self.processing_key = 'meetings:processing'

    def get_processing_count(self):
        """Get number of meetings currently being processed"""
        return self.redis.scard(self.processing_key)

    def get_processing_meetings(self):
        """Get list of meetings being processed"""
        return list(self.redis.smembers(self.processing_key))

    def clear_processing_queue(self):
        """Clear all processing meetings (use carefully!)"""
        return self.redis.delete(self.processing_key)

    def monitor_live(self, interval=1):
        """Live monitoring of processing queue"""
        try:
            while True:
                processing = self.get_processing_meetings()
                print(f"\rProcessing {len(processing)} meetings: {processing}", end='', flush=True)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")

# Usage example:
if __name__ == "__main__":
    monitor = RedisMonitor()
    
    # Print current state
    print(f"Processing count: {monitor.get_processing_count()}")
    print(f"Processing meetings: {monitor.get_processing_meetings()}")
    
    # Live monitor
    # monitor.monitor_live()
