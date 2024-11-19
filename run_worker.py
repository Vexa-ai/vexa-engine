from indexing.worker import IndexingWorker
from redis import Redis
import asyncio
import logging


import os
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    worker = IndexingWorker(redis, max_concurrent=20)
    
    try:
        await worker.run()
    except KeyboardInterrupt:
        logging.info("Worker shutdown requested")
    except Exception as e:
        logging.error(f"Worker error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 
    
    
#conda activate langchain; python run_worker.py