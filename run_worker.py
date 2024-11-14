from indexing.worker import IndexingWorker
from redis import Redis
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    redis = Redis(host='localhost', port=6379, db=0)
    worker = IndexingWorker(redis, max_concurrent=3)
    
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