from indexing.worker import IndexingWorker
from redis import Redis
from elasticsearch import AsyncElasticsearch
from qdrant_client import AsyncQdrantClient
import asyncio
import logging
import os

# Environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT'))
QDRANT_HOST = os.getenv('QDRANT_HOST')
QDRANT_PORT = int(os.getenv('QDRANT_PORT'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def init_search_clients():
    """Initialize search engine clients"""
    try:
        # Initialize Elasticsearch
        es_client = AsyncElasticsearch(
            hosts=[f'http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}']
        )
        
        # Initialize Qdrant
        qdrant_client = AsyncQdrantClient(
            host=QDRANT_HOST,
            port=QDRANT_PORT
        )
        
        # Test connections
        await asyncio.gather(
            es_client.info(),
            qdrant_client.get_collections()
        )
        
        return es_client, qdrant_client
        
    except Exception as e:
        logger.error(f"Failed to initialize search clients: {e}")
        raise

async def main():
    try:
        # Initialize Redis
        redis = Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            db=0,
            decode_responses=True
        )
        
        # Get Voyage API key from environment
        voyage_api_key = os.getenv('VOYAGE_API_KEY')
        if not voyage_api_key:
            raise ValueError("VOYAGE_API_KEY environment variable not set")
        
        # Initialize worker
        worker = IndexingWorker(
            redis=redis,
            qdrant_api_key=voyage_api_key,
            max_concurrent=1,
            debug=True
        )
        
        logger.info("Starting indexing worker...")
        await worker.run()
        
    except KeyboardInterrupt:
        logger.info("Worker shutdown requested")
    except Exception as e:
        logger.error(f"Worker error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
    
    
#conda activate langchain; python run_worker.py