from qdrant_search import QdrantSearchEngine
import asyncio

async def setup_qdrant():
    search_engine = QdrantSearchEngine()

    # Initialize new collection with proper schema
    await search_engine.drop_collection()
    await search_engine.create_collection()
    
    print("Qdrant collection setup completed successfully")

if __name__ == "__main__":
    asyncio.run(setup_qdrant()) 