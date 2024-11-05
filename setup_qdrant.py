from qdrant_search import QdrantSearchEngine
import asyncio

async def setup_qdrant():
    search_engine = QdrantSearchEngine()

    # Check if collection exists
    try:
        collection_info = await search_engine.client.get_collection(search_engine.collection_name)
        if collection_info:
            print(f"Collection '{search_engine.collection_name}' exists, dropping it...")
            await search_engine.drop_collection()
    except Exception:
        print(f"Collection '{search_engine.collection_name}' does not exist")

    # Initialize new collection with proper schema
    print(f"Initializing collection '{search_engine.collection_name}'...")
    await search_engine.initialize()
    
    print("Qdrant collection setup completed successfully")

if __name__ == "__main__":
    asyncio.run(setup_qdrant()) 