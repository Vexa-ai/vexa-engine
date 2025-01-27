from bm25_search import ElasticsearchBM25
import asyncio

async def setup_elastic():
    search_engine = await ElasticsearchBM25.create()

    # Drop existing index if exists
    if search_engine.es_client:
        exists = await search_engine.es_client.indices.exists(index=search_engine.index_name)
        if exists:
            print(f"Dropping index: {search_engine.index_name}")
            await search_engine.es_client.indices.delete(index=search_engine.index_name)
    
    # Create new index
    await search_engine.create_index()
    
    print("Elasticsearch index setup completed successfully")
    
    # Properly close the client
    await search_engine.es_client.close()

if __name__ == "__main__":
    asyncio.run(setup_elastic())