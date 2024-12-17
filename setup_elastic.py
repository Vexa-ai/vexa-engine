from bm25_search import ElasticsearchBM25
import asyncio

def setup_elastic():
    search_engine = ElasticsearchBM25()

    # Drop existing index if exists
    if search_engine.es_client and search_engine.es_client.indices.exists(index=search_engine.index_name):
        print(f"Dropping index: {search_engine.index_name}")
        search_engine.es_client.indices.delete(index=search_engine.index_name)
    
    # Create new index
    search_engine.create_index()
    
    print("Elasticsearch index setup completed successfully")

if __name__ == "__main__":
    setup_elastic() # No need for asyncio since ES client is synchronous 