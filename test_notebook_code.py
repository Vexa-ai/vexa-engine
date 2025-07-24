import asyncio
import sys

# Add the search directory to the path
sys.path.append('search')
from bm25 import ElasticsearchBM25

async def test_notebook_code():
    """Test the original notebook code"""
    print("Testing original notebook code...")
    
    try:
        # Original notebook code
        es_engine = ElasticsearchBM25()
        await es_engine.create()
        
        print("✅ Original notebook code works!")
        print(f"✅ Index name: {es_engine.index_name}")
        print(f"✅ Connection status: {es_engine.is_connected()}")
        
        # Test health check
        health = await es_engine.health_check()
        print(f"✅ Health check: {health}")
        
        # Clean up
        await es_engine.close()
        print("✅ Connection closed")
        
    except Exception as e:
        print(f"❌ Notebook code failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_notebook_code()) 