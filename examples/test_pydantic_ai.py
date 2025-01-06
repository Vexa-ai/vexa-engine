import asyncio
import os
import uuid
from dotenv import load_dotenv
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
from agents import ChatAgent
from psql_models import User, async_session
async def create_test_user() -> str:
    """Create a test user and return their ID"""
    user_id = uuid.uuid4()
    user = User(
        id=user_id,
        email=f"test_{user_id}@example.com",
        username=f"test_user_{user_id}"
    )
    async with async_session() as session:
        async with session.begin():
            session.add(user)
            await session.commit()
    return str(user_id)
async def main():
    # Load environment variables
    load_dotenv()
    voyage_api_key = os.getenv("VOYAGE_API_KEY")
    if not voyage_api_key:
        raise ValueError("VOYAGE_API_KEY not found in environment variables")
    # Init search engines
    qdrant_engine = QdrantSearchEngine(voyage_api_key=voyage_api_key)
    es_engine = ElasticsearchBM25()
    # Create agent
    agent = ChatAgent(qdrant_engine=qdrant_engine, es_engine=es_engine)
    try:
        # Create test user
        user_id = await create_test_user()
        print(f"\nTest User ID: {user_id}")
        # Test simple query
        query = "Find discussions about machine learning from our meetings"
        print(f"\nQuery: {query}")
        print("Response:")
        result = await agent.chat(user_id=user_id, query=query)
        print(result.response)
        thread_id = result.thread_id
        # Test thread continuation
        print("\n\nTesting thread continuation...")
        # First message
        print("\nFirst message:")
        query = "What were the key points discussed about data processing?"
        result = await agent.chat(user_id=user_id, query=query)
        print(result.response)
        thread_id = result.thread_id
        # Follow-up
        print("\n\nFollow-up question:")
        query = "Can you elaborate on the first point?"
        result = await agent.chat(
            user_id=user_id,
            query=query,
            thread_id=thread_id
        )
        print(result.response)
    except Exception as e:
        print(f"\nError during execution: {str(e)}")
        raise
if __name__ == "__main__":
    asyncio.run(main()) 