import uuid
from datetime import datetime
from typing import List, Optional
from qdrant_client import AsyncQdrantClient, models
from pydantic import BaseModel, Field
from core import Msg

class SearchAssistantThread(BaseModel):
    thread_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    thread_name: str
    messages: List[Msg]
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class ThreadManager:
    def __init__(self, host: str = "127.0.0.1", port: int = 6333, collection_name: str = "search_assistant_threads"):
        self.client = AsyncQdrantClient(host, port=port)
        self.collection_name = collection_name

    @classmethod
    async def create(cls, host: str = "127.0.0.1", port: int = 6333, collection_name: str = "search_assistant_threads"):
        self = cls(host, port, collection_name)
        await self._ensure_collection_exists()
        return self

    async def _ensure_collection_exists(self):
        collections = await self.client.get_collections()
        if not any(collection.name == self.collection_name for collection in collections.collections):
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=1, distance=models.Distance.DOT),
            )

    async def upsert_thread(self, user_id: str, messages: List[Msg], thread_name: Optional[str] = None, thread_id: Optional[str] = None) -> str:
        if thread_id:
            existing_thread = await self.get_thread(thread_id)
            if existing_thread:
                thread = SearchAssistantThread(
                    thread_id=thread_id,
                    user_id=user_id,
                    thread_name=existing_thread.thread_name,
                    messages=messages,
                    timestamp=existing_thread.timestamp
                )
            else:
                thread = SearchAssistantThread(
                    thread_id=thread_id,
                    user_id=user_id,
                    thread_name=thread_name or "",
                    messages=messages
                )
        else:
            thread = SearchAssistantThread(
                user_id=user_id,
                thread_name=thread_name or "",
                messages=messages
            )

        await self.client.upsert(
            collection_name=self.collection_name,
            points=[
                models.PointStruct(
                    id=thread.thread_id,
                    vector=[1.0],  # Dummy vector
                    payload={
                        "user_id": thread.user_id,
                        "thread_name": thread.thread_name,
                        "messages": [msg.__dict__ for msg in thread.messages],
                        "timestamp": thread.timestamp.isoformat(),
                    }
                )
            ]
        )
        print(f"Thread upserted with id {thread.thread_id}")
        return thread.thread_id

    async def get_thread(self, thread_id: str) -> Optional[SearchAssistantThread]:
        results = await self.client.retrieve(
            collection_name=self.collection_name,
            ids=[thread_id],
        )
        if results:
            point = results[0]
            return SearchAssistantThread(
                thread_id=point.id,
                user_id=point.payload["user_id"],
                thread_name=point.payload["thread_name"],
                messages=[Msg(**msg) for msg in point.payload["messages"]],
                timestamp=datetime.fromisoformat(point.payload["timestamp"]),
            )
        return None

    async def get_user_threads(self, user_id: str) -> List[dict]:
        results = await self.client.scroll(
            limit=100000,
            collection_name=self.collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="user_id",
                        match=models.MatchValue(value=user_id),
                    )
                ]
            ),
        )
        threads = [
            {
                "thread_id": point.id,
                "thread_name": point.payload["thread_name"],
                "timestamp": datetime.fromisoformat(point.payload["timestamp"]),
            }
            for point in results[0]
        ]
        return sorted(threads, key=lambda x: x["timestamp"], reverse=True)

    async def get_messages_by_thread_id(self, thread_id: str) -> Optional[List[Msg]]:
        thread = await self.get_thread(thread_id)
        if thread:
            return thread.messages
        return None

    async def delete_thread(self, thread_id: str) -> bool:
        try:
            await self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(
                    points=[thread_id]
                )
            )
            print(f"Thread deleted with id {thread_id}")
            return True
        except Exception as e:
            print(f"Error deleting thread {thread_id}: {str(e)}")
            return False
