import uuid
from datetime import datetime
from typing import List, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models
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
        self.client = QdrantClient(host, port=port)
        self.collection_name = collection_name
        self._ensure_collection_exists()

    def _ensure_collection_exists(self):
        collections = self.client.get_collections().collections
        if not any(collection.name == self.collection_name for collection in collections):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=1, distance=models.Distance.DOT),
            )

    def upsert_thread(self, user_id: str, messages: List[Msg], thread_name: Optional[str] = None, thread_id: Optional[str] = None) -> str:
        if thread_id:
            existing_thread = self.get_thread(thread_id)
            if existing_thread:
                # Update existing thread
                thread = SearchAssistantThread(
                    thread_id=thread_id,
                    user_id=user_id,
                    thread_name=existing_thread.thread_name,  # Keep the existing thread_name
                    messages=messages,
                    timestamp=existing_thread.timestamp
                )
            else:
                # Create new thread with supplied ID
                thread = SearchAssistantThread(
                    thread_id=thread_id,
                    user_id=user_id,
                    thread_name=thread_name or "",  # Use provided thread_name or empty string
                    messages=messages
                )
        else:
            # Create new thread with auto-generated ID
            thread = SearchAssistantThread(
                user_id=user_id,
                thread_name=thread_name or "",  # Use provided thread_name or empty string
                messages=messages
            )

        # Upsert the thread
        self.client.upsert(
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

    def get_thread(self, thread_id: str) -> Optional[SearchAssistantThread]:
        results = self.client.retrieve(
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

    def get_user_threads(self, user_id: str) -> List[dict]:
        results = self.client.scroll(
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
        )[0]
        threads = [
            {
                "thread_id": point.id,
                "thread_name": point.payload["thread_name"],
                "timestamp": datetime.fromisoformat(point.payload["timestamp"]),
            }
            for point in results
        ]
        # Sort threads by timestamp in descending order
        return sorted(threads, key=lambda x: x["timestamp"], reverse=True)

    def get_messages_by_thread_id(self, thread_id: str) -> Optional[List[Msg]]:
        thread = self.get_thread(thread_id)
        if thread:
            return thread.messages
        return None

    def delete_thread(self, thread_id: str) -> bool:
        """
        Delete a thread by its ID.
        
        Args:
            thread_id (str): The ID of the thread to delete.
        
        Returns:
            bool: True if the thread was successfully deleted, False otherwise.
        """
        try:
            self.client.delete(
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
