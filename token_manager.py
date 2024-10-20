import uuid
from typing import List, Optional, Tuple
from qdrant_client import QdrantClient
from qdrant_client.http import models
from pydantic import BaseModel, Field
from vexa import VexaAPI

class UserToken(BaseModel):
    token: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    user_name: str
class TokenManager:
    def __init__(self, host: str = "127.0.0.1", port: int = 6333, collection_name: str = "user_tokens"):
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

    def submit_token(self, token: str) -> Tuple[str | None, str | None]:
        vexa = VexaAPI(token=token)
        
        if vexa.user_id is None or vexa.user_name is None:
            print("Error: Token authentication failed")
            return None, None
        
        user_id = vexa.user_id
        user_name = vexa.user_name
        
        if self.check_token(token):
            return user_id, user_name
        
        user_token = UserToken(token=token, user_id=user_id, user_name=user_name)
        self._store_token(user_token)
        return user_id, user_name

    def _store_token(self, token: UserToken):
        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                models.PointStruct(
                    id=token.token,
                    vector=[1.0],  # Dummy vector
                    payload={
                        "user_id": token.user_id,
                        "user_name": token.user_name,
                    }
                )
            ]
        )

    def check_token(self, token: str) -> Optional[str]:
        results = self.client.retrieve(
            collection_name=self.collection_name,
            ids=[token],
        )
        if results:
            return results[0].payload["user_id"], results[0].payload["user_name"]
        return None

    def get_user_tokens(self, user_id: str) -> List[str]:
        results = self.client.scroll(
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
        return [point.id for point in results]

    def revoke_token(self, token: str) -> bool:
        try:
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(points=[token]),
            )
            return True
        except Exception:
            return False

    def revoke_all_user_tokens(self, user_id: str) -> int:
        tokens = self.get_user_tokens(user_id)
        revoked_count = 0
        for token in tokens:
            if self.revoke_token(token):
                revoked_count += 1
        return revoked_count

    def drop_collection(self) -> bool:
        try:
            self.client.delete_collection(collection_name=self.collection_name)
            return True
        except Exception as e:
            print(f"Error dropping collection: {e}")
            return False
