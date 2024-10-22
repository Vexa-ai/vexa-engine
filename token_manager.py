import uuid
from typing import List, Optional, Tuple
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models
from pydantic import BaseModel, Field
from vexa import VexaAPI

class UserToken(BaseModel):
    token: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    user_name: str

class TokenManager:
    def __init__(self, host: str = "127.0.0.1", port: int = 6333, collection_name: str = "user_tokens"):
        self.client = AsyncQdrantClient(host, port=port)
        self.collection_name = collection_name

    async def _ensure_collection_exists(self):
        collections = await self.client.get_collections()
        if not any(collection.name == self.collection_name for collection in collections.collections):
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=1, distance=models.Distance.DOT),
            )

    async def submit_token(self, token: str) -> Tuple[str | None, str | None]:
        vexa = VexaAPI(token=token)
        await vexa.get_user_info()
        
        if vexa.user_id is None or vexa.user_name is None:
            print("Error: Token authentication failed")
            return None, None
        
        user_id = vexa.user_id
        user_name = vexa.user_name
        
        existing_token = await self.check_token(token)
        if existing_token:
            return user_id, user_name
        
        user_token = UserToken(token=token, user_id=user_id, user_name=user_name)
        await self._store_token(user_token)
        return user_id, user_name

    async def _store_token(self, token: UserToken):
        await self.client.upsert(
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

    async def check_token(self, token: str) -> Optional[Tuple[str, str]]:
        results = await self.client.retrieve(
            collection_name=self.collection_name,
            ids=[token],
        )
        if results:
            return results[0].payload["user_id"], results[0].payload["user_name"]
        return None

    async def get_user_tokens(self, user_id: str) -> List[str]:
        results = await self.client.scroll(
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
        return [point.id for point in results[0]]

    async def revoke_token(self, token: str) -> bool:
        try:
            await self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(points=[token]),
            )
            return True
        except Exception:
            return False

    async def revoke_all_user_tokens(self, user_id: str) -> int:
        tokens = await self.get_user_tokens(user_id)
        revoked_count = 0
        for token in tokens:
            if await self.revoke_token(token):
                revoked_count += 1
        return revoked_count

    async def drop_collection(self) -> bool:
        try:
            await self.client.delete_collection(collection_name=self.collection_name)
            return True
        except Exception as e:
            print(f"Error dropping collection: {e}")
            return False
