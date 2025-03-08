import pytest
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import User, AccessLevel, ExternalIDType, Transcript, TranscriptAccess, Content, UserContent
from routers.transcripts import router, get_transcript_manager, InvalidExternalIDTypeError
from dashboard.services.transcript import TranscriptManager
from dashboard.services.psql_helpers import get_session
from auth import get_current_user
from typing import List, Dict, Optional, Any

@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(router)
    return app

@pytest.fixture
def test_client(app):
    return TestClient(app)

@pytest.fixture
def mock_user():
    return User(id=UUID('12345678-1234-5678-1234-567812345678'))

@pytest.fixture
def mock_segments():
    return [
        {
            "content": "Test content 1",
            "start_timestamp": "2025-02-13T15:32:42.170000+00:00",
            "end_timestamp": "2025-02-13T15:32:45.170000+00:00",
            "confidence": 0.95,
            "segment_id": 1,
            "words": [["test", 0.0, 1.0]],
            "speaker": "Speaker 1",
            "html_content": "<p>Test content 1</p>",
            "html_content_short": "Test content 1",
            "keywords": []
        },
        {
            "content": "Test content 2",
            "start_timestamp": "2025-02-13T15:32:58.170000+00:00",
            "end_timestamp": "2025-02-13T15:33:01.390000+00:00",
            "confidence": 0.98,
            "segment_id": 2,
            "words": [["test", 0.0, 1.0]],
            "speaker": "Speaker 2",
            "meeting_id": "meeting-123",
            "server_timestamp": "2025-02-13T15:33:01.390000+00:00",
            "transcription_timestamp": "2025-02-13T15:33:01.390000+00:00",
            "present_user_ids": [],
            "partially_present_user_ids": []
        }
    ]

@pytest.fixture
def mock_session():
    class MockSession:
        def __init__(self):
            self.transcripts = {}
            self.transcript_access = {}
            self.content = {}
            self.user_content = {}

        def add(self, obj):
            if isinstance(obj, Transcript):
                self.transcripts[obj.id] = obj
                print(f"Added transcript: {obj.id}")
            elif isinstance(obj, TranscriptAccess):
                key = (obj.transcript_id, obj.user_id)
                if key in self.transcript_access:
                    # Update existing access
                    self.transcript_access[key].access_level = obj.access_level
                    print(f"Updated access for transcript {obj.transcript_id}, user {obj.user_id}: {obj.access_level}")
                else:
                    # Create new access
                    self.transcript_access[key] = obj
                    print(f"Added access for transcript {obj.transcript_id}, user {obj.user_id}: {obj.access_level}")
                    print(f"Current transcript_access: {[(k, v.access_level) for k, v in self.transcript_access.items()]}")
            elif isinstance(obj, Content):
                self.content[obj.id] = obj
            elif isinstance(obj, UserContent):
                self.user_content[obj.content_id] = obj

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        async def commit(self):
            pass

        async def rollback(self):
            pass

        async def execute(self, query):
            print(f"\nExecuting query: {str(query)}")
            print(f"Transcript access records: {[(k, v.access_level) for k, v in self.transcript_access.items()]}")
            print(f"Transcripts: {list(self.transcripts.keys())}")
            
            class Result:
                def __init__(self, data=None):
                    self.data = data

                def scalar_one_or_none(self):
                    return self.data[0] if isinstance(self.data, list) and self.data else self.data

                def scalars(self):
                    class All:
                        def __init__(self, data):
                            self.data = data if isinstance(data, list) else [data] if data else []
                        def all(self):
                            return self.data
                    return All(self.data)

                def all(self):
                    return self.data if isinstance(self.data, list) else []

            # Extract query conditions
            transcript_id = None
            user_id = None
            access_level = None

            for criterion in query._where_criteria:
                if hasattr(criterion, 'left') and hasattr(criterion, 'right'):
                    left_str = str(criterion.left).lower()
                    if "transcript.id" in left_str or "transcript_id" in left_str:
                        transcript_id = criterion.right.value
                    elif "user_id" in left_str:
                        user_id = criterion.right.value
                    elif "access_level" in left_str:
                        access_level = criterion.right.value

            print(f"Query conditions: transcript_id={transcript_id}, user_id={user_id}, access_level={access_level}")

            # Handle TranscriptAccess queries
            if "FROM transcript_access" in str(query):
                key = (transcript_id, user_id)
                access = self.transcript_access.get(key)
                print(f"Looking for access with key {key}: {access is not None}")
                if access:
                    print(f"Found access level: {access.access_level}")
                if access and (not access_level or access.access_level == access_level):
                    return Result(access)
                return Result(None)

            # Handle transcript queries
            if "FROM transcript" in str(query):
                if transcript_id:
                    transcript = self.transcripts.get(transcript_id)
                    print(f"Found transcript: {transcript is not None}")
                    if not transcript:
                        return Result(None)

                    if "join transcript_access" in str(query).lower():
                        # Check if there's any access record for this transcript with the right access level
                        access = self.transcript_access.get((transcript_id, user_id))
                        print(f"Found access: {access is not None}")
                        if access:
                            print(f"Access level: {access.access_level}, Required: {access_level}")
                        if access and access.access_level == access_level:
                            return Result(transcript)
                        return Result(None)
                    return Result(transcript)

            return Result(None)

    return MockSession()

def override_get_current_user(mock_user):
    async def _get_current_user():
        return mock_user
    return _get_current_user

def override_get_session(mock_session):
    async def _get_session():
        return mock_session
    return _get_session

def override_get_transcript_manager(mock_transcript_manager):
    async def _get_transcript_manager():
        return mock_transcript_manager
    return _get_transcript_manager

@pytest.fixture
def mock_transcript_manager(mock_session):
    class MockTranscriptManager:
        async def update_transcript_access(
            self,
            transcript_id: UUID,
            user_id: UUID,
            target_user_id: UUID,
            access_level: str,
            session: AsyncSession = None
        ) -> bool:
            print(f"\nMock transcript manager: update_transcript_access")
            print(f"transcript_id: {transcript_id}")
            print(f"user_id: {user_id}")
            print(f"target_user_id: {target_user_id}")
            print(f"access_level: {access_level}")
            print(f"Session transcript_access: {[(k, v.access_level) for k, v in session.transcript_access.items()]}")
            
            # Check if transcript exists
            transcript = session.transcripts.get(transcript_id)
            if not transcript:
                print("Transcript not found")
                raise ValueError("Transcript not found")
            
            # Check if user has owner access
            owner_key = (transcript_id, user_id)
            owner_access = session.transcript_access.get(owner_key)
            print(f"Owner access found: {owner_access is not None}")
            if owner_access:
                print(f"Owner access level: {owner_access.access_level}")
            
            if not owner_access or owner_access.access_level != AccessLevel.OWNER.value:
                print("Access denied - not owner")
                raise ValueError("Access denied")
            
            # Update or create target user access
            target_key = (transcript_id, target_user_id)
            if target_key in session.transcript_access:
                session.transcript_access[target_key].access_level = access_level
                print(f"Updated access for {target_key} to {access_level}")
            else:
                target_access = TranscriptAccess(
                    transcript_id=transcript_id,
                    user_id=target_user_id,
                    access_level=access_level,
                    granted_by=user_id
                )
                session.add(target_access)
                print(f"Added new access for {target_key} with {access_level}")
            return True

        async def get_transcript_segments(
            self,
            external_id: str,
            external_id_type: str,
            user_id: UUID,
            last_msg_timestamp: Optional[datetime] = None,
            session: AsyncSession = None,
            limit: int = 100
        ) -> List[Dict[str, Any]]:
            if external_id_type not in [e.value for e in ExternalIDType]:
                raise InvalidExternalIDTypeError(external_id_type)
            
            return [
                {
                    "id": "test-segment-1",
                    "speaker": "Speaker 1",
                    "content": "Test content",
                    "html_content": None,
                    "timestamp": "2025-02-13T15:32:42.170000Z",
                    "segment_id": 1
                }
            ]

        async def ingest_transcript_segments(
            self,
            external_id: str,
            external_id_type: str,
            segments: List[Dict[str, Any]],
            user_id: UUID,
            session: AsyncSession = None
        ) -> List[Dict[str, Any]]:
            if external_id_type not in [e.value for e in ExternalIDType]:
                raise InvalidExternalIDTypeError(external_id_type)
            
            return [{"id": "test-transcript-id", **segments[0]}]

    return MockTranscriptManager()

@pytest.mark.asyncio
async def test_ingest_transcript_segments(
    app,
    test_client,
    mock_user,
    mock_segments,
    mock_transcript_manager,
    mock_session
):
    # Override dependencies
    app.dependency_overrides[get_current_user] = override_get_current_user(mock_user)
    app.dependency_overrides[get_session] = override_get_session(mock_session)
    app.dependency_overrides[get_transcript_manager] = override_get_transcript_manager(mock_transcript_manager)
    
    # Test successful ingestion with unique external_id
    external_id = f"meeting-{uuid4()}"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    response = test_client.post(
        f"/api/transcripts/segments/{external_id_type}/{external_id}",
        json=mock_segments
    )
    assert response.status_code == 200
    assert "id" in response.json()[0]
    
    # Test invalid external_id_type
    response = test_client.post(
        f"/api/transcripts/segments/invalid_type/{external_id}",
        json=mock_segments
    )
    assert response.status_code == 400
    assert "Invalid external_id_type" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_transcript_segments(
    app,
    test_client,
    mock_user,
    mock_segments,
    mock_transcript_manager,
    mock_session
):
    # Override dependencies
    app.dependency_overrides[get_current_user] = override_get_current_user(mock_user)
    app.dependency_overrides[get_session] = override_get_session(mock_session)
    app.dependency_overrides[get_transcript_manager] = override_get_transcript_manager(mock_transcript_manager)
    
    # Test successful retrieval with unique external_id
    external_id = f"meeting-{uuid4()}"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    response = test_client.get(
        f"/api/transcripts/segments/{external_id_type}/{external_id}"
    )
    assert response.status_code == 200
    assert "id" in response.json()[0]
    
    # Test invalid external_id_type
    response = test_client.get(
        f"/api/transcripts/segments/invalid_type/{external_id}"
    )
    assert response.status_code == 400
    assert "Invalid external_id_type" in response.json()["detail"]

@pytest.mark.asyncio
async def test_update_transcript_access(
    app,
    test_client,
    mock_user,
    mock_transcript_manager,
    mock_session
):
    # Override dependencies
    app.dependency_overrides[get_current_user] = override_get_current_user(mock_user)
    app.dependency_overrides[get_session] = override_get_session(mock_session)
    app.dependency_overrides[get_transcript_manager] = override_get_transcript_manager(mock_transcript_manager)

    # Set up mock transcript and access
    transcript_id = UUID('12345678-1234-5678-1234-567812345678')
    content_id = uuid4()
    
    # Create transcript
    transcript = Transcript(
        id=transcript_id,
        content_id=content_id,
        text_content="Test content",
        start_timestamp=datetime.now(timezone.utc),
        end_timestamp=datetime.now(timezone.utc),
        confidence=0.95,
        original_segment_id=1,
        word_timing_data={"words": []},
        segment_metadata={}
    )
    mock_session.add(transcript)

    # Create owner access for the user
    owner_access = TranscriptAccess(
        transcript_id=transcript_id,
        user_id=mock_user.id,
        access_level=AccessLevel.OWNER.value,
        granted_by=mock_user.id
    )
    mock_session.add(owner_access)

    # Test successful access update
    target_user_id = UUID('87654321-4321-8765-4321-876543210987')  # Different UUID
    access_update = {
        "target_user_id": str(target_user_id),
        "access_level": AccessLevel.SHARED.value
    }
    print("\nSending request with access_update:", access_update)
    response = test_client.put(
        f"/api/transcripts/{transcript_id}/access",
        json=access_update
    )
    print("Response status:", response.status_code)
    print("Response content:", response.content)
    assert response.status_code == 200
    assert response.json() == {"status": "success"}

    # Verify access was updated
    target_access = mock_session.transcript_access.get((transcript_id, target_user_id))
    assert target_access is not None
    assert target_access.access_level == AccessLevel.SHARED.value

    # Test invalid access level
    invalid_access = {
        "target_user_id": str(target_user_id),
        "access_level": "invalid"
    }
    response = test_client.put(
        f"/api/transcripts/{transcript_id}/access",
        json=invalid_access
    )
    assert response.status_code == 422  # FastAPI validation error

    # Test non-existent transcript
    non_existent_id = uuid4()
    response = test_client.put(
        f"/api/transcripts/{non_existent_id}/access",
        json=access_update
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Transcript not found"

@pytest.mark.asyncio
async def test_get_transcript_segments_with_timestamp(
    app,
    test_client,
    mock_user,
    mock_segments,
    mock_transcript_manager,
    mock_session
):
    # Override dependencies
    app.dependency_overrides[get_current_user] = override_get_current_user(mock_user)
    app.dependency_overrides[get_session] = override_get_session(mock_session)
    app.dependency_overrides[get_transcript_manager] = override_get_transcript_manager(mock_transcript_manager)
    
    # Test with unique external_id and timestamp parameter
    external_id = f"meeting-{uuid4()}"
    external_id_type = ExternalIDType.GOOGLE_MEET.value
    last_msg_timestamp = "2025-02-13T15:33:00.000000Z"
    
    response = test_client.get(
        f"/api/transcripts/segments/{external_id_type}/{external_id}",
        params={"last_msg_timestamp": last_msg_timestamp}
    )
    
    assert response.status_code == 200
    segments = response.json()
    assert len(segments) > 0
    
    # Verify response format
    segment = segments[0]
    assert "speaker" in segment
    assert "content" in segment
    assert "timestamp" in segment
    assert isinstance(segment["timestamp"], str)
    assert "Z" in segment["timestamp"]  # Verify ISO format with UTC
    
    # Test invalid timestamp format - FastAPI returns 422 for validation errors
    response = test_client.get(
        f"/api/transcripts/segments/{external_id_type}/{external_id}",
        params={"last_msg_timestamp": "invalid-timestamp"}
    )
    assert response.status_code == 422  # FastAPI validation error 