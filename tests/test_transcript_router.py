import pytest
from uuid import UUID
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import User, AccessLevel
from routers.transcripts import router, get_transcript_manager
from transcript_manager import TranscriptManager
from psql_helpers import get_session
from auth import get_current_user

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
    return User(id=UUID('12345678-1234-5678-1234-567812345678'), email='test@test.com')

@pytest.fixture
def mock_segments():
    return [
        {
            'content': 'Test content 1',
            'start_timestamp': '2025-02-13T15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13T15:32:42.170000+00:00',
            'confidence': 0.95,
            'segment_id': 123,
            'words': [['Test', 0.0, 0.5], ['content', 0.5, 1.0], ['1', 1.0, 1.5]],
            'speaker': 'Speaker 1',
            'html_content': '<p>Test content 1</p>'
        },
        {
            'content': 'Test content 2',
            'start_timestamp': '2025-02-13T15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13T15:33:01.390000+00:00',
            'confidence': 0.98,
            'segment_id': 124,
            'meeting_id': 'meeting-123',
            'words': [['Test', 0.0, 0.5], ['content', 0.5, 1.0], ['2', 1.0, 1.5]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13T15:33:01.390000+00:00',
            'transcription_timestamp': '2025-02-13T15:33:01.390000+00:00',
            'present_user_ids': ['user1', 'user2'],
            'partially_present_user_ids': []
        }
    ]

@pytest.fixture
def mock_transcript_manager(mock_segments):
    class MockTranscriptManager:
        @classmethod
        async def create(cls):
            return cls()
            
        async def ingest_transcript_segments(self, content_id, segments, user_id, session):
            if content_id == UUID('00000000-0000-0000-0000-000000000000'):
                raise ValueError("Content not found")
            return segments
            
        async def get_transcript_segments(self, content_id, user_id, session):
            if content_id == UUID('00000000-0000-0000-0000-000000000000'):
                raise ValueError("Content not found")
            return mock_segments
            
        async def update_transcript_access(self, transcript_id, user_id, target_user_id, access_level, session):
            if transcript_id == UUID('00000000-0000-0000-0000-000000000000'):
                raise ValueError("Transcript not found")
            if transcript_id == UUID('11111111-1111-1111-1111-111111111111'):
                return False
            return True
            
    return MockTranscriptManager

@pytest.fixture
def mock_session():
    class MockSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
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
        return await mock_transcript_manager.create()
    return _get_transcript_manager

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
    
    # Test successful ingestion
    content_id = UUID('12345678-1234-5678-1234-567812345678')
    response = test_client.post(f"/api/transcripts/segments/{content_id}", json=mock_segments)
    assert response.status_code == 200
    assert len(response.json()) == 2
    
    # Test content not found
    content_id = UUID('00000000-0000-0000-0000-000000000000')
    response = test_client.post(f"/api/transcripts/segments/{content_id}", json=mock_segments)
    assert response.status_code == 404
    assert "Content not found" in response.json()["detail"]

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
    
    # Test successful retrieval
    content_id = UUID('12345678-1234-5678-1234-567812345678')
    response = test_client.get(f"/api/transcripts/segments/{content_id}")
    assert response.status_code == 200
    assert len(response.json()) == 2
    
    # Test content not found
    content_id = UUID('00000000-0000-0000-0000-000000000000')
    response = test_client.get(f"/api/transcripts/segments/{content_id}")
    assert response.status_code == 400
    assert "Content not found" in response.json()["detail"]

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
    
    # Test successful update
    transcript_id = UUID('12345678-1234-5678-1234-567812345678')
    access_update = {
        "target_user_id": "87654321-8765-4321-8765-432187654321",
        "access_level": AccessLevel.SHARED.value
    }
    response = test_client.put(f"/api/transcripts/{transcript_id}/access", json=access_update)
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    
    # Test transcript not found
    transcript_id = UUID('00000000-0000-0000-0000-000000000000')
    response = test_client.put(f"/api/transcripts/{transcript_id}/access", json=access_update)
    assert response.status_code == 404
    assert "Transcript not found" in response.json()["detail"]
    
    # Test access denied
    transcript_id = UUID('11111111-1111-1111-1111-111111111111')
    response = test_client.put(f"/api/transcripts/{transcript_id}/access", json=access_update)
    assert response.status_code == 403
    assert "Access denied" in response.json()["detail"] 