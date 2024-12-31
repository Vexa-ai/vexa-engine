import pytest
from uuid import uuid4
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from api.threads.chat import router, thread_manager, token_manager
from core import Msg

# Test data
TEST_USER_ID = str(uuid4())
TEST_USER_NAME = "test_user"
TEST_TOKEN = "test_token"
TEST_THREAD_ID = str(uuid4())

@pytest.fixture
def client():
    return TestClient(router)

@pytest.fixture
def mock_token_check():
    with patch.object(token_manager, 'check_token', new_callable=AsyncMock) as mock:
        mock.return_value = (TEST_USER_ID, TEST_USER_NAME)
        yield mock

@pytest.fixture
def mock_thread_manager():
    with patch.object(thread_manager, 'get_thread', new_callable=AsyncMock) as mock_get:
        with patch.object(thread_manager, 'edit_message', new_callable=AsyncMock) as mock_edit:
            yield {
                'get_thread': mock_get,
                'edit_message': mock_edit
            }

@pytest.mark.asyncio
async def test_edit_message_success(client, mock_token_check, mock_thread_manager):
    """Test successful message edit"""
    # Setup test data
    test_messages = [
        Msg(role="user", content="original message"),
        Msg(role="assistant", content="original response")
    ]
    
    # Mock thread manager responses
    mock_thread_manager['get_thread'].return_value = AsyncMock(
        user_id=TEST_USER_ID,
        messages=test_messages
    )
    mock_thread_manager['edit_message'].return_value = [
        Msg(role="user", content="edited message")
    ]
    
    # Make request
    response = client.put(
        f"/chat/messages/{TEST_THREAD_ID}",
        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
        json={
            "message_index": 0,
            "new_content": "edited message"
        }
    )
    
    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert data["thread_id"] == TEST_THREAD_ID
    assert len(data["messages"]) == 1
    assert data["messages"][0]["content"] == "edited message"

@pytest.mark.asyncio
async def test_edit_message_not_found(client, mock_token_check, mock_thread_manager):
    """Test editing message in non-existent thread"""
    # Mock thread not found
    mock_thread_manager['get_thread'].return_value = None
    
    response = client.put(
        f"/chat/messages/{TEST_THREAD_ID}",
        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
        json={
            "message_index": 0,
            "new_content": "edited message"
        }
    )
    
    assert response.status_code == 404
    assert response.json()["detail"] == "Thread not found"

@pytest.mark.asyncio
async def test_edit_message_unauthorized(client, mock_token_check, mock_thread_manager):
    """Test editing message in another user's thread"""
    # Mock thread with different owner
    mock_thread_manager['get_thread'].return_value = AsyncMock(
        user_id=str(uuid4()),  # Different user ID
        messages=[]
    )
    
    response = client.put(
        f"/chat/messages/{TEST_THREAD_ID}",
        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
        json={
            "message_index": 0,
            "new_content": "edited message"
        }
    )
    
    assert response.status_code == 403
    assert response.json()["detail"] == "Not thread owner"

@pytest.mark.asyncio
async def test_edit_message_invalid_token(client, mock_token_check):
    """Test editing message with invalid token"""
    # Mock invalid token
    mock_token_check.return_value = (None, None)
    
    response = client.put(
        f"/chat/messages/{TEST_THREAD_ID}",
        headers={"Authorization": f"Bearer invalid_token"},
        json={
            "message_index": 0,
            "new_content": "edited message"
        }
    )
    
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid token"

@pytest.mark.asyncio
async def test_edit_message_failed(client, mock_token_check, mock_thread_manager):
    """Test when message edit operation fails"""
    # Mock thread exists but edit fails
    mock_thread_manager['get_thread'].return_value = AsyncMock(
        user_id=TEST_USER_ID,
        messages=[]
    )
    mock_thread_manager['edit_message'].return_value = None
    
    response = client.put(
        f"/chat/messages/{TEST_THREAD_ID}",
        headers={"Authorization": f"Bearer {TEST_TOKEN}"},
        json={
            "message_index": 0,
            "new_content": "edited message"
        }
    )
    
    assert response.status_code == 400
    assert response.json()["detail"] == "Failed to edit message" 