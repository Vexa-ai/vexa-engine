import pytest
from httpx import AsyncClient
from unittest.mock import patch, AsyncMock
from datetime import datetime, timedelta
import os
from uuid import UUID
from auth_manager import AuthManager

# Test data
VALID_EMAIL = "test@example.com"
INVALID_EMAIL = "not_an_email"
STREAMQUEUE_URL = os.getenv('STREAMQUEUE_URL')

@pytest.fixture
def auth_manager():
    return AuthManager()

@pytest.mark.asyncio
@patch('auth_manager.AuthManager._propagate_token_to_stream')  # Patch the internal method instead
async def test_default_auth_valid_email(mock_propagate, client: AsyncClient, test_session, auth_manager):
    """Test successful registration with valid email and token propagation"""
    # Setup mock response
    mock_propagate.return_value = None  # Method returns None
    
    response = await client.post("/auth/default", json={
        "email": VALID_EMAIL,
        "utm_source": "test",
        "utm_medium": "email"
    })
    
    assert response.status_code == 200
    data = response.json()
    assert "user_id" in data
    assert "token" in data
    
    # Verify propagation was called with correct headers
    mock_propagate.assert_called_once()
    call_args = mock_propagate.call_args[0]  # Get positional args
    assert call_args[0] == data["token"]  # First arg should be token
    assert str(call_args[1]) == data["user_id"]  # Second arg should be user_id

    # Verify the headers were set correctly in the actual implementation
    with patch('auth_manager.AsyncClient.post') as mock_http_post:
        mock_http_post.return_value.status_code = 200
        await auth_manager._propagate_token_to_stream(data["token"], UUID(data["user_id"]))
        mock_http_post.assert_called_once()
        _, kwargs = mock_http_post.call_args
        assert kwargs["headers"]["Authorization"] == f"Bearer {os.getenv('STREAMQUEUE_API_KEY')}"

@pytest.mark.asyncio
async def test_default_auth_invalid_email(client: AsyncClient):
    """Test rejection of invalid email format"""
    response = await client.post("/auth/default", json={
        "email": INVALID_EMAIL
    })
    assert response.status_code == 422

@pytest.mark.asyncio
@patch('auth_manager.AuthManager._propagate_token_to_stream')
async def test_token_validation(mock_propagate, client: AsyncClient, test_session):
    """Test token submission and validation with stream queue propagation"""
    # Setup mock
    mock_propagate.return_value = None
    
    # First create a user with token
    auth_response = await client.post("/auth/default", json={"email": VALID_EMAIL})
    assert auth_response.status_code == 200
    data = auth_response.json()
    token = data["token"]
    expected_user_id_str = data["user_id"]  # This is a string in your API response
    
    # Reset mock to verify next call
    mock_propagate.reset_mock()
    
    # Test token validation
    response = await client.post("/auth/submit_token", json={"token": token})
    assert response.status_code == 200
    response_data = response.json()
    assert "user_id" in response_data
    
    # Instead of assert_called_once_with(token, expected_user_id_str),
    # retrieve the call args and then compare:
    call_args = mock_propagate.call_args[0]
    assert call_args[0] == token
    # Convert the actual user_id arg to string for comparison
    assert str(call_args[1]) == expected_user_id_str

@pytest.mark.asyncio
@patch('auth_manager.AuthManager._propagate_token_to_stream')
async def test_token_validation_with_failed_propagation(mock_propagate, client: AsyncClient, test_session):
    """Test token validation succeeds even if stream queue propagation fails"""
    # First create a user with token (with successful propagation)
    mock_propagate.return_value = None
    auth_response = await client.post("/auth/default", json={"email": VALID_EMAIL})
    assert auth_response.status_code == 200
    data = auth_response.json()
    token = data["token"]
    
    # Now setup mock to simulate failure for validation
    mock_propagate.side_effect = Exception("Stream queue error")
    
    # Test token validation still succeeds
    response = await client.post("/auth/submit_token", json={"token": token})
    assert response.status_code == 200  # Should succeed despite propagation failure
    response_data = response.json()
    assert "user_id" in response_data

@pytest.mark.asyncio
async def test_invalid_token_validation(client: AsyncClient, test_session):
    """Test validation of invalid token"""
    response = await client.post("/auth/submit_token", json={"token": "invalid_token"})
    assert response.status_code == 401 