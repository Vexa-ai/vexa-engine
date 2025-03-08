import pytest
import pytest_asyncio
from services.auth import AuthManager
from unittest.mock import patch, MagicMock
import asyncio

@pytest_asyncio.fixture
async def auth_manager():
    return await AuthManager.create()

@pytest.mark.asyncio
async def test_google_auth_success(auth_manager):
    # Mock successful response
    expected_result = {
        "user_id": "test_user_id",
        "token": "test_token",
        "email": "test@example.com"
    }
    
    async def mock_google_auth(*args, **kwargs):
        return expected_result
    
    with patch.object(auth_manager.vexa_auth, 'google_auth', 
                     side_effect=mock_google_auth) as mock_auth:
        result = await auth_manager.google_auth(
            token="test_token",
            utm_params={"utm_source": "test"}
        )
        
        assert result == expected_result
        mock_auth.assert_called_once_with(
            token="test_token",
            utm_params={"utm_source": "test"}
        )

@pytest.mark.asyncio
async def test_google_auth_token_timing_retry(auth_manager):
    # Mock responses for retry behavior
    with patch.object(auth_manager.vexa_auth, 'google_auth') as mock_auth:
        # First call raises error, second succeeds
        mock_auth.side_effect = [
            Exception("Token used too early"),
            {"user_id": "test_user_id"}
        ]
        
        result = await auth_manager.google_auth(
            token="test_token",
            max_retries=2,
            base_delay=0  # Speed up test by not actually sleeping
        )
        
        assert result == {"user_id": "test_user_id"}
        assert mock_auth.call_count == 2

@pytest.mark.asyncio
async def test_google_auth_failure(auth_manager):
    # Mock authentication failure
    with patch.object(auth_manager.vexa_auth, 'google_auth',
                     side_effect=Exception("Auth failed")):
        
        with pytest.raises(ValueError, match="Auth failed"):
            await auth_manager.google_auth(token="invalid_token")

@pytest.mark.asyncio
async def test_google_auth_timing_error_max_retries(auth_manager):
    # Mock consistent timing errors
    with patch.object(auth_manager.vexa_auth, 'google_auth',
                     side_effect=Exception("Token used too early")):
        
        with pytest.raises(ValueError, match="Authentication timing error"):
            await auth_manager.google_auth(
                token="test_token",
                max_retries=2,
                base_delay=0
            ) 