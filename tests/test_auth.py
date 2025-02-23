import pytest
from httpx import AsyncClient
from unittest.mock import patch, AsyncMock
from datetime import datetime, timedelta

# Test data
VALID_EMAIL = "test@example.com"
INVALID_EMAIL = "not_an_email"

@pytest.mark.asyncio
async def test_default_auth_valid_email(client: AsyncClient, test_session):
    """Test successful registration with valid email"""
    response = await client.post("/auth/default", json={
        "email": VALID_EMAIL,
        "utm_source": "test",
        "utm_medium": "email"
    })
    assert response.status_code == 200
    assert "user_id" in response.json()
    assert "token" in response.json()

@pytest.mark.asyncio
async def test_default_auth_invalid_email(client: AsyncClient):
    """Test rejection of invalid email format"""
    response = await client.post("/auth/default", json={
        "email": INVALID_EMAIL
    })
    assert response.status_code == 422  # FastAPI returns 422 for validation errors

@pytest.mark.asyncio
async def test_token_validation(client: AsyncClient, test_session):
    """Test token submission and validation"""
    # First create a user with token
    auth_response = await client.post("/auth/default", json={"email": VALID_EMAIL})
    assert auth_response.status_code == 200
    token = auth_response.json()["token"]
    
    # Test token validation
    response = await client.post("/auth/submit_token", json={"token": token})
    assert response.status_code == 200
    assert "user_id" in response.json() 