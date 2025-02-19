from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from psql_models import User
from uuid import UUID

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)) -> User:
    # This is a simplified version for testing
    # In production, this would validate the token and fetch the actual user
    if not credentials or not credentials.credentials:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    
    # Return a mock user for testing
    return User(
        id=UUID('12345678-1234-5678-1234-567812345678'),
        email='test@test.com'
    ) 