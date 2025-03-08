from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import sys
import os
import logging
import logging.handlers

from routers.auth import router as auth_router
from routers.transcripts import router as transcripts
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Placeholder for any future initialization if needed
    yield

app = FastAPI(lifespan=lifespan)

# Move this BEFORE any other middleware or app setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://assistant.dev.vexa.ai", "http://localhost:5173", "http://localhost:5174","https://vexa.ai","http://host.docker.internal"],  # Add this],  # Must be explicit
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Other middleware and routes should come after CORS middleware

# Development flag
DEV = os.getenv('REDIS_HOST', '127.0.0.1') == '127.0.0.1'

# Add logging configuration after the imports and before app initialization
def setup_logger():
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger('vexa_api')
    logger.setLevel(logging.DEBUG)

    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create and setup file handler
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/api.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Create and setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Add both handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

# Add this with other router includes
app.include_router(auth_router)
app.include_router(transcripts)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8010, reload=True)
    
    # conda activate langchain && uvicorn app:app --host 0.0.0.0 --port 8765 --workers 1 --loop uvloop --reload

