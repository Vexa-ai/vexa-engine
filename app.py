from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from app.api.v1.content import router as content_router
from app.api.v1.auth import router as auth_router

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers - auth must come first
app.include_router(auth_router)
app.include_router(content_router)

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting application...")
    uvicorn.run(app, host="0.0.0.0", port=8010)

