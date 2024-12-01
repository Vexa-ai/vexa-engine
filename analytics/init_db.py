import sys
sys.path.append('/app')

from sqlalchemy.schema import CreateSchema
from psql_models import engine, Base
import psql_models  # Import to include main models in metadata
import models  # Import analytics models
from sqlalchemy import text

async def init_analytics_db():
    # Create schema using raw SQL first
    async with engine.connect() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
        await conn.commit()
        
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(init_analytics_db())
        print("Analytics database initialized successfully")
    except Exception as e:
        print(f"Error initializing analytics database: {str(e)}")