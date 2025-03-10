import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from models.db import Base, DATABASE_URL

from services.psql_helpers import _compile_drop_table


async def init_db(drop_tables=False):
    engine = create_async_engine(DATABASE_URL)
    
    async with engine.begin() as conn:
        # Drop all tables with CASCADE if flag is True
        if drop_tables:
            async def drop_all_tables():
                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.drop_all)
                    
            await drop_all_tables()
            print("All tables dropped.")
            
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    
    print("Database initialized successfully.")

if __name__ == "__main__":
    asyncio.run(init_db(drop_tables=False)) 