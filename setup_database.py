from psql_helpers import init_db
import asyncio

if __name__ == "__main__":
    asyncio.run(init_db()) 