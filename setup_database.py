from psql_helpers import setup_database
import asyncio

if __name__ == "__main__":
    asyncio.run(setup_database()) 