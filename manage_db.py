import asyncio
import os
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from back.psql_models import init_db

async def reset_db():
    """Reset the database (drop all tables and recreate them)"""
    await init_db()
    print("Database reset complete")

def run_migrations():
    """Run all pending migrations"""
    os.system("alembic upgrade head")
    print("Migrations complete")

def create_migration(message):
    """Create a new migration"""
    os.system(f'alembic revision --autogenerate -m "{message}"')
    print("Migration created")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please specify a command: reset_db, migrate, or create_migration")
        sys.exit(1)

    command = sys.argv[1]
    if command == "reset_db":
        asyncio.run(reset_db())
    elif command == "migrate":
        run_migrations()
    elif command == "create_migration":
        if len(sys.argv) < 3:
            print("Please provide a migration message")
            sys.exit(1)
        create_migration(sys.argv[2])
    else:
        print("Unknown command. Available commands: reset_db, migrate, create_migration")
        
        
#python scripts/manage_db.py create_migration "Add new field"
