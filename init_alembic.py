import os
from sqlalchemy import create_engine, text

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)
with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS alembic_version'))
    conn.execute(text('CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)'))
    conn.execute(text("INSERT INTO alembic_version (version_num) VALUES ('e45a91a84d1d')"))
    conn.commit() 