from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy import create_engine, text
import uuid
from datetime import datetime
import hashlib
from uuid import UUID
from typing import List, Optional, Union
from datetime import timezone
from sqlalchemy import or_
from sqlalchemy import func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select
from contextlib import asynccontextmanager
from sqlalchemy import and_
from enum import Enum
from sqlalchemy import Index, UniqueConstraint, CheckConstraint
from sqlalchemy import Table, Column, Integer, String, Text, Float, Boolean, ForeignKey
from sqlalchemy import distinct

from app.models.psql_models import (
    Base, engine, async_session, DATABASE_URL,
)

#docker run --name dima_entities -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

import secrets

from datetime import timedelta

import os

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')

DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def execute_query(query):
    async with engine.connect() as conn:
        await conn.execute(text(query))

from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

async def setup_database():
    from app.models.psql_models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        
        # Set timezone to UTC
        await conn.execute(text("SET TIME ZONE 'UTC'"))

@asynccontextmanager
async def get_session():
    async with async_session() as session:
        try:
            # Set timezone to UTC for this session
            await session.execute(text("SET TIME ZONE 'UTC'"))
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession

async def read_table_async(table_or_query):
    async with AsyncSession(engine) as session:
        # Set timezone to UTC
        await session.execute(text("SET TIME ZONE 'UTC'"))
        
        # Convert table class to select query if needed
        if hasattr(table_or_query, '__table__'):
            query = select(table_or_query)
        else:
            query = table_or_query
            
        result = await session.execute(query)
        rows = result.fetchall()
        
        # If it's a simple table query
        if hasattr(table_or_query, '__table__'):
            data = []
            for row in rows:
                obj = row[0]  # Assuming the object is the first item in each row
                data.append({column.name: getattr(obj, column.name) for column in table_or_query.__table__.columns})
        # If it's a complex query with multiple entities
        else:
            data = []
            for row in rows:
                row_data = {}
                for entity, obj in zip(query.selected_columns, row):
                    if hasattr(obj, '__table__'):
                        # If it's an ORM object
                        for column in obj.__table__.columns:
                            row_data[f"{obj.__table__.name}_{column.name}"] = getattr(obj, column.name)
                    else:
                        # If it's a single column
                        row_data[entity.name] = obj
                data.append(row_data)
        
        return pd.DataFrame(data)
    
    

async def get_schema_info():
    async with async_session() as session:
        # Set timezone to UTC
        await session.execute(text("SET TIME ZONE 'UTC'"))
        
        # Query to get table information
        tables_query = """
        SELECT 
            t.table_name,
            array_agg(
                c.column_name || ' ' || 
                c.data_type || 
                CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END ||
                CASE WHEN c.column_default IS NOT NULL THEN ' DEFAULT ' || c.column_default ELSE '' END
            ) as columns,
            array_agg(
                CASE 
                    WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PK: ' || kcu.column_name
                    WHEN tc.constraint_type = 'FOREIGN KEY' THEN 
                        'FK: ' || kcu.column_name || ' -> ' || ccu.table_name || '.' || ccu.column_name
                    WHEN tc.constraint_type = 'UNIQUE' THEN 'UQ: ' || kcu.column_name
                    WHEN tc.constraint_type = 'CHECK' THEN 'CHECK: ' || tc.constraint_name
                END
            ) FILTER (WHERE tc.constraint_type IS NOT NULL) as constraints
        FROM 
            information_schema.tables t
        LEFT JOIN 
            information_schema.columns c ON t.table_name = c.table_name
        LEFT JOIN 
            information_schema.table_constraints tc ON t.table_name = tc.table_name
        LEFT JOIN 
            information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN 
            information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE 
            t.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
        GROUP BY 
            t.table_name
        ORDER BY 
            t.table_name;
        """
        
        result = await session.execute(text(tables_query))
        schema_info = result.fetchall()
        
        return schema_info

