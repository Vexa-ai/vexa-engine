from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy import create_engine, text
import uuid
from datetime import datetime
import hashlib
from uuid import UUID
from sqlalchemy import Table
from typing import List

engine = create_engine('postgresql://postgres:mysecretpassword@localhost:5432/postgres')
Session = sessionmaker(bind=engine)

#docker run --name dima_entities -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

Base = declarative_base()

# Association table for the many-to-many relationship
item_object_association = Table('item_object', Base.metadata,
    Column('item_id', Integer, ForeignKey('items.id')),
    Column('object_id', Integer, ForeignKey('objects.id'))
)

class Speaker(Base):
    __tablename__ = 'speakers'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    meetings = relationship('Meeting', back_populates='speaker')
    items = relationship('Item', back_populates='speaker')

    def __repr__(self):
        return f"<Speaker(id={self.id}, name='{self.name}')>"

class Meeting(Base):
    __tablename__ = 'meetings'

    id = Column(Integer, primary_key=True)
    meeting_id = Column(PostgresUUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    transcript = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    speaker_id = Column(Integer, ForeignKey('speakers.id'))

    speaker = relationship('Speaker', back_populates='meetings')
    items = relationship('Item', back_populates='meeting')

    def __repr__(self):
        return f"<Meeting(id={self.id}, meeting_id='{self.meeting_id}')>"

class Item(Base):
    __tablename__ = 'items'

    id = Column(Integer, primary_key=True)
    summary_index = Column(Integer)
    summary = Column(Text)
    details = Column(Text)
    referenced_text = Column(Text)
    meeting_id = Column(PostgresUUID(as_uuid=True), ForeignKey('meetings.meeting_id'))
    speaker_id = Column(Integer, ForeignKey('speakers.id'))

    meeting = relationship('Meeting', back_populates='items')
    speaker = relationship('Speaker', back_populates='items')
    objects = relationship('Object', secondary=item_object_association, back_populates='items')

    def __repr__(self):
        return f"<Item(id={self.id}, type='{self.type}')>"

class Object(Base):
    __tablename__ = 'objects'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)
    type = Column(String(50))  # Moved 'type' from Item to Object

    items = relationship('Item', secondary=item_object_association, back_populates='objects')

    def __repr__(self):
        return f"<Object(id={self.id}, name='{self.name}')>"

class Output(Base):
    __tablename__ = 'outputs'

    id = Column(Integer, primary_key=True)
    input_text = Column(Text, nullable=False)
    output_text = Column(Text)
    model = Column(String(100), nullable=False)
    temperature = Column(Float, nullable=False)
    max_tokens = Column(Integer, nullable=False)
    cache_key = Column(String(64), unique=True, nullable=False)

    def __repr__(self):
        return f"<Output(id={self.id}, model='{self.model}', cache_key='{self.cache_key}')>"

    @staticmethod
    def generate_cache_key(input_text: str, model: str, temperature: float, max_tokens: int) -> str:
        key = f"{input_text}_{model}_{temperature}_{max_tokens}"
        return hashlib.md5(key.encode()).hexdigest()

from datetime import timedelta, datetime
from sqlalchemy import and_, or_
from uuid import UUID

def fetch_context(session, speaker_names: List[str], reference_date: datetime):
    existing_speakers = session.query(Speaker).filter(Speaker.name.in_(speaker_names)).all()
    if not existing_speakers:
        return []

    existing_speaker_names = [speaker.name for speaker in existing_speakers]

    items_query = session.query(Item).options(
        joinedload(Item.objects),
        joinedload(Item.meeting),
        joinedload(Item.speaker)
    ).join(Meeting).join(Speaker).filter(
        Speaker.name.in_(existing_speaker_names),
        Meeting.timestamp <= reference_date
    ).order_by(Meeting.timestamp.desc())

    items = items_query.all()

    context = []
    for item in items:
        context.append({
            'objects': [{'name': obj.name, 'type': obj.type} for obj in item.objects],
            'summary': item.summary,
            'details': item.details,
            'speaker': item.speaker.name,
            'timestamp': item.meeting.timestamp,
            'meeting_id': item.meeting.meeting_id
        })

    return context

def execute_query(query):
    with engine.connect() as connection:
        connection.execute(text(query))

def init_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
