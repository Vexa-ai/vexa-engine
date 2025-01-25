from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
from enum import Enum
from app.models.base import BaseCall
from app.models.schema.errors import VexaAPIError

class ContentType(str, Enum):
    MEETING = 'meeting'
    DOCUMENT = 'document'
    TITLE = 'title'
    SUMMARY = 'summary'
    ACTION_POINTS = 'action_points'
    NOTE = 'note'

class AccessLevel(str, Enum):
    OWNER = 'owner'
    EDITOR = 'editor'
    VIEWER = 'viewer'
    REMOVED = 'removed'

class EntityType(str, Enum):
    SPEAKER = 'speaker'
    TAG = 'tag'

class EntityRef(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    type: EntityType

class ContentFilter(BaseModel):
    content_type: Optional[ContentType] = None
    entity_type: Optional[EntityType] = None
    entity_names: Optional[List[str]] = Field(default=None, max_length=20)
    parent_id: Optional[UUID] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    
    @model_validator(mode='after')
    def validate_dates(self) -> 'ContentFilter':
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError('date_from must be before date_to')
        return self
    
    @field_validator('entity_names')
    @classmethod
    def validate_entity_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is not None:
            if not v:
                raise ValueError('entity_names cannot be empty list')
            if len(v) > 20:
                raise ValueError('maximum 20 entity names allowed')
            if not all(isinstance(name, str) and name.strip() for name in v):
                raise ValueError('all entity names must be non-empty strings')
        return v

class ContentListRequest(BaseModel):
    filter: Optional[ContentFilter] = None
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)

class ContentCreate(BaseModel):
    type: ContentType
    text: str = Field(..., min_length=1)
    parent_id: Optional[UUID] = None
    entities: Optional[List[EntityRef]] = Field(default=None, max_length=50)
    
    @field_validator('type')
    @classmethod
    def validate_content_type(cls, v: ContentType) -> ContentType:
        if v == ContentType.MEETING:
            raise ValueError('cannot create content of type MEETING')
        return v
    
    @field_validator('entities')
    @classmethod
    def validate_entities(cls, v: Optional[List[EntityRef]]) -> Optional[List[EntityRef]]:
        if v is not None:
            if not v:
                raise ValueError('entities cannot be empty list')
            if len(v) > 50:
                raise ValueError('maximum 50 entities allowed')
            seen = set()
            for entity in v:
                key = (entity.name, entity.type)
                if key in seen:
                    raise ValueError(f'duplicate entity: {entity.name} of type {entity.type}')
                seen.add(key)
        return v

class ContentUpdate(BaseModel):
    text: Optional[str] = Field(default=None, min_length=1)
    entities: Optional[List[EntityRef]] = Field(default=None, max_length=50)
    
    @model_validator(mode='after')
    def validate_not_empty(self) -> 'ContentUpdate':
        if not self.text and not self.entities:
            raise ValueError('at least one of text or entities must be provided')
        return self
    
    @field_validator('entities')
    @classmethod
    def validate_entities(cls, v: Optional[List[EntityRef]]) -> Optional[List[EntityRef]]:
        if v is not None:
            if not v:
                raise ValueError('entities cannot be empty list')
            if len(v) > 50:
                raise ValueError('maximum 50 entities allowed')
            seen = set()
            for entity in v:
                key = (entity.name, entity.type)
                if key in seen:
                    raise ValueError(f'duplicate entity: {entity.name} of type {entity.type}')
                seen.add(key)
        return v

class ContentMetadata(BaseModel):
    speakers: Optional[List[str]] = None
    start_datetime: Optional[datetime] = None
    custom_data: Optional[Dict[str, Any]] = None

class ContentResponse(BaseModel):
    content_id: UUID
    type: ContentType
    text: str
    timestamp: datetime
    last_update: datetime
    parent_id: Optional[UUID] = None
    entities: List[EntityRef] = Field(default_factory=list)
    is_indexed: bool = False
    access_level: AccessLevel
    is_owner: bool
    metadata: ContentMetadata = Field(default_factory=ContentMetadata)

class ContentIndexStatus(str, Enum):
    PENDING = "pending"
    INDEXED = "indexed"
    UPDATED = "updated"
    FAILED = "failed"

class ContentArchiveRequest(BaseModel):
    archive_children: bool = Field(default=False, description='Whether to archive child content items')
