from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float, Table, JSON, Boolean
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy import Index, UniqueConstraint, CheckConstraint, Enum as SQLEnum
from datetime import datetime
import uuid
from enum import Enum
from psql_models import Base

class EventType(str, Enum):
    PAGE_VIEW = 'page_view'
    FEATURE_USE = 'feature_use'
    BUTTON_CLICK = 'button_click'
    MEETING_INTERACTION = 'meeting_interaction'
    SEARCH = 'search'
    ERROR = 'error'
    PERFORMANCE = 'performance'

class AnalyticsSession(Base):
    __tablename__ = 'analytics_sessions'
    __table_args__ = (
        Index('idx_analytics_sessions_user_id', 'user_id'),
        Index('idx_analytics_sessions_start_time', 'start_time'),
        {'schema': 'analytics'}
    )

    session_id = Column(String(36), primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True))
    initial_referrer = Column(String(512))
    initial_path = Column(String(512))
    utm_source = Column(String(255))
    utm_medium = Column(String(255))
    utm_campaign = Column(String(255))
    utm_content = Column(String(255))
    utm_term = Column(String(255))
    country = Column(String(2))
    city = Column(String(100))
    region = Column(String(100))
    timezone = Column(String(50))
    device_type = Column(String(50))
    browser = Column(String(50))
    os = Column(String(50))
    is_mobile = Column(Boolean)

class AnalyticsEvent(Base):
    __tablename__ = 'analytics_events'
    __table_args__ = (
        Index('idx_analytics_events_session_id', 'session_id'),
        Index('idx_analytics_events_timestamp', 'timestamp'),
        Index('idx_analytics_events_type', 'event_type'),
        {'schema': 'analytics'}
    )

    id = Column(Integer, primary_key=True)
    session_id = Column(String(36), ForeignKey('analytics.analytics_sessions.session_id'))
    event_type = Column(SQLEnum(EventType))
    event_name = Column(String(100))
    path = Column(String(512))
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    duration = Column(Integer)  # milliseconds
    event_metadata = Column(JSON)

class PageView(Base):
    __tablename__ = 'page_views'
    __table_args__ = (
        Index('idx_page_views_session_id', 'session_id'),
        Index('idx_page_views_timestamp', 'timestamp'),
        Index('idx_page_views_path', 'path'),
        {'schema': 'analytics'}
    )

    id = Column(Integer, primary_key=True)
    session_id = Column(String(36), ForeignKey('analytics.analytics_sessions.session_id'))
    path = Column(String(512))
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    duration = Column(Integer)  # seconds
    scroll_depth = Column(Integer)  # percentage
    is_bounce = Column(Boolean, default=True)
    exit_page = Column(Boolean, default=True)

class FeatureUsage(Base):
    __tablename__ = 'feature_usage'
    __table_args__ = (
        Index('idx_feature_usage_session_id', 'session_id'),
        Index('idx_feature_usage_timestamp', 'timestamp'),
        Index('idx_feature_usage_feature', 'feature'),
        {'schema': 'analytics'}
    )

    id = Column(Integer, primary_key=True)
    session_id = Column(String(36), ForeignKey('analytics.analytics_sessions.session_id'))
    feature = Column(String(100))
    action = Column(String(50))
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    event_metadata = Column(JSON)