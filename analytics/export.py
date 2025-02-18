from sqlalchemy import select
from psql_helpers import async_session
from .models import AnalyticsSession, AnalyticsEvent, PageView, FeatureUsage
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from typing import Optional, List, Dict, Any

class AnalyticsExporter:
    def __init__(self):
        pass

    async def get_sessions_df(self, start_date: Optional[datetime] = None, 
                            end_date: Optional[datetime] = None) -> pd.DataFrame:
        async with async_session() as db:
            query = select(AnalyticsSession)
            if start_date:
                query = query.where(AnalyticsSession.start_time >= start_date)
            if end_date:
                query = query.where(AnalyticsSession.start_time <= end_date)
            
            result = await db.execute(query)
            sessions = result.scalars().all()
            
            return pd.DataFrame([{
                'session_id': s.session_id,
                'user_id': s.user_id,
                'start_time': s.start_time,
                'end_time': s.end_time,
                'initial_referrer': s.initial_referrer,
                'initial_path': s.initial_path,
                'utm_source': s.utm_source,
                'utm_medium': s.utm_medium,
                'utm_campaign': s.utm_campaign,
                'device_type': s.device_type,
                'browser': s.browser,
                'os': s.os,
                'is_mobile': s.is_mobile,
                'country': s.country,
                'city': s.city,
                'region': s.region
            } for s in sessions])

    async def get_events_df(self, start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None,
                          event_types: Optional[List[str]] = None) -> pd.DataFrame:
        async with async_session() as db:
            query = select(AnalyticsEvent)
            if start_date:
                query = query.where(AnalyticsEvent.timestamp >= start_date)
            if end_date:
                query = query.where(AnalyticsEvent.timestamp <= end_date)
            if event_types:
                query = query.where(AnalyticsEvent.event_type.in_(event_types))
            
            result = await db.execute(query)
            events = result.scalars().all()
            
            return pd.DataFrame([{
                'id': e.id,
                'session_id': e.session_id,
                'event_type': e.event_type,
                'event_name': e.event_name,
                'path': e.path,
                'timestamp': e.timestamp,
                'duration': e.duration,
                'metadata': e.event_metadata
            } for e in events])

    async def get_pageviews_df(self, start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> pd.DataFrame:
        async with async_session() as db:
            query = select(PageView)
            if start_date:
                query = query.where(PageView.timestamp >= start_date)
            if end_date:
                query = query.where(PageView.timestamp <= end_date)
            
            result = await db.execute(query)
            pageviews = result.scalars().all()
            
            return pd.DataFrame([{
                'id': p.id,
                'session_id': p.session_id,
                'path': p.path,
                'timestamp': p.timestamp,
                'duration': p.duration,
                'scroll_depth': p.scroll_depth,
                'is_bounce': p.is_bounce,
                'exit_page': p.exit_page
            } for p in pageviews])

    async def get_feature_usage_df(self, start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None,
                                 features: Optional[List[str]] = None) -> pd.DataFrame:
        async with async_session() as db:
            query = select(FeatureUsage)
            if start_date:
                query = query.where(FeatureUsage.timestamp >= start_date)
            if end_date:
                query = query.where(FeatureUsage.timestamp <= end_date)
            if features:
                query = query.where(FeatureUsage.feature.in_(features))
            
            result = await db.execute(query)
            features = result.scalars().all()
            
            return pd.DataFrame([{
                'id': f.id,
                'session_id': f.session_id,
                'feature': f.feature,
                'action': f.action,
                'timestamp': f.timestamp,
                'metadata': f.event_metadata
            } for f in features])

    async def export_all(self, start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None) -> Dict[str, pd.DataFrame]:
        return {
            'sessions': await self.get_sessions_df(start_date, end_date),
            'events': await self.get_events_df(start_date, end_date),
            'pageviews': await self.get_pageviews_df(start_date, end_date),
            'feature_usage': await self.get_feature_usage_df(start_date, end_date)
        } 