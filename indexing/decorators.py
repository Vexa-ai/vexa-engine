from functools import wraps
import logging
from typing import Optional, Type, Callable, Any
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

def handle_processing_errors(error_type: Optional[Type[Exception]] = None):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if error_type and not isinstance(e, error_type):
                    raise
                logger.error(f"{func.__name__} failed: {str(e)}", exc_info=True)
                if 'session' in kwargs:
                    await kwargs['session'].rollback()
                raise
        return wrapper
    return decorator

def with_logging(level: str = 'info'):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            log_func = getattr(logger, level.lower())
            log_func(f"Starting {func.__name__}")
            try:
                result = await func(*args, **kwargs)
                log_func(f"Completed {func.__name__}")
                return result
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                raise
        return wrapper
    return decorator

def with_redis_ops(operation: str):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, content_id: str, *args, **kwargs):
            logger.debug(f"{operation} for content {content_id}")
            try:
                result = await func(self, content_id, *args, **kwargs)
                logger.debug(f"Completed {operation} for content {content_id}")
                return result
            except Exception as e:
                logger.error(f"Redis {operation} failed for content {content_id}: {str(e)}")
                raise
        return wrapper
    return decorator 