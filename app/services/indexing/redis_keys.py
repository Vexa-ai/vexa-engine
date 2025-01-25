from enum import Enum

class RedisKeys(str, Enum):
    INDEXING_QUEUE = "indexing:queue"
    INDEXING_STATUS = "indexing:status:{content_id}"
    INDEXING_LOCK = "indexing:lock:{content_id}"
    INDEXING_ERROR = "indexing:error:{content_id}"
    INDEXING_RESULT = "indexing:result:{content_id}"
    
    def format(self, **kwargs) -> str:
        return self.value.format(**kwargs) 