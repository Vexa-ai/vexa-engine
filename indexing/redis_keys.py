class RedisKeys:
    # Queue management
    INDEXING_QUEUE = "indexing_queue"
    PROCESSING_SET = "processing_set"
    FAILED_SET = "failed_set"
    
    # User tracking
    SEEN_USERS = "seen_users_set"
    
    # Error handling
    RETRY_ERRORS = "retry_errors"
    
    # Monitoring
    LAST_CHECK = "last_active_check" 