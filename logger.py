import logging
import sys

# Configure logger
logger = logging.getLogger("vexa_api")
logger.setLevel(logging.DEBUG)

# Create console formatter
console_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

# Add handler to logger
logger.addHandler(console_handler)

# Prevent logs from propagating to root logger
logger.propagate = False 