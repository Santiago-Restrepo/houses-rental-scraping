"""
Improved logging configuration for the ETL pipeline.
"""

import logging
import os
from datetime import datetime
from config.settings import LOG_LEVEL, LOG_FORMAT, DATA_DIR


def setup_logging(log_level: str = LOG_LEVEL) -> logging.Logger:
    """Setup logging configuration for the ETL pipeline."""
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(DATA_DIR, 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'etl_pipeline_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    # Create logger for this module
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module."""
    return logging.getLogger(name)

