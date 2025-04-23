import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logging():
    """Configure application logging with rotation and proper formatting"""
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Configure the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create rotating file handler with date-based filename
    current_date = datetime.now().strftime('%Y-%m')
    log_file = os.path.join(log_dir, f'winfleet_{current_date}.log')
    
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=10*1024*1024,  # 10MB per file
        backupCount=10,          # Keep 5 backup files
        encoding='utf-8'
    )

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Set formatter for both handlers
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger