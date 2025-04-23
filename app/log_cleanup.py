import os
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def cleanup_old_logs(log_dir='app/logs', days_to_keep=30):
    """Remove log files older than specified days"""
    try:
        if not os.path.exists(log_dir):
            logger.warning(f"Log directory {log_dir} does not exist")
            return

        current_time = datetime.now()
        count_removed = 0
        
        for filename in os.listdir(log_dir):
            if not filename.endswith('.log'):
                continue
                
            filepath = os.path.join(log_dir, filename)
            file_modified = datetime.fromtimestamp(os.path.getmtime(filepath))
            
            if current_time - file_modified > timedelta(days=days_to_keep):
                try:
                    os.remove(filepath)
                    count_removed += 1
                    logger.info(f"Removed old log file: {filename}")
                except OSError as e:
                    logger.error(f"Failed to remove log file {filename}: {e}")
        
        logger.info(f"Log cleanup completed. Removed {count_removed} old log files")
    except Exception as e:
        logger.error(f"Log cleanup failed: {e}")