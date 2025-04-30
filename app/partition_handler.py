import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import psycopg2
from psycopg2.extensions import AsIs
import logging
from main import db_pool  # Import db_pool from main.py

def create_partition(conn, start_date):
    """Create a monthly partition starting from the given date."""
    cur = conn.cursor()
    try:
        # Ensure start_date is the 1st of the month
        partition_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        partition_end = partition_start + relativedelta(months=1)
        
        partition_name = f"posts_{partition_start.strftime('%Y_%m')}"
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS %s PARTITION OF posts
            FOR VALUES FROM (%s) TO (%s)
        """, (AsIs(partition_name), partition_start, partition_end))
        
        conn.commit()
        logging.info(f"Created partition {partition_name} for {partition_start} to {partition_end}")
        return True
        
    except Exception as e:
        logging.error(f"Error creating partition {partition_name}: {str(e)}")
        conn.rollback()
        return False
    finally:
        cur.close()

def handle_missing_partition_error(conn, error_message):
    """Handle missing partition errors by creating the required partition."""
    try:
        # Extract date from timestamptz error message (e.g., '2025-04-28 05:59:04' or '2025-04-28 05:59:04+00')
        date_match = re.search(r'\((\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}', error_message)
        if not date_match:
            logging.error(f"Could not extract date from error message: {error_message}")
            return False

        date_str = date_match.group(1)
        event_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Create partition for the extracted date
        return create_partition(conn, event_date)

    except Exception as e:
        logging.error(f"Error handling missing partition: {str(e)}")
        conn.rollback()
        return False

def create_future_partitions():
    """Create partitions for the current and next two months to prevent missing partition errors."""
    conn = db_pool.getconn()
    try:
        current_date = datetime.now()
        for i in range(3):  # Current month + next 2 months
            create_partition(conn, current_date + relativedelta(months=i))
        logging.info("Future partitions created successfully")
        return True
    except Exception as e:
        logging.error(f"Error creating future partitions: {str(e)}")
        return False
    finally:
        db_pool.putconn(conn)
