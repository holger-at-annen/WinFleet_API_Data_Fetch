import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2
from psycopg2.extensions import AsIs
import logging

def create_partition_for_date(conn, date):
    """Create a monthly partition for the given date"""
    cur = conn.cursor()
    try:
        # Calculate partition start (1st of the month) and end (1st of next month)
        partition_start = date.replace(day=1)
        partition_end = partition_start + relativedelta(months=1)
        
        partition_name = f"posts_{partition_start.strftime('%Y_%m')}"
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS %s PARTITION OF posts
            FOR VALUES FROM (%s) TO (%s)
        """, (AsIs(partition_name), partition_start, partition_end))
        
        conn.commit()
        logging.info(f"Created new partition {partition_name}")
        return True
        
    except Exception as e:
        logging.error(f"Error creating partition: {str(e)}")
        return False
    finally:
        cur.close()

def handle_missing_partition_error(conn, error_message):
    """Extract date from error message and create missing partition"""
    # Extract date from error message using regex
    match = re.search(r'event_time\) = \((\d{4}-\d{2}-\d{2})', str(error_message))
    if match:
        date_str = match.group(1)
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            return create_partition_for_date(conn, date)
        except ValueError:
            logging.error(f"Could not parse date from error message: {date_str}")
            return False
    return False
