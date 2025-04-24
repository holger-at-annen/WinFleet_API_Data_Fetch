import os
import psycopg2
import logging
from time import sleep

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_db(dsn, max_attempts=60, wait_seconds=2):
    """Wait for database to become available"""
    for attempt in range(max_attempts):
        try:
            conn = psycopg2.connect(dsn)
            cur = conn.cursor()
            cur.execute("SELECT 1")  # Test if we can actually execute queries
            cur.close()
            conn.close()
            return True
        except (psycopg2.OperationalError, psycopg2.Error):
            logger.info(f"Waiting for database... attempt {attempt + 1}/{max_attempts}")
            if attempt < max_attempts - 1:
                sleep(wait_seconds)
            continue
    return False

def init_database():
    """Initialize the database with required extensions and schema"""
    # Get database configuration from environment
    db_config = {
        'host': '/var/run/postgresql',  # Use Unix socket
        'user': os.getenv('POSTGRES_USER', 'dbuser'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password'),
        'database': 'postgres'  # Connect to default database first
    }

    app_db = os.getenv('POSTGRES_DB', 'apidata')
    
    # First wait for PostgreSQL to be ready
    dsn = f"dbname=postgres user={db_config['user']} password={db_config['password']} host={db_config['host']}"
    if not wait_for_db(dsn):
        raise Exception("Database is not available after maximum retries")

    logger.info("PostgreSQL is available, initializing database...")
    
    try:
        # Connect to default database first
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        cur = conn.cursor()

        # Create application database if it doesn't exist
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{app_db}'")
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{app_db}"')
            logger.info(f"Created database {app_db}")

        # Close default database connection
        cur.close()
        conn.close()

        # Connect to application database
        db_config['database'] = app_db
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        cur = conn.cursor()

        # Setup extensions
        logger.info("Setting up extensions...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron CASCADE;")
        cur.execute("CREATE EXTENSION IF NOT EXISTS dblink;")
        
        # Setup cron schema permissions
        logger.info("Setting up cron schema permissions...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS cron;")
        cur.execute("GRANT USAGE ON SCHEMA cron TO current_user;")
        cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cron TO current_user;")
        
        # Create partition management log table
        logger.info("Creating partition management log table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS partition_management_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                action TEXT,
                partition_name TEXT
            );
        """)

        # Create parent table with partitioning
        logger.info("Creating parent table with partitioning...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id SERIAL,
                asset_id INTEGER NOT NULL,
                name TEXT,
                plate_number TEXT,
                vin TEXT,
                position_description TEXT,
                event_time TIMESTAMP NOT NULL,
                latitude DECIMAL(10,8),
                longitude DECIMAL(11,8),
                status_text TEXT,
                PRIMARY KEY (asset_id, event_time)
            ) PARTITION BY RANGE (event_time);
        """)

        # Create partition management function
        logger.info("Creating partition management function...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION manage_partitions() 
            RETURNS void AS $$
            DECLARE
                current_partition text;
                next_partition text;
                partition_start date;
                partition_end date;
            BEGIN
                partition_start := date_trunc('month', CURRENT_DATE);
                partition_end := partition_start + interval '1 month';
                
                current_partition := format('posts_%s_%s',
                    to_char(partition_start, 'YYYY'),
                    to_char(partition_start, 'MM'));
                next_partition := format('posts_%s_%s',
                    to_char(partition_end, 'YYYY'),
                    to_char(partition_end, 'MM'));
                    
                IF NOT EXISTS (
                    SELECT FROM pg_class c 
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = current_partition
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS %I PARTITION OF posts 
                         FOR VALUES FROM (%L) TO (%L)',
                        current_partition,
                        partition_start,
                        partition_end
                    );
                    
                    EXECUTE format(
                        'CREATE INDEX IF NOT EXISTS %I ON %I (event_time)',
                        'idx_' || current_partition || '_event_time',
                        current_partition
                    );
                    
                    EXECUTE format(
                        'CREATE INDEX IF NOT EXISTS %I ON %I (asset_id)',
                        'idx_' || current_partition || '_asset_id',
                        current_partition
                    );
                    
                    INSERT INTO partition_management_log (action, partition_name)
                    VALUES ('Created partition', current_partition);
                END IF;
                
                IF NOT EXISTS (
                    SELECT FROM pg_class c 
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = next_partition
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS %I PARTITION OF posts 
                         FOR VALUES FROM (%L) TO (%L)',
                        next_partition,
                        partition_end,
                        partition_end + interval '1 month'
                    );
                    
                    EXECUTE format(
                        'CREATE INDEX IF NOT EXISTS %I ON %I (event_time)',
                        'idx_' || next_partition || '_event_time',
                        next_partition
                    );
                    
                    EXECUTE format(
                        'CREATE INDEX IF NOT EXISTS %I ON %I (asset_id)',
                        'idx_' || next_partition || '_asset_id',
                        next_partition
                    );
                    
                    INSERT INTO partition_management_log (action, partition_name)
                    VALUES ('Created partition', next_partition);
                END IF;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Create backup management function
        logger.info("Creating backup management function...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION manage_backups() 
            RETURNS void AS $$
            DECLARE
                current_ts timestamp;
                backup_name text;
                daily_backup_name text;
                daily_retention interval := interval '7 days';
                weekly_retention interval := interval '1 month';
                monthly_retention interval := interval '1 year';
            BEGIN
                current_ts := CURRENT_TIMESTAMP;
                daily_backup_name := 'backup_daily_' || to_char(current_ts, 'YYYY_MM_DD');
                
                -- Create daily backup first as it will be potentially reused
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I AS SELECT * FROM posts WHERE event_time >= %L',
                    daily_backup_name,
                    current_ts - interval '1 day'
                );
                
                -- On Sundays, reuse daily backup for weekly
                IF extract(DOW from current_ts) = 0 THEN
                    backup_name := 'backup_weekly_' || to_char(current_ts, 'YYYY_WW');
                    EXECUTE format(
                        'CREATE TABLE %I AS TABLE %I',
                        backup_name,
                        daily_backup_name
                    );
                END IF;
                
                -- On first day of month, reuse daily backup for monthly
                IF extract(DAY from current_ts) = 1 THEN
                    backup_name := 'backup_monthly_' || to_char(current_ts, 'YYYY_MM');
                    EXECUTE format(
                        'CREATE TABLE %I AS TABLE %I',
                        backup_name,
                        daily_backup_name
                    );
                END IF;
                
                -- On first day of year, reuse daily backup for annual
                IF extract(DOY from current_ts) = 1 THEN
                    backup_name := 'backup_annual_' || to_char(current_ts, 'YYYY');
                    EXECUTE format(
                        'CREATE TABLE %I AS TABLE %I',
                        backup_name,
                        daily_backup_name
                    );
                END IF;

                -- Cleanup old backups
                -- Daily backups older than 7 days
                FOR backup_name IN 
                    SELECT tablename FROM pg_tables 
                    WHERE tablename LIKE 'backup_daily_%'
                    AND to_timestamp(split_part(tablename, '_', 3), 'YYYY_MM_DD') < current_ts - daily_retention
                LOOP
                    EXECUTE format('DROP TABLE IF EXISTS %I', backup_name);
                    INSERT INTO partition_management_log (action, partition_name)
                    VALUES ('Dropped old daily backup', backup_name);
                END LOOP;

                -- Weekly backups older than 1 month
                FOR backup_name IN 
                    SELECT tablename FROM pg_tables 
                    WHERE tablename LIKE 'backup_weekly_%'
                    AND to_timestamp(split_part(tablename, '_', 3), 'YYYY_WW') < current_ts - weekly_retention
                LOOP
                    EXECUTE format('DROP TABLE IF EXISTS %I', backup_name);
                    INSERT INTO partition_management_log (action, partition_name)
                    VALUES ('Dropped old weekly backup', backup_name);
                END LOOP;

                -- Monthly backups older than 1 year
                FOR backup_name IN 
                    SELECT tablename FROM pg_tables 
                    WHERE tablename LIKE 'backup_monthly_%'
                    AND to_timestamp(split_part(tablename, '_', 3), 'YYYY_MM') < current_ts - monthly_retention
                LOOP
                    EXECUTE format('DROP TABLE IF EXISTS %I', backup_name);
                    INSERT INTO partition_management_log (action, partition_name)
                    VALUES ('Dropped old monthly backup', backup_name);
                END LOOP;

                -- Log backup creation
                INSERT INTO partition_management_log (action, partition_name)
                VALUES ('Backup management completed', 'ALL');
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Schedule jobs
        logger.info("Scheduling jobs...")
        
        # Partition management - daily at midnight
        cur.execute("""
            SELECT cron.schedule(
                'manage_partitions_job',
                '0 0 * * *',
                $$SELECT manage_partitions()$$
            );
        """)

        # Backup management - daily at 1 AM
        cur.execute("""
            SELECT cron.schedule(
                'backup_management_job',
                '0 1 * * *',
                $$SELECT manage_backups()$$
            );
        """)

        # Create initial partitions
        logger.info("Creating initial partitions...")
        cur.execute("SELECT manage_partitions();")

        logger.info("Schema and partitioning setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    init_database()
