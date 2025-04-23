-- Create required databases and extensions
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Set pg_cron database after extension is created
ALTER SYSTEM SET cron.database_name TO '${POSTGRES_DB}';

-- Create partition management function
CREATE OR REPLACE FUNCTION manage_partitions() 
RETURNS void AS $$
DECLARE
    current_partition text;
    next_partition text;
    partition_start date;
    partition_end date;
BEGIN
    -- Calculate partition dates
    partition_start := date_trunc('month', CURRENT_DATE);
    partition_end := partition_start + interval '1 month';
    
    -- Generate partition names
    current_partition := format('posts_%s_%s',
        to_char(partition_start, 'YYYY'),
        to_char(partition_start, 'MM'));
    next_partition := format('posts_%s_%s',
        to_char(partition_end, 'YYYY'),
        to_char(partition_end, 'MM'));
        
    -- Create current month partition if not exists
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
        
        -- Create indexes on the new partition
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
    END IF;
    
    -- Create next month partition if not exists
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
        
        -- Create indexes on the new partition
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
    END IF;
    
    -- Log partition management
    INSERT INTO partition_management_log (action, partition_name)
    VALUES ('Created/Verified partitions', current_partition || ', ' || next_partition);
END;
$$ LANGUAGE plpgsql;

-- Create partition management log table
CREATE TABLE IF NOT EXISTS partition_management_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    action TEXT,
    partition_name TEXT
);

-- Create parent table with partitioning
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

-- Create global indexes on parent table
CREATE INDEX IF NOT EXISTS idx_posts_event_time ON posts (event_time);
CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts (asset_id);

-- Initialize partitions
SELECT manage_partitions();

-- Schedule automatic partition management
SELECT cron.schedule(
    'manage_partitions_job',
    '0 0 * * *',  -- Run daily at midnight
    $$SELECT manage_partitions()$$
);
