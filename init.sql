-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create parent table for partitioning
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

-- Function to manage partitions
CREATE OR REPLACE FUNCTION manage_posts_partitions()
RETURNS void AS $$
DECLARE
    current_date_val DATE;
    next_month_date DATE;
    partition_name TEXT;
BEGIN
    -- Create partition for current month if it doesn't exist
    current_date_val := date_trunc('month', CURRENT_DATE);
    partition_name := 'posts_' || to_char(current_date_val, 'YYYY_MM');
    
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF posts 
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            current_date_val,
            current_date_val + interval '1 month'
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    END IF;

    -- Create partition for next month if it doesn't exist
    next_month_date := current_date_val + interval '1 month';
    partition_name := 'posts_' || to_char(next_month_date, 'YYYY_MM');
    
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF posts 
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            next_month_date,
            next_month_date + interval '1 month'
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions
SELECT manage_posts_partitions();

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_posts_event_time ON posts (event_time);
CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts (asset_id);

-- Schedule partition management (runs daily at midnight)
SELECT cron.schedule('manage_posts_partitions', '0 0 * * *', 'SELECT manage_posts_partitions()');
