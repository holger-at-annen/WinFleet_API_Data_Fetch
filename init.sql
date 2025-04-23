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

-- Partition management function
CREATE OR REPLACE FUNCTION manage_partitions() 
RETURNS void AS $$
BEGIN
    -- Create current month partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS posts_%s_%s PARTITION OF posts 
         FOR VALUES FROM (%L) TO (%L)',
        to_char(CURRENT_DATE, 'YYYY'),
        to_char(CURRENT_DATE, 'MM'),
        date_trunc('month', CURRENT_DATE),
        date_trunc('month', CURRENT_DATE + interval '1 month')
    );
    
    -- Create next month partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS posts_%s_%s PARTITION OF posts 
         FOR VALUES FROM (%L) TO (%L)',
        to_char(CURRENT_DATE + interval '1 month', 'YYYY'),
        to_char(CURRENT_DATE + interval '1 month', 'MM'),
        date_trunc('month', CURRENT_DATE + interval '1 month'),
        date_trunc('month', CURRENT_DATE + interval '2 month')
    );
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions
SELECT manage_partitions();

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_posts_event_time ON posts (event_time);
CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts (asset_id);

-- Schedule partition management (runs daily)
SELECT cron.schedule('daily_partition_management', '0 0 * * *', 'SELECT manage_partitions()');
