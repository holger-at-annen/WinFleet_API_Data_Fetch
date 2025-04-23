-- Create partition management log table first
CREATE TABLE IF NOT EXISTS partition_management_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    action TEXT,
    partition_name TEXT
);

-- Create partition management function
CREATE OR REPLACE FUNCTION manage_partitions()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    current_partition text;
    next_partition text;
    partition_start date;
    partition_end date;
BEGIN
    -- Log the start of partition management
    INSERT INTO partition_management_log (action) 
    VALUES ('Starting partition management for database: ' || current_database());
    
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
    
    -- Create partitions if they don't exist
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF posts 
         FOR VALUES FROM (%L) TO (%L)',
        current_partition,
        partition_start,
        partition_end
    );
    
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF posts 
         FOR VALUES FROM (%L) TO (%L)',
        next_partition,
        partition_end,
        partition_end + interval '1 month'
    );
END;
$$;

-- Create parent table with partitioning
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL,
    asset_id INTEGER NOT NULL,
    event_time TIMESTAMP NOT NULL,
    name TEXT,
    plate_number TEXT,
    vin TEXT,
    position_description TEXT,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    status_text TEXT,
    PRIMARY KEY (asset_id, event_time)
) PARTITION BY RANGE (event_time);

-- Create global indexes
CREATE INDEX IF NOT EXISTS idx_posts_event_time ON posts (event_time);
CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts (asset_id);

-- Initialize partitions
SELECT manage_partitions();
