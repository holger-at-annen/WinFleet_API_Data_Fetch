-- Create partition management log table
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
-- ... existing partition management function code ...
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
