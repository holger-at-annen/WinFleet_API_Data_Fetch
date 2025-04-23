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

-- Function to create partitions
CREATE OR REPLACE FUNCTION create_partition_if_not_exists()
RETURNS void AS $$
DECLARE
    current_year INTEGER;
    current_month INTEGER;
    next_month_year INTEGER;
    next_month INTEGER;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    -- Current month partition
    current_year := EXTRACT(YEAR FROM CURRENT_DATE);
    current_month := EXTRACT(MONTH FROM CURRENT_DATE);
    partition_name := format('posts_%s_%s', current_year, LPAD(current_month::text, 2, '0'));
    start_date := date_trunc('month', CURRENT_DATE);
    end_date := date_trunc('month', CURRENT_DATE + interval '1 month');
    
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF posts FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        RAISE NOTICE 'Created partition %', partition_name;
    END IF;

    -- Next month partition
    start_date := date_trunc('month', CURRENT_DATE + interval '1 month');
    end_date := date_trunc('month', CURRENT_DATE + interval '2 month');
    next_month_year := EXTRACT(YEAR FROM start_date);
    next_month := EXTRACT(MONTH FROM start_date);
    partition_name := format('posts_%s_%s', next_month_year, LPAD(next_month::text, 2, '0'));
    
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF posts FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        RAISE NOTICE 'Created partition %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions
SELECT create_partition_if_not_exists();

-- Create indexes (these will be inherited by partitions)
CREATE INDEX IF NOT EXISTS idx_posts_event_time ON posts (event_time);
CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts (asset_id);

-- Create a scheduled job to check for new partitions (runs daily)
CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule('0 0 * * *', $$SELECT create_partition_if_not_exists();$$);
