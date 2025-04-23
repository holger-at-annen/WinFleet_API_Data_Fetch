#!/bin/bash

# Ensure POSTGRES_USER and POSTGRES_DB are set
if [ -z "$POSTGRES_USER" ]; then
  echo "Error: POSTGRES_USER environment variable is not set"
  exit 1
fi

if [ -z "$POSTGRES_DB" ]; then
  echo "Error: POSTGRES_DB environment variable is not set"
  exit 1
fi

# Generate init-db.sql with POSTGRES_USER substituted
cat << EOF > /tmp/init-db.sql
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

-- Create initial partitions based on current date
DO \$\$
DECLARE
    current_year INTEGER := EXTRACT(YEAR FROM CURRENT_DATE);
    current_month INTEGER := EXTRACT(MONTH FROM CURRENT_DATE);
BEGIN
    -- Create single partition for current month
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS posts_%s_%s PARTITION OF posts 
         FOR VALUES FROM (%L) TO (%L)',
        current_year,
        to_char(CURRENT_DATE, 'MM'),
        date_trunc('month', CURRENT_DATE),
        date_trunc('month', CURRENT_DATE + interval '1 month')
    );
END \$\$;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_posts_event_time ON posts (event_time);
CREATE INDEX IF NOT EXISTS idx_posts_asset_id ON posts (asset_id);
EOF

# Execute the SQL using the existing connection pool
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/init-db.sql