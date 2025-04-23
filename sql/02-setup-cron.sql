-- Connect to postgres database
\c postgres

-- Remove existing job if it exists
SELECT cron.unschedule('manage_partitions_job');

-- Schedule partition management
SELECT cron.schedule(
    'manage_partitions_job',
    '0 0 * * *',  -- Run daily at midnight
    format(
        'SELECT manage_partitions() FROM dblink(''dbname=%I'', ''SELECT manage_partitions()'') AS t(result void)',
        current_database()
    )
);

-- Verify job was created
SELECT jobid, jobname, schedule, command, nodename, nodeport, database, username 
FROM cron.job 
WHERE jobname = 'manage_partitions_job';
