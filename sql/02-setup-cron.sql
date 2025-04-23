\c postgres

-- Schedule partition management
SELECT cron.schedule(
    'manage_partitions_job',
    '0 0 * * *',
    format(
        'SELECT manage_partitions() FROM dblink(''dbname=%I'', ''SELECT manage_partitions()'') AS t(result void)',
        current_database()
    )
);
