\c apidata;

-- Show table structure
\d+ posts;

-- Show existing partitions
SELECT 
    child.relname AS partition_name,
    pg_get_expr(child.relpartbound, child.oid) AS partition_expression
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'posts';

-- Show partition management log
SELECT * FROM partition_management_log ORDER BY timestamp DESC LIMIT 5;
