SELECT *
FROM information_schema.tables
WHERE table_schema = 'public';
SELECT schema_name,
    relname,
    pg_size_pretty(table_size) AS size,
    table_size
FROM (
        SELECT pg_catalog.pg_namespace.nspname AS schema_name,
            relname,
            pg_relation_size(pg_catalog.pg_class.oid) AS table_size
        FROM pg_catalog.pg_class
            JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
    ) t
WHERE schema_name = 'public'
    AND relname NOT LIKE '%_pkey'
    AND relname NOT LIKE '%_seq'
ORDER BY table_size DESC;
