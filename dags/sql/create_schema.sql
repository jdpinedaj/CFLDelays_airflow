DROP SCHEMA IF EXISTS {{ params.schema_name }} CASCADE;
CREATE SCHEMA IF NOT EXISTS {{ params.schema_name }};