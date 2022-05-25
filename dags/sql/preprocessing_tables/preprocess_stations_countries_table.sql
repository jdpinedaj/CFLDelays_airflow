DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(STATION, 'NaN') AS VARCHAR) AS station,
    CAST(NULLIF(COUNTRY, 'NaN') AS VARCHAR) AS country,
    CAST(NULLIF(LAT, 'NaN') AS FLOAT) AS lat,
    CAST(NULLIF(LONG, 'NaN') AS FLOAT) AS long
FROM {{ params.origin_table }};