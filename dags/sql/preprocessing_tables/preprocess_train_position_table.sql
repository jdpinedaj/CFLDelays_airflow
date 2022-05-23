DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(idtrain_position, 'NaN') AS FLOAT) AS id_train_position,
    CAST(NULLIF(idtrain, 'NaN') AS FLOAT) AS id_train,
    CAST(NULLIF(dateh_position, 'NaN') AS TIMESTAMP) AS dateH_position,
    CAST(NULLIF(latitude, 'NaN') AS FLOAT) AS latitude,
    CAST(NULLIF(longitude, 'NaN') AS FLOAT) AS longitude
FROM (
        SELECT DISTINCT ON (IDTRAIN_POSITION) *
        FROM {{ params.origin_table }}
        ORDER BY IDTRAIN_POSITION
    ) train_position
ORDER BY id_train_position,
    dateH_position;