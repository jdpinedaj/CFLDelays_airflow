DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDWAGON_POSITION VARCHAR NOT NULL,
    IDWAGON VARCHAR NOT NULL,
    DateH_position VARCHAR NOT NULL,
    Latitude VARCHAR NOT NULL,
    Longitude VARCHAR NOT NULL,
    Application_GPS VARCHAR NOT NULL,
    Qualifiant VARCHAR NOT NULL,
    Etat_position VARCHAR NOT NULL
);