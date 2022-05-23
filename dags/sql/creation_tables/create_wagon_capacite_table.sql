DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDWAGON_CAPACITE VARCHAR NOT NULL,
    IDUtilisateur_creation VARCHAR NOT NULL,
    DateH_creation VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    IDWAGON_MODELE VARCHAR NOT NULL,
    Code_pays VARCHAR NOT NULL,
    Capacite_C VARCHAR NOT NULL,
    Capacite_N VARCHAR NOT NULL,
    Capacite_P VARCHAR NOT NULL
);