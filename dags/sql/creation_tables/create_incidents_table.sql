DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDINCIDENT VARCHAR NOT NULL,
    Type_incident VARCHAR NOT NULL,
    IDINCIDENT_Pere VARCHAR NOT NULL,
    IDUtilisateur_creation VARCHAR NOT NULL,
    DateH_creation VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    DateH_incident VARCHAR NOT NULL,
    Lieu VARCHAR NOT NULL,
    Num_incident VARCHAR NOT NULL,
    Commentaire VARCHAR NOT NULL,
    Statut VARCHAR NOT NULL,
    Gravite VARCHAR NOT NULL,
    Motif_client VARCHAR NOT NULL,
    IDGARE VARCHAR NOT NULL,
    Statut_commercial VARCHAR NOT NULL,
    Statut_financier VARCHAR NOT NULL
);