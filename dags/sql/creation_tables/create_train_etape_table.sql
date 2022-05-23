DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDTRAIN_ETAPE VARCHAR NOT NULL,
    IDTRAIN_LOT VARCHAR NOT NULL,
    Type_etape VARCHAR NOT NULL,
    DH_Theorique VARCHAR NOT NULL,
    H_Thorique_ecart VARCHAR NOT NULL,
    DH_Reelle VARCHAR NOT NULL,
    DH_Theorique_fin VARCHAR NOT NULL,
    DH_Reelle_fin VARCHAR NOT NULL,
    Commentaire VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    H_Theorique_ecart1 VARCHAR NOT NULL,
    H_Theorique_ecart2 VARCHAR NOT NULL
);