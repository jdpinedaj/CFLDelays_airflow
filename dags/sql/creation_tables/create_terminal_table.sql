DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDTERMINAL VARCHAR NOT NULL,
    Nom_terminal VARCHAR NOT NULL,
    IDGARE_Desserte VARCHAR NOT NULL,
    Enr_actif VARCHAR NOT NULL,
    IDUtilisateur_creation VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    DateH_creation VARCHAR NOT NULL,
    IDSOCIETE VARCHAR NOT NULL,
    Nom_court VARCHAR NOT NULL,
    IDGARE_Reseau VARCHAR NOT NULL,
    Maritime VARCHAR NOT NULL
);