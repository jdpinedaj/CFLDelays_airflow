DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    Code_UIC VARCHAR NOT NULL,
    IDGARE VARCHAR NOT NULL,
    Code VARCHAR NOT NULL,
    Nom VARCHAR NOT NULL,
    NomComplet VARCHAR NOT NULL,
    Code_pays VARCHAR NOT NULL,
    IDGARE_Parent VARCHAR NOT NULL,
    Gare_Frontiere VARCHAR NOT NULL,
    Gare_Marchandises VARCHAR NOT NULL,
    Latitude VARCHAR NOT NULL,
    Longitude VARCHAR NOT NULL,
    Enr_actif VARCHAR NOT NULL,
    IDUtilisateur_creation VARCHAR NOT NULL,
    DateH_creation VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    Type_gare VARCHAR NOT NULL,
    Top_Everysens_existe VARCHAR NOT NULL,
    Geojson VARCHAR NOT NULL
);