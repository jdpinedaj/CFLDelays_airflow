DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(Code_UIC, 'NaN') AS FLOAT) AS code_uic,
    CAST(NULLIF(IDGARE, 'NaN') AS FLOAT) AS id_gare,
    CAST(NULLIF(Code, 'NaN') AS VARCHAR) AS code,
    CAST(NULLIF(Nom, 'NaN') AS VARCHAR) AS nom,
    CAST(NULLIF(NomComplet, 'NaN') AS VARCHAR) AS nom_complet,
    CAST(NULLIF(Code_pays, 'NaN') AS VARCHAR) AS code_pays,
    CAST(NULLIF(IDGARE_Parent, 'NaN') AS FLOAT) AS id_gare_parent,
    CAST(NULLIF(Gare_Frontiere, 'NaN') AS FLOAT) AS gare_frontiere,
    CAST(NULLIF(Gare_Marchandises, 'NaN') AS FLOAT) AS gare_marchandises,
    CAST(NULLIF(Latitude, 'NaN') AS DOUBLE PRECISION) AS latitude,
    CAST(NULLIF(Longitude, 'NaN') AS DOUBLE PRECISION) AS longitude,
    CAST(NULLIF(Enr_actif, 'NaN') AS FLOAT) AS enr_actif,
    CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation,
    CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(Type_gare, 'NaN') AS VARCHAR) AS type_gare,
    CAST(NULLIF(Top_Everysens_existe, 'NaN') AS FLOAT) AS top_everysens_existe,
    CAST(NULLIF(Geojson, 'NaN') AS VARCHAR) AS geojson
FROM (
        SELECT DISTINCT ON (IDGARE) *
        FROM {{ params.origin_table }}
        ORDER BY IDGARE
    ) station;