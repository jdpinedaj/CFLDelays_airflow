DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(IDINCIDENT, 'NaN') AS FLOAT) AS id_incident,
    CAST(NULLIF(Type_incident, 'NaN') AS VARCHAR) AS type_incident,
    CAST(NULLIF(IDINCIDENT_Pere, 'NaN') AS FLOAT) AS id_incident_pere,
    CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation,
    CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(DateH_incident, 'NaN') AS TIMESTAMP) AS dateH_incident,
    CAST(NULLIF(Lieu, 'NaN') AS VARCHAR) AS lieu,
    CAST(NULLIF(Num_incident, 'NaN') AS FLOAT) AS num_incident,
    CAST(NULLIF(Commentaire, 'NaN') AS VARCHAR) AS commentaire,
    CAST(NULLIF(Statut, 'NaN') AS FLOAT) AS statut,
    CAST(NULLIF(Gravite, 'NaN') AS FLOAT) AS gravite,
    CAST(NULLIF(Motif_client, 'NaN') AS VARCHAR) AS motif_client,
    CAST(NULLIF(IDGARE, 'NaN') AS FLOAT) AS id_gare,
    CAST(NULLIF(Statut_commercial, 'NaN') AS FLOAT) AS statut_commercial,
    CAST(NULLIF(Statut_financier, 'NaN') AS FLOAT) AS statut_financier
FROM {{ params.origin_table }};