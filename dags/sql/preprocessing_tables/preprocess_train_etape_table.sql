DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(IDTRAIN_LOT, 'NaN') AS FLOAT) AS id_train_lot,
    CAST(NULLIF(IDTRAIN_ETAPE, 'NaN') AS FLOAT) AS id_train_etape,
    CAST(NULLIF(Type_etape, 'NaN') AS VARCHAR) AS type_etape,
    CAST(NULLIF(DH_Theorique, 'NaN') AS TIMESTAMP) AS dh_theorique,
    CAST(NULLIF(H_Thorique_ecart, 'NaN') AS FLOAT) AS h_thorique_ecart,
    CAST(NULLIF(DH_Reelle, 'NaN') AS TIMESTAMP) AS dh_reelle,
    CAST(NULLIF(DH_Theorique_fin, 'NaN') AS TIMESTAMP) AS dh_theorique_fin,
    CAST(NULLIF(DH_Reelle_fin, 'NaN') AS TIMESTAMP) AS dh_reelle_fin,
    CAST(NULLIF(Commentaire, 'NaN') AS VARCHAR) AS commentaire,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(H_Theorique_ecart1, 'NaN') AS FLOAT) AS h_theorique_ecart1,
    CAST(NULLIF(H_Theorique_ecart2, 'NaN') AS FLOAT) AS h_theorique_ecart2
FROM (
        SELECT DISTINCT ON (IDTRAIN_LOT) *
        FROM {{ params.origin_table }}
        ORDER BY IDTRAIN_LOT
    ) train_etape;