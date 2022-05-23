DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(IDTRAIN, 'NaN') AS FLOAT) AS id_train,
    CAST(NULLIF(Train_num, 'NaN') AS FLOAT) AS train_num,
    CAST(NULLIF(IDRELATION, 'NaN') AS FLOAT) AS id_relation,
    CAST(NULLIF(Sens, 'NaN') AS VARCHAR) AS sens,
    CAST(NULLIF(IDCONTACT_Responsable, 'NaN') AS FLOAT) AS id_contact_responsable,
    CAST(NULLIF(Motif_suppression, 'NaN') AS VARCHAR) AS motif_suppression,
    CAST(NULLIF(IDTRAINREF_JOUR, 'NaN') AS FLOAT) AS id_train_ref_jour,
    CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation,
    CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(IDATE, 'NaN') AS FLOAT) AS idate,
    CAST(NULLIF(Commentaire, 'NaN') AS VARCHAR) AS commentaire,
    CAST(NULLIF(Poids_maxi, 'NaN') AS FLOAT) AS poids_maxi,
    CAST(NULLIF(Longueur_maxi, 'NaN') AS FLOAT) AS longueur_maxi,
    CAST(NULLIF(DateH_suppression, 'NaN') AS TIMESTAMP) AS dateH_suppression,
    CAST(NULLIF(IDSOCIETE_suppression, 'NaN') AS FLOAT) AS id_societe_suppression,
    CAST(NULLIF(IDTRAIN_Parent, 'NaN') AS FLOAT) AS id_train_parent
FROM (
        SELECT DISTINCT ON (IDTRAIN) *
        FROM {{ params.origin_table }}
        ORDER BY IDTRAIN
    ) train;