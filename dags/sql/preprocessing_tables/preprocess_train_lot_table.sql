DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(IDTRAIN_LOT, 'NaN') AS FLOAT) AS id_train_lot,
    CAST(NULLIF(IDTRAIN, 'NaN') AS FLOAT) AS id_train,
    CAST(NULLIF(Lot_num, 'NaN') AS FLOAT) AS lot_num,
    CAST(NULLIF(Nom_lot, 'NaN') AS VARCHAR) AS nom_lot,
    CAST(NULLIF(Commentaire, 'NaN') AS VARCHAR) AS commentaire,
    CAST(NULLIF(Distance, 'NaN') AS FLOAT) AS distance,
    CAST(NULLIF(Poids_CO2, 'NaN') AS FLOAT) AS poids_co2,
    CAST(NULLIF(IDSOCIETE_Expediteur, 'NaN') AS FLOAT) AS id_societe_expediteur,
    CAST(NULLIF(IDSOCIETE_Destinataire, 'NaN') AS FLOAT) AS id_societe_destinataire,
    CAST(NULLIF(IDJALON_Origine, 'NaN') AS FLOAT) AS id_jalon_origine,
    CAST(NULLIF(IDJALON_Destination, 'NaN') AS FLOAT) AS id_jalon_destination,
    CAST(NULLIF(Objectif_CA, 'NaN') AS FLOAT) AS objectif_ca,
    CAST(NULLIF(Objectif_TEU, 'NaN') AS FLOAT) AS objectif_teu,
    CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation,
    CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(Ref_Expediteur, 'NaN') AS VARCHAR) AS ref_expediteur,
    CAST(NULLIF(Ref_Destinataire, 'NaN') AS VARCHAR) AS ref_destinataire,
    CAST(NULLIF(Train_Position, 'NaN') AS FLOAT) AS train_position,
    CAST(NULLIF(Incoterm, 'NaN') AS VARCHAR) AS incoterm,
    CAST(NULLIF(Num_LDV, 'NaN') AS VARCHAR) AS num_ldv,
    CAST(NULLIF(Objectif_T, 'NaN') AS FLOAT) AS objectif_t,
    CAST(NULLIF(Objectif_UTI, 'NaN') AS FLOAT) AS objectif_uti,
    CAST(NULLIF(Nbre_TEU, 'NaN') AS FLOAT) AS TEU_Count,
    CAST(NULLIF(Capacite_TEU, 'NaN') AS FLOAT) AS Max_TEU,
    CAST(NULLIF(Nbre_UTI, 'NaN') AS FLOAT) AS nbre_uti,
    CAST(NULLIF(Poids_total, 'NaN') AS FLOAT) AS poids_total,
    CAST(NULLIF(Status, 'NaN') AS FLOAT) AS status,
    CAST(NULLIF(Longueur_totale, 'NaN') AS FLOAT) AS longueur_totale,
    CAST(NULLIF(Motif_suppression, 'NaN') AS VARCHAR) AS motif_suppression,
    CAST(NULLIF(DateH_suppression, 'NaN') AS TIMESTAMP) AS dateH_suppression,
    CAST(
        NULLIF(DateH_info_fournisseur, 'NaN') AS TIMESTAMP
    ) AS dateH_info_fournisseur,
    CAST(NULLIF(Responsable_annulation, 'NaN') AS VARCHAR) AS responsable_annulation
FROM (
        SELECT DISTINCT ON (IDTRAIN_LOT) *
        FROM {{ params.origin_table }}
        ORDER BY IDTRAIN_LOT
    ) train_lot;