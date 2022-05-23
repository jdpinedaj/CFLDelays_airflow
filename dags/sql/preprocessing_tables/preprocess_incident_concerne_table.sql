DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
SELECT CAST(NULLIF(IDINCIDENT_CONCERNE, 'NaN') AS FLOAT) AS id_incident_concerne,
    CAST(NULLIF(IDINCIDENT, 'NaN') AS FLOAT) AS id_incident,
    CAST(NULLIF(IDTRAIN_UTI, 'NaN') AS FLOAT) AS id_train_uti,
    CAST(NULLIF(IDTRAIN_WAGON, 'NaN') AS FLOAT) AS id_train_wagon,
    CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation,
    CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(Unite, 'NaN') AS VARCHAR) AS unite,
    CAST(NULLIF(IDCOMMANDE_UTI, 'NaN') AS FLOAT) AS id_commande_uti,
    CAST(NULLIF(IDAGRES, 'NaN') AS FLOAT) AS id_agres,
    CAST(NULLIF(IDFACTURE, 'NaN') AS FLOAT) AS id_facture,
    CAST(NULLIF(IDTRAIN_LOT, 'NaN') AS FLOAT) AS id_train_lot,
    CAST(NULLIF(IDWAGON, 'NaN') AS FLOAT) AS id_wagon,
    CAST(NULLIF(Annule_TrainLot, 'NaN') AS FLOAT) AS annule_trainlot,
    CAST(NULLIF(IDSOCIETE, 'NaN') AS FLOAT) AS id_societe,
    CAST(NULLIF(IDFACTURE_FOUR, 'NaN') AS FLOAT) AS id_facture_four
FROM {{ params.origin_table }};