--! Creating a new schema for storing final tables for ML
CREATE SCHEMA IF NOT EXISTS public_ready_for_ML;
--! Preparing TABLES
--* Merging train_data with incident_data
DROP TABLE IF EXISTS public_ready_for_ML.train_data;
CREATE TABLE IF NOT EXISTS public_ready_for_ML.train_data AS
SELECT train_data_final.*,
    type_incident,
    id_incident,
    dateH_incident,
    lieu,
    statut,
    statut_commercial,
    statut_financier,
    gravite,
    motif_client,
    commentaire
FROM public_processed.train_data_final
    LEFT JOIN public_processed.incident_data ON public_processed.train_data_final.idtrain_lot = public_processed.incident_data.id_train_lot;
--! SELECTING all columns from Station_Name_%
WITH station_name_columns AS (
    SELECT CAST()
);
SELECT jsonagg(
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public_ready_for_ml'
            AND table_name = 'train_data'
            AND LOWER(column_name) LIKE 'station_name%'
    );