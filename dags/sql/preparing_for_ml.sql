--! Creating a new schema for storing final tables for ML
CREATE SCHEMA IF NOT EXISTS public_ready_for_ML;
--! Preparing TABLES
--* Merging train_data with incident_data
DROP TABLE IF EXISTS public_ready_for_ML.train_data;
CREATE TABLE IF NOT EXISTS public_ready_for_ML.train_data AS
SELECT public_processed.train_data_final.*,
    public_processed.incident_data.id_incident,
    public_processed.incident_data.type_incident,
    public_processed.incident_data.dateH_incident,
    public_processed.incident_data.lieu,
    public_processed.incident_data.statut,
    public_processed.incident_data.statut_commercial,
    public_processed.incident_data.statut_financier,
    public_processed.incident_data.gravite,
    public_processed.incident_data.motif_client,
    public_processed.incident_data.commentaire
FROM public_processed.train_data_final
    LEFT JOIN public_processed.incident_data ON public_processed.train_data_final.idtrain_lot = public_processed.incident_data.id_train_lot;