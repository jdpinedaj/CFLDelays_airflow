--! Creating a new schema for storing processed tables
CREATE SCHEMA IF NOT EXISTS public_processed;
--! Removing duplicates from keys and storing new tables in the new schema
-- train
DROP TABLE IF EXISTS public_processed.train;
CREATE TABLE IF NOT EXISTS public_processed.train AS
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
        FROM public.train
        ORDER BY IDTRAIN
    ) train;
-- train_etape
DROP TABLE IF EXISTS public_processed.train_etape;
CREATE TABLE IF NOT EXISTS public_processed.train_etape AS
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
        FROM public.train_etape
        ORDER BY IDTRAIN_LOT
    ) train_etape;
-- train_lot
DROP TABLE IF EXISTS public_processed.train_lot;
CREATE TABLE IF NOT EXISTS public_processed.train_lot AS
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
    CAST(NULLIF(Nbre_TEU, 'NaN') AS FLOAT) AS nbre_teu,
    CAST(NULLIF(Capacite_TEU, 'NaN') AS FLOAT) AS capacite_teu,
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
        FROM public.train_lot
        ORDER BY IDTRAIN_LOT
    ) train_lot;
-- train_position
DROP TABLE IF EXISTS public_processed.train_position;
CREATE TABLE IF NOT EXISTS public_processed.train_position AS
SELECT CAST(NULLIF(idtrain_position, 'NaN') AS FLOAT) AS id_train_position,
    CAST(NULLIF(idtrain, 'NaN') AS FLOAT) AS id_train,
    CAST(NULLIF(dateh_position, 'NaN') AS TIMESTAMP) AS dateH_position,
    CAST(NULLIF(latitude, 'NaN') AS FLOAT) AS latitude,
    CAST(NULLIF(longitude, 'NaN') AS FLOAT) AS longitude
FROM (
        SELECT DISTINCT ON (IDTRAIN_POSITION) *
        FROM public.train_position
        ORDER BY IDTRAIN_POSITION
    ) train_position
ORDER BY id_train_position,
    dateH_position;
-- station
DROP TABLE IF EXISTS public_processed.station;
CREATE TABLE IF NOT EXISTS public_processed.station AS
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
        FROM public.station
        ORDER BY IDGARE
    ) station;
-- train_jalon
DROP TABLE IF EXISTS public_processed.train_jalon;
CREATE TABLE IF NOT EXISTS public_processed.train_jalon AS
SELECT CAST(NULLIF(IDTRAIN_JALON, 'NaN') AS FLOAT) AS id_train_jalon,
    CAST(NULLIF(Jalon_num, 'NaN') AS FLOAT) AS jalon_num,
    CAST(NULLIF(DHR_Arrivee, 'NaN') AS TIMESTAMP) AS dhr_arrivee,
    CAST(NULLIF(DHT_Arrivee, 'NaN') AS TIMESTAMP) AS dht_arrivee,
    CAST(NULLIF(DHT_Depart, 'NaN') AS TIMESTAMP) AS dht_depart,
    CAST(NULLIF(DHR_Depart, 'NaN') AS TIMESTAMP) AS dhr_depart,
    CAST(NULLIF(IDTRAIN, 'NaN') AS FLOAT) AS id_train,
    CAST(NULLIF(IDGARE, 'NaN') AS FLOAT) AS id_gare,
    CAST(NULLIF(Jalon_passage, 'NaN') AS FLOAT) AS jalon_passage,
    CAST(NULLIF(H_Depart_ecart, 'NaN') AS FLOAT) AS h_depart_ecart,
    CAST(NULLIF(H_Arrivee_ecart, 'NaN') AS FLOAT) AS h_arrivee_ecart,
    CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation,
    CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation,
    CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj,
    CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj,
    CAST(NULLIF(Commentaire, 'NaN') AS VARCHAR) AS commentaire,
    CAST(NULLIF(Distance_origine, 'NaN') AS FLOAT) AS distance_origine,
    CAST(NULLIF(H_Arrivee_ecart1, 'NaN') AS FLOAT) AS h_arrivee_ecart1,
    CAST(NULLIF(H_Arrivee_ecart2, 'NaN') AS FLOAT) AS h_arrivee_ecart2,
    CAST(NULLIF(H_Depart_ecart1, 'NaN') AS FLOAT) AS h_depart_ecart1,
    CAST(NULLIF(H_Depart_ecart2, 'NaN') AS FLOAT) AS h_depart_ecart2,
    CAST(NULLIF(DHO_Depart, 'NaN') AS TIMESTAMP) AS dho_depart,
    CAST(NULLIF(DHO_Arrivee, 'NaN') AS TIMESTAMP) AS dho_arrivee
FROM (
        SELECT DISTINCT ON (IDTRAIN_JALON) *
        FROM public.train_jalon
        ORDER BY IDTRAIN_JALON
    ) train_jalon;
-- incident_concerne
DROP TABLE IF EXISTS public_processed.incident_concerne;
CREATE TABLE IF NOT EXISTS public_processed.incident_concerne AS
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
FROM public.incident_concerne;
-- incidents
DROP TABLE IF EXISTS public_processed.incidents;
CREATE TABLE IF NOT EXISTS public_processed.incidents AS
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
FROM public.incidents;
--! WAGON_DATA table
DROP TABLE IF EXISTS public_processed.wagon_data;
CREATE TABLE IF NOT EXISTS public_processed.wagon_data AS WITH train_wagon AS (
    SELECT --* IDTRAIN_LOT
        CAST(NULLIF(IDTRAIN_LOT, 'NaN') AS FLOAT) AS id_train_lot_train_wagon,
        CAST(NULLIF(IDTRAIN_WAGON, 'NaN') AS FLOAT) AS id_train_wagon_train_wagon,
        --* IDWAGON
        CAST(NULLIF(IDWAGON, 'NaN') AS FLOAT) AS id_wagon_train_wagon,
        CAST(NULLIF(Num_Position, 'NaN') AS FLOAT) AS Wagon_Order,
        CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation_train_wagon,
        CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation_train_wagon,
        CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj_train_wagon,
        CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj_train_wagon,
        --* IDWAGON_MODELE
        CAST(NULLIF(IDWAGON_MODELE, 'NaN') AS FLOAT) AS id_wagon_modele_train_wagon
    FROM public.train_wagon
),
wagon AS (
    SELECT --* IDWAGON
        CAST(NULLIF(IDWAGON, 'NaN') AS FLOAT) AS id_wagon_wagon,
        CAST(NULLIF(Code_wagon, 'NaN') AS VARCHAR) AS code_wagon_wagon,
        CAST(NULLIF(IDGARE_actuelle, 'NaN') AS FLOAT) AS id_gare_actuelle_wagon,
        CAST(NULLIF(Frein_regime, 'NaN') AS VARCHAR) AS frein_regime_wagon,
        CAST(NULLIF(Tare, 'NaN') AS FLOAT) AS Tare_Weight,
        CAST(NULLIF(ID_Proprietaire, 'NaN') AS FLOAT) AS id_proprietaire_wagon,
        CAST(NULLIF(ID_Detenteur, 'NaN') AS FLOAT) AS id_detenteur_wagon,
        CAST(NULLIF(ID_AyantDroit, 'NaN') AS FLOAT) AS id_ayantdroit_wagon,
        CAST(NULLIF(ID_Maintenance, 'NaN') AS FLOAT) AS id_maintenance_wagon,
        CAST(NULLIF(Serie, 'NaN') AS VARCHAR) AS serie_wagon,
        CAST(NULLIF(Aptitude_circulation, 'NaN') AS FLOAT) AS aptitude_circulation_wagon,
        CAST(NULLIF(Enr_actif, 'NaN') AS FLOAT) AS enr_actif_wagon,
        CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation_wagon,
        CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation_wagon,
        CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj_wagon,
        CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj_wagon,
        CAST(NULLIF(IDWAGON_MODELE, 'NaN') AS FLOAT) AS id_wagon_modele_wagon,
        CAST(
            NULLIF(Date_mise_circulation, 'NaN') AS TIMESTAMP
        ) AS Wagon_Start_Date,
        CAST(NULLIF(Voie_num, 'NaN') AS VARCHAR) AS voie_num_wagon,
        CAST(NULLIF(Circulation_statut, 'NaN') AS FLOAT) AS circulation_statut_wagon,
        CAST(NULLIF(Etat_wagon, 'NaN') AS VARCHAR) AS etat_wagon_wagon,
        CAST(NULLIF(Commentaire, 'NaN') AS VARCHAR) AS commentaire_wagon,
        CAST(NULLIF(Capteur_num, 'NaN') AS VARCHAR) AS capteur_num_wagon,
        CAST(NULLIF(Date_sortie_parc, 'NaN') AS TIMESTAMP) AS date_sortie_parc_wagon,
        CAST(NULLIF(Capacite, 'NaN') AS FLOAT) AS capacite_wagon,
        CAST(NULLIF(km_actuel, 'NaN') AS FLOAT) AS km_actuel_wagon,
        CAST(NULLIF(Wagon_client, 'NaN') AS FLOAT) AS wagon_client_wagon,
        CAST(NULLIF(GPS_performance, 'NaN') AS FLOAT) AS gps_performance_wagon
    FROM public.wagon
),
wagon_modele AS (
    SELECT --* IDWAGON_MODELE
        CAST(NULLIF(IDWAGON_MODELE, 'NaN') AS FLOAT) AS id_wagon_modele_wagon_modele,
        CAST(NULLIF(Modele_wagon, 'NaN') AS VARCHAR) AS modele_wagon_wagon_modele,
        CAST(NULLIF(Caracteristiques, 'NaN') AS VARCHAR) AS caracteristiques_wagon_modele,
        CAST(NULLIF(Type_wagon, 'NaN') AS VARCHAR) AS Wagon_Type,
        CAST(NULLIF(Lettres_categories, 'NaN') AS VARCHAR) AS lettres_categories_wagon_modele,
        CAST(NULLIF(Enr_actif, 'NaN') AS FLOAT) AS enr_actif_wagon_modele,
        CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation_wagon_modele,
        CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation_wagon_modele,
        CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj_wagon_modele,
        CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj_wagon_modele,
        CAST(NULLIF(Frein_regime, 'NaN') AS VARCHAR) AS frein_regime_wagon_modele,
        CAST(NULLIF(Vitesse_maxi, 'NaN') AS FLOAT) AS Max_Speed,
        CAST(NULLIF(Longueur_HT, 'NaN') AS FLOAT) AS longueur_ht_wagon_modele,
        CAST(NULLIF(Nb_Plateaux, 'NaN') AS FLOAT) AS nb_plateaux_wagon_modele,
        CAST(NULLIF(Longueur_utile, 'NaN') AS FLOAT) AS longueur_utile_wagon_modele,
        CAST(NULLIF(Hauteur_chargt, 'NaN') AS FLOAT) AS hauteur_chargt_wagon_modele,
        CAST(NULLIF(Charge_utile, 'NaN') AS FLOAT) AS charge_utile_wagon_modele,
        CAST(NULLIF(Charge_TEU, 'NaN') AS FLOAT) AS charge_teu_wagon_modele,
        CAST(NULLIF(Charge_essieu, 'NaN') AS FLOAT) AS charge_essieu_wagon_modele,
        CAST(NULLIF(Roue_diam, 'NaN') AS FLOAT) AS roue_diam_wagon_modele,
        CAST(NULLIF(Courbure_rayon, 'NaN') AS FLOAT) AS courbure_rayon_wagon_modele,
        CAST(NULLIF(Plancher_hauteur_rail, 'NaN') AS FLOAT) AS plancher_hauteur_rail_wagon_modele,
        CAST(NULLIF(Poche_longueur, 'NaN') AS FLOAT) AS poche_longueur_wagon_modele,
        CAST(NULLIF(Nb_essieux, 'NaN') AS FLOAT) AS nb_essieux_wagon_modele,
        CAST(NULLIF(Nb_Positions_maxi, 'NaN') AS FLOAT) AS nb_positions_maxi_wagon_modele,
        CAST(NULLIF(IDNHM_vide, 'NaN') AS FLOAT) AS id_nhm_vide_wagon_modele,
        CAST(NULLIF(IDNHM_Charge, 'NaN') AS FLOAT) AS id_nhm_charge_wagon_modele,
        CAST(NULLIF(Tare, 'NaN') AS FLOAT) AS Wagon_Model_Tare_Weight,
        CAST(NULLIF(Chargement_mode, 'NaN') AS VARCHAR) AS chargement_mode_wagon_modele,
        CAST(NULLIF(CentreCout, 'NaN') AS VARCHAR) AS centrecout_wagon_modele,
        CAST(
            NULLIF(Regroupement_analytique_wagon, 'NaN') AS VARCHAR
        ) AS regroupement_analytique_wagon_wagon_modele
    FROM public.wagon_modele
)
SELECT *
FROM train_wagon
    LEFT JOIN wagon ON train_wagon.id_wagon_train_wagon = wagon.id_wagon_wagon
    LEFT JOIN wagon_modele ON train_wagon.id_wagon_modele_train_wagon = wagon_modele.id_wagon_modele_wagon_modele;
-------
DROP TABLE IF EXISTS public_processed.wagon_data_unstack;
CREATE TABLE IF NOT EXISTS public_processed.wagon_data_unstack AS WITH wagon_data_to_unstack AS (
    SELECT id_train_lot_train_wagon AS idtrain_lot,
        Wagon_Order,
        id_wagon_train_wagon,
        modele_wagon_wagon_modele,
        Wagon_Type,
        Max_Speed,
        Wagon_Model_Tare_Weight,
        Tare_Weight,
        Wagon_Start_Date
    FROM public_processed.wagon_data
    ORDER BY id_train_lot_train_wagon,
        Wagon_Order
) -- unstacking_data will change depending on max value of Wagon_Order (now is 35)
SELECT idtrain_lot,
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN id_wagon_train_wagon
        END
    ) AS "idwagon_35",
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN modele_wagon_wagon_modele
        END
    ) AS "modele_wagon_35",
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN Wagon_Type
        END
    ) AS "Wagon_Type_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN Wagon_Type
        END
    ) AS "Wagon_Type_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN Wagon_Type
        END
    ) AS "Wagon_Type_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN Wagon_Type
        END
    ) AS "Wagon_Type_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN Wagon_Type
        END
    ) AS "Wagon_Type_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN Wagon_Type
        END
    ) AS "Wagon_Type_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN Wagon_Type
        END
    ) AS "Wagon_Type_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN Wagon_Type
        END
    ) AS "Wagon_Type_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN Wagon_Type
        END
    ) AS "Wagon_Type_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN Wagon_Type
        END
    ) AS "Wagon_Type_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN Wagon_Type
        END
    ) AS "Wagon_Type_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN Wagon_Type
        END
    ) AS "Wagon_Type_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN Wagon_Type
        END
    ) AS "Wagon_Type_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN Wagon_Type
        END
    ) AS "Wagon_Type_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN Wagon_Type
        END
    ) AS "Wagon_Type_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN Wagon_Type
        END
    ) AS "Wagon_Type_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN Wagon_Type
        END
    ) AS "Wagon_Type_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN Wagon_Type
        END
    ) AS "Wagon_Type_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN Wagon_Type
        END
    ) AS "Wagon_Type_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN Wagon_Type
        END
    ) AS "Wagon_Type_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN Wagon_Type
        END
    ) AS "Wagon_Type_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN Wagon_Type
        END
    ) AS "Wagon_Type_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN Wagon_Type
        END
    ) AS "Wagon_Type_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN Wagon_Type
        END
    ) AS "Wagon_Type_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN Wagon_Type
        END
    ) AS "Wagon_Type_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN Wagon_Type
        END
    ) AS "Wagon_Type_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN Wagon_Type
        END
    ) AS "Wagon_Type_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN Wagon_Type
        END
    ) AS "Wagon_Type_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN Wagon_Type
        END
    ) AS "Wagon_Type_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN Wagon_Type
        END
    ) AS "Wagon_Type_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN Wagon_Type
        END
    ) AS "Wagon_Type_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN Wagon_Type
        END
    ) AS "Wagon_Type_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN Wagon_Type
        END
    ) AS "Wagon_Type_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN Wagon_Type
        END
    ) AS "Wagon_Type_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN Wagon_Type
        END
    ) AS "Wagon_Type_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN Wagon_Type
        END
    ) AS "Wagon_Type_35",
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN Max_Speed
        END
    ) AS "Max_Speed_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN Max_Speed
        END
    ) AS "Max_Speed_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN Max_Speed
        END
    ) AS "Max_Speed_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN Max_Speed
        END
    ) AS "Max_Speed_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN Max_Speed
        END
    ) AS "Max_Speed_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN Max_Speed
        END
    ) AS "Max_Speed_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN Max_Speed
        END
    ) AS "Max_Speed_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN Max_Speed
        END
    ) AS "Max_Speed_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN Max_Speed
        END
    ) AS "Max_Speed_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN Max_Speed
        END
    ) AS "Max_Speed_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN Max_Speed
        END
    ) AS "Max_Speed_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN Max_Speed
        END
    ) AS "Max_Speed_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN Max_Speed
        END
    ) AS "Max_Speed_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN Max_Speed
        END
    ) AS "Max_Speed_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN Max_Speed
        END
    ) AS "Max_Speed_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN Max_Speed
        END
    ) AS "Max_Speed_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN Max_Speed
        END
    ) AS "Max_Speed_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN Max_Speed
        END
    ) AS "Max_Speed_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN Max_Speed
        END
    ) AS "Max_Speed_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN Max_Speed
        END
    ) AS "Max_Speed_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN Max_Speed
        END
    ) AS "Max_Speed_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN Max_Speed
        END
    ) AS "Max_Speed_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN Max_Speed
        END
    ) AS "Max_Speed_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN Max_Speed
        END
    ) AS "Max_Speed_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN Max_Speed
        END
    ) AS "Max_Speed_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN Max_Speed
        END
    ) AS "Max_Speed_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN Max_Speed
        END
    ) AS "Max_Speed_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN Max_Speed
        END
    ) AS "Max_Speed_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN Max_Speed
        END
    ) AS "Max_Speed_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN Max_Speed
        END
    ) AS "Max_Speed_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN Max_Speed
        END
    ) AS "Max_Speed_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN Max_Speed
        END
    ) AS "Max_Speed_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN Max_Speed
        END
    ) AS "Max_Speed_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN Max_Speed
        END
    ) AS "Max_Speed_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN Max_Speed
        END
    ) AS "Max_Speed_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN Max_Speed
        END
    ) AS "Max_Speed_35",
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN Wagon_Model_Tare_Weight
        END
    ) AS "Wagon_Model_Tare_Weight_35",
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN Tare_Weight
        END
    ) AS "Tare_Weight_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN Tare_Weight
        END
    ) AS "Tare_Weight_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN Tare_Weight
        END
    ) AS "Tare_Weight_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN Tare_Weight
        END
    ) AS "Tare_Weight_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN Tare_Weight
        END
    ) AS "Tare_Weight_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN Tare_Weight
        END
    ) AS "Tare_Weight_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN Tare_Weight
        END
    ) AS "Tare_Weight_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN Tare_Weight
        END
    ) AS "Tare_Weight_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN Tare_Weight
        END
    ) AS "Tare_Weight_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN Tare_Weight
        END
    ) AS "Tare_Weight_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN Tare_Weight
        END
    ) AS "Tare_Weight_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN Tare_Weight
        END
    ) AS "Tare_Weight_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN Tare_Weight
        END
    ) AS "Tare_Weight_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN Tare_Weight
        END
    ) AS "Tare_Weight_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN Tare_Weight
        END
    ) AS "Tare_Weight_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN Tare_Weight
        END
    ) AS "Tare_Weight_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN Tare_Weight
        END
    ) AS "Tare_Weight_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN Tare_Weight
        END
    ) AS "Tare_Weight_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN Tare_Weight
        END
    ) AS "Tare_Weight_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN Tare_Weight
        END
    ) AS "Tare_Weight_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN Tare_Weight
        END
    ) AS "Tare_Weight_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN Tare_Weight
        END
    ) AS "Tare_Weight_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN Tare_Weight
        END
    ) AS "Tare_Weight_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN Tare_Weight
        END
    ) AS "Tare_Weight_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN Tare_Weight
        END
    ) AS "Tare_Weight_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN Tare_Weight
        END
    ) AS "Tare_Weight_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN Tare_Weight
        END
    ) AS "Tare_Weight_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN Tare_Weight
        END
    ) AS "Tare_Weight_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN Tare_Weight
        END
    ) AS "Tare_Weight_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN Tare_Weight
        END
    ) AS "Tare_Weight_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN Tare_Weight
        END
    ) AS "Tare_Weight_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN Tare_Weight
        END
    ) AS "Tare_Weight_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN Tare_Weight
        END
    ) AS "Tare_Weight_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN Tare_Weight
        END
    ) AS "Tare_Weight_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN Tare_Weight
        END
    ) AS "Tare_Weight_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN Tare_Weight
        END
    ) AS "Tare_Weight_35",
    MAX(
        CASE
            WHEN Wagon_Order = 0 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_0",
    MAX(
        CASE
            WHEN Wagon_Order = 1 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_1",
    MAX(
        CASE
            WHEN Wagon_Order = 2 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_2",
    MAX(
        CASE
            WHEN Wagon_Order = 3 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_3",
    MAX(
        CASE
            WHEN Wagon_Order = 4 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_4",
    MAX(
        CASE
            WHEN Wagon_Order = 5 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_5",
    MAX(
        CASE
            WHEN Wagon_Order = 6 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_6",
    MAX(
        CASE
            WHEN Wagon_Order = 7 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_7",
    MAX(
        CASE
            WHEN Wagon_Order = 8 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_8",
    MAX(
        CASE
            WHEN Wagon_Order = 9 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_9",
    MAX(
        CASE
            WHEN Wagon_Order = 10 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_10",
    MAX(
        CASE
            WHEN Wagon_Order = 11 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_11",
    MAX(
        CASE
            WHEN Wagon_Order = 12 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_12",
    MAX(
        CASE
            WHEN Wagon_Order = 13 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_13",
    MAX(
        CASE
            WHEN Wagon_Order = 14 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_14",
    MAX(
        CASE
            WHEN Wagon_Order = 15 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_15",
    MAX(
        CASE
            WHEN Wagon_Order = 16 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_16",
    MAX(
        CASE
            WHEN Wagon_Order = 17 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_17",
    MAX(
        CASE
            WHEN Wagon_Order = 18 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_18",
    MAX(
        CASE
            WHEN Wagon_Order = 19 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_19",
    MAX(
        CASE
            WHEN Wagon_Order = 20 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_20",
    MAX(
        CASE
            WHEN Wagon_Order = 21 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_21",
    MAX(
        CASE
            WHEN Wagon_Order = 22 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_22",
    MAX(
        CASE
            WHEN Wagon_Order = 23 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_23",
    MAX(
        CASE
            WHEN Wagon_Order = 24 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_24",
    MAX(
        CASE
            WHEN Wagon_Order = 25 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_25",
    MAX(
        CASE
            WHEN Wagon_Order = 26 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_26",
    MAX(
        CASE
            WHEN Wagon_Order = 27 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_27",
    MAX(
        CASE
            WHEN Wagon_Order = 28 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_28",
    MAX(
        CASE
            WHEN Wagon_Order = 29 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_29",
    MAX(
        CASE
            WHEN Wagon_Order = 30 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_30",
    MAX(
        CASE
            WHEN Wagon_Order = 31 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_31",
    MAX(
        CASE
            WHEN Wagon_Order = 32 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_32",
    MAX(
        CASE
            WHEN Wagon_Order = 33 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_33",
    MAX(
        CASE
            WHEN Wagon_Order = 34 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_34",
    MAX(
        CASE
            WHEN Wagon_Order = 35 THEN Wagon_Start_Date
        END
    ) AS "Wagon_Start_Date_35"
FROM wagon_data_to_unstack
GROUP BY idtrain_lot
ORDER BY idtrain_lot;
-----
SELECT *
FROM public_processed.wagon_data_unstack
LIMIT 20;
--! STATION_DATA
--* Creating stations_data table
DROP TABLE IF EXISTS public_processed.stations_data;
CREATE TABLE IF NOT EXISTS public_processed.stations_data AS WITH train_jalon AS (
    SELECT id_train_jalon AS id_train_jalon_train_jalon,
        jalon_num AS Station_Order,
        dhr_arrivee AS Actual_Arrival,
        dht_arrivee AS Planned_Arrival,
        dht_depart AS Planned_Departure,
        dhr_depart AS Actual_Departure,
        id_train AS id_train_train_jalon,
        id_gare AS id_gare_train_jalon,
        jalon_passage AS jalon_passage_train_jalon,
        h_depart_ecart AS h_depart_ecart_train_jalon,
        h_arrivee_ecart AS h_arrivee_ecart_train_jalon,
        id_utilisateur_creation AS id_utilisateur_creation_train_jalon,
        dateH_creation AS dateH_creation_train_jalon,
        id_utilisateur_maj AS id_utilisateur_maj_train_jalon,
        dateH_maj AS dateH_maj_train_jalon,
        commentaire AS commentaire_train_jalon,
        distance_origine AS distance_origine_train_jalon,
        h_arrivee_ecart1 AS h_arrivee_ecart1_train_jalon,
        h_arrivee_ecart2 AS h_arrivee_ecart2_train_jalon,
        h_depart_ecart1 AS h_depart_ecart1_train_jalon,
        h_depart_ecart2 AS h_depart_ecart2_train_jalon,
        dho_depart AS dho_depart_train_jalon,
        dho_arrivee AS dho_arrivee_train_jalon
    FROM public_processed.train_jalon
),
station AS (
    SELECT code_uic AS code_uic_station,
        id_gare AS id_gare_station,
        code AS code_station,
        nom AS nom_station,
        CASE
            WHEN nom_complet = 'Antwerpen-Buitenschoor-Combinant' THEN 'Antwerp'
            WHEN nom_complet = 'Barcelona can Tunis' THEN 'Barcelona'
            WHEN nom_complet = 'Bettembourg-Container-Terminal' THEN 'Bettembourg'
            WHEN nom_complet = 'Boulou-Perthus-(Le)' THEN 'Boulou-Perthus'
            WHEN nom_complet = 'Champigneulles' THEN 'Champigneulles'
            WHEN nom_complet = 'Clip Intermodal Sp.zo.o( Poznan)' THEN 'Poznan'
            WHEN nom_complet = 'Esch-sur-Alzette' THEN 'Esch-sur-Alzette'
            WHEN nom_complet = 'Gent-Zeehaven' THEN 'Gent'
            WHEN nom_complet = 'Kiel Schwedenkai KV' THEN 'Kiel'
            WHEN nom_complet = 'Lyon-Guillotiere-Port-Herriot' THEN 'Lyon'
            WHEN nom_complet = 'Rostock Seehafen' THEN 'Rostock'
            WHEN nom_complet = 'Trieste Campo Marzio Rive' THEN 'Trieste'
            WHEN nom_complet = 'Valenton-Local' THEN 'Valenton'
            WHEN nom_complet = 'Zeebrugge-Ramsk.-Brittania-cont' THEN 'Zeebrugge'
            ELSE nom_complet
        END AS Station_Name,
        code_pays AS Station_Country,
        id_gare_parent AS id_gare_parent_station,
        gare_frontiere AS gare_frontiere_station,
        CASE
            WHEN latitude = 0 THEN NULL
            ELSE latitude
        END AS Station_Latitude,
        CASE
            WHEN longitude = 0 THEN NULL
            ELSE longitude
        END AS Station_Longitude,
        id_utilisateur_creation AS id_utilisateur_creation_station,
        dateH_creation AS dateH_creation_station,
        id_utilisateur_maj AS id_utilisateur_maj_station,
        dateH_maj AS dateH_maj_station,
        type_gare AS type_gare_station,
        top_everysens_existe AS top_everysens_existe_station,
        geojson AS geojson_station
    FROM public_processed.station
)
SELECT *
FROM train_jalon
    LEFT JOIN station ON train_jalon.id_gare_train_jalon = station.id_gare_station;
--* Creating station_data_stops table
DROP TABLE IF EXISTS public_processed.station_data_stops;
CREATE TABLE public_processed.station_data_stops AS WITH planned_arrival AS (
    SELECT id_train_train_jalon AS id_train,
        id_gare_train_jalon AS id_gare,
        Station_Order,
        Station_Name,
        Station_Country,
        Station_Latitude,
        Station_Longitude,
        Actual_Arrival,
        Actual_Departure,
        'Planned_Arrival' AS Schedule,
        Planned_Arrival AS Plan_Timestamp
    FROM public_processed.stations_data
    WHERE Station_Order <> 1
),
planned_departure AS (
    SELECT id_train_train_jalon AS id_train,
        id_gare_train_jalon AS id_gare,
        Station_Order,
        Station_Name,
        Station_Country,
        Station_Latitude,
        Station_Longitude,
        Actual_Arrival,
        Actual_Departure,
        'Planned_Departure' AS Schedule,
        Planned_Departure AS Plan_Timestamp
    FROM public_processed.stations_data
    WHERE Station_Order <> 9999
),
scheduled AS (
    SELECT *
    FROM planned_arrival
    UNION ALL
    SELECT *
    FROM planned_departure
),
timestamp_order AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_train
            ORDER BY plan_timestamp,
                schedule ASC
        ) -1 AS Timestamp_Order,
        CASE
            WHEN Schedule = 'Planned_Arrival' THEN (Actual_Arrival - Plan_Timestamp)
            ELSE NULL
        END AS Arrive_Variance,
        CASE
            WHEN Schedule = 'Planned_Departure' THEN (Actual_Departure - Plan_Timestamp)
            ELSE NULL
        END AS Depart_Variance,
        CASE
            WHEN Actual_Arrival IS NOT NULL THEN Actual_Arrival
            ELSE Actual_Departure
        END AS Actual_Timestamp
    FROM scheduled
),
times_from_prior AS (
    SELECT *,
        Plan_Timestamp - LAG(Plan_Timestamp) OVER (
            PARTITION BY id_train
            ORDER BY Timestamp_Order
        ) AS Time_From_Prior_Planned,
        Actual_Timestamp - LAG(Actual_Timestamp) OVER (
            PARTITION BY id_train
            ORDER BY Timestamp_Order
        ) AS Time_From_Prior_Actual,
        EXTRACT(
            EPOCH
            FROM(Depart_Variance) / 60
        ) AS Depart_Variance_Mins,
        EXTRACT(
            EPOCH
            FROM(Arrive_Variance) / 60
        ) AS Arrive_Variance_Mins
    FROM timestamp_order
),
time_from_prior_plan_mins_table AS (
    SELECT *,
        EXTRACT(
            EPOCH
            FROM(Time_From_Prior_Planned)
        ) / 60 AS Time_From_Prior_Plan_Mins,
        CASE
            WHEN Timestamp_Order = 0 THEN Depart_Variance_Mins --? TODO: Apparently this is not working properly in William's process
            ELSE EXTRACT(
                EPOCH
                FROM(Time_From_Prior_Actual)
            ) / 60
        END AS Time_From_Prior_Actual_Mins
    FROM times_from_prior
),
time_from_priors AS(
    SELECT *,
        CASE
            WHEN Schedule = 'Planned_Arrival' THEN Time_From_Prior_Plan_Mins
            ELSE NULL
        END AS Travel_Time_Mins,
        CASE
            WHEN Schedule = 'Planned_Departure' THEN Time_From_Prior_Plan_Mins
            ELSE NULL
        END AS Idle_Time_Mins,
        SUM(Time_From_Prior_Plan_Mins) OVER (
            PARTITION BY id_train
            ORDER BY Timestamp_Order
        ) AS Cumm_Schedule_Mins,
        SUM(Time_From_Prior_Actual_Mins) OVER (
            PARTITION BY id_train
            ORDER BY Timestamp_Order
        ) AS Cumm_Actual_Mins
    FROM time_from_prior_plan_mins_table
),
-- distance between stations in Kilometers using Haversine in Postgresql casting longitude and latitude
distances AS (
    SELECT *,
        6371 * acos(
            cos(radians(Station_Latitude)) * cos(
                radians(
                    LAG(Station_Latitude) OVER (
                        PARTITION BY id_train
                        ORDER BY Station_Order
                    )
                )
            ) * cos(
                radians(
                    LAG(Station_Longitude) OVER (
                        PARTITION BY id_train
                        ORDER BY Station_Order
                    )
                ) - radians(Station_Longitude)
            ) + sin(radians(Station_Latitude)) * sin(
                radians(
                    LAG(Station_Latitude) OVER (
                        PARTITION BY id_train
                        ORDER BY Station_Order
                    )
                )
            )
        ) AS KM_Distance_Event
    FROM time_from_priors
),
events_actualtimestampvalid AS(
    SELECT *,
        SUM(KM_Distance_Event) OVER (
            PARTITION BY id_train
            ORDER BY Timestamp_Order
        ) AS Cumm_Distance_KM,
        KM_Distance_Event / NULLIF(Time_From_Prior_Plan_Mins / 60, 0) AS KM_HR_Event,
        CASE
            WHEN Actual_Timestamp IS NOT NULL THEN 1
            ELSE 0
        END AS Actual_Timestamp_Valid
    FROM distances
),
station_data_actuals AS (
    SELECT id_train,
        AVG(Actual_Timestamp_Valid) AS Actual_Timestamp_Perc
    FROM events_actualtimestampvalid
    GROUP BY id_train
),
aggregating_data_actuals AS (
    SELECT events_actualtimestampvalid.*,
        station_data_actuals.Actual_Timestamp_Perc AS Actual_Timestamp_Perc
    FROM events_actualtimestampvalid
        LEFT JOIN station_data_actuals ON events_actualtimestampvalid.id_train = station_data_actuals.id_train
)
SELECT *
FROM aggregating_data_actuals
ORDER BY id_train,
    Station_Order,
    Schedule;
----------
SELECT *
FROM public_processed.station_data_stops
WHERE id_train = 263;
--* Creating station_data_agg table
DROP TABLE IF EXISTS public_processed.station_data_agg;
CREATE TABLE public_processed.station_data_agg AS WITH aggregated_data AS (
    SELECT id_train,
        MIN(Origin_Station) AS Origin_Station,
        MAX(Destination_Station) AS Destination_Station,
        SUM(KM_Distance_Event) AS Train_Distance_KM,
        SUM(Time_From_Prior_Plan_Mins) AS Train_Total_Time_Mins,
        SUM(Travel_Time_Mins) AS Train_Travel_Time_Mins,
        SUM(Idle_Time_Mins) AS Train_Idle_Time_Mins,
        MIN(Plan_Timestamp) AS Planned_Departure,
        MAX(Plan_Timestamp) AS Planned_Arrival,
        MAX(
            CASE
                WHEN Station_Order = 1 THEN Depart_Variance_Mins
                ELSE NULL
            END
        ) AS Train_Depart_Variance_Mins,
        MAX(
            CASE
                WHEN Station_Order = 9999 THEN Arrive_Variance_Mins
                ELSE NULL
            END
        ) AS Train_Arrive_Variance_Mins
    FROM (
            SELECT id_train,
                KM_Distance_Event,
                Time_From_Prior_Plan_Mins,
                Travel_Time_Mins,
                Idle_Time_Mins,
                timestamp_order,
                Depart_Variance_Mins,
                Arrive_Variance_Mins,
                Station_Order,
                Plan_Timestamp,
                CASE
                    WHEN Station_Order = 1 THEN Station_Name
                    ELSE NULL
                END AS Origin_Station,
                CASE
                    WHEN Station_Order = 9999 THEN Station_Name
                    ELSE NULL
                END AS Destination_Station
            FROM public_processed.station_data_stops
        ) t0
    GROUP BY id_train
),
aggregating_speeds AS (
    SELECT *,
        Train_Distance_KM / NULLIF(Train_Travel_Time_Mins / 60, 0) AS Travel_KM_HR,
        Train_Distance_KM / NULLIF(Train_Total_Time_Mins / 60, 0) AS Total_KM_HR
    FROM aggregated_data
)
SELECT *
FROM aggregating_speeds
ORDER BY id_train;
----------
SELECT *
FROM public_processed.station_data_agg
LIMIT 20;
--* Creating final station_data table
DROP TABLE IF EXISTS public_processed.station_data;
CREATE TABLE public_processed.station_data AS WITH my_constants (start_date, end_date) AS (
    values('2019-11-01', '2021-04-06')
),
aggregating_station_data AS (
    SELECT station_data_stops.id_train,
        station_data_stops.id_gare,
        station_data_stops.Station_Order,
        station_data_stops.Station_Name,
        station_data_stops.Station_Country,
        station_data_stops.Station_Latitude,
        station_data_stops.Station_Longitude,
        station_data_stops.Actual_Arrival,
        station_data_stops.Actual_Departure,
        station_data_stops.Schedule,
        station_data_stops.Plan_Timestamp,
        station_data_stops.Timestamp_Order,
        station_data_stops.Depart_Variance,
        station_data_stops.Arrive_Variance,
        station_data_stops.Time_From_Prior_Planned,
        station_data_stops.Actual_Timestamp,
        station_data_stops.Time_From_Prior_Actual,
        station_data_stops.Depart_Variance_Mins,
        station_data_stops.Arrive_Variance_Mins,
        station_data_stops.Time_From_Prior_Plan_Mins,
        station_data_stops.Time_From_Prior_Actual_Mins,
        station_data_stops.Travel_Time_Mins,
        station_data_stops.Idle_Time_Mins,
        station_data_stops.KM_Distance_Event,
        station_data_stops.Cumm_Distance_KM,
        station_data_stops.KM_HR_Event,
        station_data_stops.Actual_Timestamp_Valid,
        station_data_stops.Actual_Timestamp_Perc,
        station_data_stops.Cumm_Schedule_Mins,
        station_data_stops.Cumm_Actual_Mins,
        station_data_agg.Origin_Station,
        station_data_agg.Destination_Station,
        station_data_agg.Train_Distance_KM,
        station_data_agg.Train_Total_Time_Mins,
        station_data_agg.Train_Travel_Time_Mins,
        station_data_agg.Train_Idle_Time_Mins,
        TO_CHAR(
            station_data_agg.Planned_Departure,
            'YYYY-MM-DD'
        ) AS Planned_Departure_Date,
        TO_CHAR(station_data_agg.Planned_Departure, 'Day') AS Planned_Departure_DOW,
        TO_CHAR(
            station_data_agg.Planned_Arrival,
            'YYYY-MM-DD'
        ) AS Planned_Arrival_Date,
        TO_CHAR(station_data_agg.Planned_Arrival, 'Day') AS Planned_Arrival_DOW,
        station_data_agg.Train_Depart_Variance_Mins,
        station_data_agg.Train_Arrive_Variance_Mins,
        station_data_agg.Travel_KM_HR,
        station_data_agg.Total_KM_HR,
        KM_Distance_Event / NULLIF(Train_Distance_KM, 0) AS Stage_Share_Journey_Distance,
        Time_From_Prior_Plan_Mins / NULLIF(Train_Total_Time_Mins, 0) AS Stage_Share_Journey_Time_Mins
    FROM public_processed.station_data_stops
        LEFT JOIN public_processed.station_data_agg ON station_data_stops.id_train = station_data_agg.id_train
),
o_d AS(
    SELECT *,
        CONCAT(
            Station_Name,
            ', ',
            Station_Country
        ) AS Station_City_Country,
        CONCAT(
            Origin_Station,
            ' | ',
            Destination_Station
        ) AS OD_Pair
    FROM aggregating_station_data,
        my_constants
    WHERE Planned_Departure_Date >= start_date
        AND Planned_Departure_Date <= end_date
),
arcs AS (
    SELECT *,
        CASE
            WHEN OD_Pair = 'Champigneulles | Bettembourg' THEN 'Champigneulles'
            WHEN OD_Pair = 'Bettembourg | Champigneulles' THEN 'Champigneulles'
            WHEN OD_Pair = 'Trieste | Bettembourg' THEN 'Trieste'
            WHEN OD_Pair = 'Bettembourg | Trieste' THEN 'Trieste'
            WHEN OD_Pair = 'Zeebrugge | Bettembourg' THEN 'Zeebrugge'
            WHEN OD_Pair = 'Bettembourg | Zeebrugge' THEN 'Zeebrugge'
            WHEN OD_Pair = 'Lyon | Bettembourg' THEN 'Lyon'
            WHEN OD_Pair = 'Bettembourg | Lyon' THEN 'Lyon'
            WHEN OD_Pair = 'Antwerp | Bettembourg' THEN 'Antwerp'
            WHEN OD_Pair = 'Bettembourg | Antwerp' THEN 'Antwerp'
            WHEN OD_Pair = 'Boulou-Perthus | Bettembourg' THEN 'Boulou-Perthus'
            WHEN OD_Pair = 'Bettembourg | Boulou-Perthus' THEN 'Boulou-Perthus'
            WHEN OD_Pair = 'Gent | Bettembourg' THEN 'Gent'
            WHEN OD_Pair = 'Bettembourg | Gent' THEN 'Gent'
            WHEN OD_Pair = 'Valenton | Esch-sur-Alzette' THEN 'Valenton'
            WHEN OD_Pair = 'Esch-sur-Alzette | Valenton' THEN 'Valenton'
            WHEN OD_Pair = 'Kiel | Bettembourg' THEN 'Kiel'
            WHEN OD_Pair = 'Bettembourg | Kiel' THEN 'Kiel'
            WHEN OD_Pair = 'Poznan | Bettembourg' THEN 'Poznan'
            WHEN OD_Pair = 'Bettembourg | Poznan' THEN 'Poznan'
            WHEN OD_Pair = 'Barcelona | Bettembourg' THEN 'Barcelona'
            WHEN OD_Pair = 'Bettembourg | Barcelona' THEN 'Barcelona'
            WHEN OD_Pair = 'Rostock | Bettembourg' THEN 'Rostock'
            WHEN OD_Pair = 'Bettembourg | Rostock' THEN 'Rostock'
            ELSE NULL
        END AS Arc
    FROM o_d
),
variances_bins AS (
    SELECT *,
        CASE
            WHEN Depart_Variance_Mins >= 0 THEN Depart_Variance_Mins
            ELSE 0
        END AS Depart_Variance_Mins_Pos,
        CASE
            WHEN Arrive_Variance_Mins >= 0 THEN Arrive_Variance_Mins
            ELSE 0
        END AS Arrive_Variance_Mins_Pos,
        CASE
            WHEN Depart_Variance_Mins > 0 THEN 1
            ELSE 0
        END AS Late_Departure_Bin,
        CASE
            WHEN Arrive_Variance_Mins > 0 THEN 1
            ELSE 0
        END AS Late_Arrival_Bin,
        CASE
            WHEN Depart_Variance_Mins > 15 THEN 1
            ELSE 0
        END AS Late_Departure_Bin_15,
        CASE
            WHEN Arrive_Variance_Mins > 15 THEN 1
            ELSE 0
        END AS Late_Arrival_Bin_15,
        CASE
            WHEN Actual_Departure IS NULL THEN 0
            ELSE 1
        END AS Actual_Departure_Bin,
        CASE
            WHEN Actual_Arrival IS NULL THEN 0
            ELSE 1
        END AS Actual_Arrival_Bin,
        CASE
            WHEN Station_Order = 1 THEN 1
            ELSE 0
        END AS Origin_Station_Bin,
        CASE
            WHEN Station_Order = 9999 THEN 1
            ELSE 0
        END AS Destination_Station_Bin
    FROM arcs
    WHERE Arc NOT IN ('Valenton', 'Barcelona')
)
SELECT *
FROM variances_bins
ORDER BY id_train,
    Station_Order,
    Schedule;
--! TRAIN_DATA TABLE
--* Creating train_od table
DROP TABLE IF EXISTS public_processed.train_od;
CREATE TABLE IF NOT EXISTS public_processed.train_od AS WITH origin AS (
    SELECT id_train_jalon_train_jalon AS id_train_jalon_origin,
        Station_Order AS jalon_num_origin,
        Actual_Arrival AS hr_arrivee_origin,
        Planned_Arrival AS dht_arrivee_origin,
        Actual_Departure AS Actual_Departure,
        Planned_Departure AS Planned_Departure,
        Actual_Departure - Planned_Departure AS Depart_Variance,
        id_train_train_jalon AS id_train_origin,
        id_gare_train_jalon AS id_gare_origin,
        jalon_passage_train_jalon AS jalon_passage_origin,
        h_depart_ecart_train_jalon AS h_depart_ecart_origin,
        h_arrivee_ecart_train_jalon AS h_arrivee_ecart_origin,
        id_utilisateur_creation_train_jalon AS id_utilisateur_creation_origin,
        dateH_creation_train_jalon AS dateH_creation_origin,
        id_utilisateur_maj_train_jalon AS id_utilisateur_maj_origin,
        dateH_maj_train_jalon AS dateH_maj_origin,
        commentaire_train_jalon AS commentaire_origin,
        distance_origine_train_jalon AS distance_origine_origin,
        h_arrivee_ecart1_train_jalon AS h_arrivee_ecart1_origin,
        h_arrivee_ecart2_train_jalon AS h_arrivee_ecart2_origin,
        h_depart_ecart1_train_jalon AS h_depart_ecart1_origin,
        h_depart_ecart2_train_jalon AS h_depart_ecart2_origin,
        dho_depart_train_jalon AS dho_depart_origin,
        dho_arrivee_train_jalon AS dho_arrivee_origin,
        code_uic_station AS code_uic_station_origin,
        id_gare_station AS id_gare_station_origin,
        code_station AS code_station_origin,
        nom_station AS nom_station_origin,
        CASE
            WHEN Station_Name = 'Antwerpen-Buitenschoor-Combinant' THEN 'Antwerp'
            WHEN Station_Name = 'Barcelona can Tunis' THEN 'Barcelona'
            WHEN Station_Name = 'Bettembourg-Container-Terminal' THEN 'Bettembourg'
            WHEN Station_Name = 'Boulou-Perthus-(Le)' THEN 'Boulou-Perthus'
            WHEN Station_Name = 'Champigneulles' THEN 'Champigneulles'
            WHEN Station_Name = 'Clip Intermodal Sp.zo.o( Poznan)' THEN 'Poznan'
            WHEN Station_Name = 'Esch-sur-Alzette' THEN 'Esch-sur-Alzette'
            WHEN Station_Name = 'Gent-Zeehaven' THEN 'Gent'
            WHEN Station_Name = 'Kiel Schwedenkai KV' THEN 'Kiel'
            WHEN Station_Name = 'Lyon-Guillotiere-Port-Herriot' THEN 'Lyon'
            WHEN Station_Name = 'Rostock Seehafen' THEN 'Rostock'
            WHEN Station_Name = 'Trieste Campo Marzio Rive' THEN 'Trieste'
            WHEN Station_Name = 'Valenton-Local' THEN 'Valenton'
            WHEN Station_Name = 'Zeebrugge-Ramsk.-Brittania-cont' THEN 'Zeebrugge'
            ELSE Station_Name
        END AS Origin_Station,
        Station_Country AS Origin_Country,
        id_gare_parent_station AS id_gare_parent_station_origin,
        gare_frontiere_station AS gare_frontiere_station_origin,
        Station_Latitude AS Origin_Latitude,
        Station_Longitude AS Origin_Longitude,
        id_utilisateur_creation_station AS id_utilisateur_creation_station_origin,
        dateH_creation_station AS dateH_creation_station_origin,
        id_utilisateur_maj_station AS id_utilisateur_maj_station_origin,
        dateH_maj_station AS dateH_maj_station_origin,
        type_gare_station AS type_gare_station_origin,
        top_everysens_existe_station AS top_everysens_existe_station_origin,
        geojson_station AS geojson_station_origin
    FROM public_processed.stations_data
    WHERE Station_Order = '1'
),
destination AS (
    SELECT id_train_jalon_train_jalon AS id_train_jalon_destination,
        Station_Order AS jalon_num_destination,
        Actual_Arrival AS Actual_Arrival,
        Planned_Arrival AS Planned_Arrival,
        Actual_Arrival - Planned_Arrival AS Arrive_Variance,
        Planned_Departure AS dht_depart_destination,
        Actual_Departure AS dhr_depart_destination,
        id_train_train_jalon AS id_train_destination,
        id_gare_train_jalon AS id_gare_destination,
        jalon_passage_train_jalon AS jalon_passage_destination,
        h_depart_ecart_train_jalon AS h_depart_ecart_destination,
        h_arrivee_ecart_train_jalon AS h_arrivee_ecart_destination,
        id_utilisateur_creation_train_jalon AS id_utilisateur_creation_destination,
        dateH_creation_train_jalon AS dateH_creation_destination,
        id_utilisateur_maj_train_jalon AS id_utilisateur_maj_destination,
        dateH_maj_train_jalon AS dateH_maj_destination,
        commentaire_train_jalon AS commentaire_destination,
        distance_origine_train_jalon AS distance_origine_destination,
        h_arrivee_ecart1_train_jalon AS h_arrivee_ecart1_destination,
        h_arrivee_ecart2_train_jalon AS h_arrivee_ecart2_destination,
        h_depart_ecart1_train_jalon AS h_depart_ecart1_destination,
        h_depart_ecart2_train_jalon AS h_depart_ecart2_destination,
        dho_depart_train_jalon AS dho_depart_destination,
        dho_arrivee_train_jalon AS dho_arrivee_destination,
        code_uic_station AS code_uic_station_destination,
        id_gare_station AS id_gare_station_destination,
        code_station AS code_station_destination,
        nom_station AS nom_station_destination,
        CASE
            WHEN Station_Name = 'Antwerpen-Buitenschoor-Combinant' THEN 'Antwerp'
            WHEN Station_Name = 'Barcelona can Tunis' THEN 'Barcelona'
            WHEN Station_Name = 'Bettembourg-Container-Terminal' THEN 'Bettembourg'
            WHEN Station_Name = 'Boulou-Perthus-(Le)' THEN 'Boulou-Perthus'
            WHEN Station_Name = 'Champigneulles' THEN 'Champigneulles'
            WHEN Station_Name = 'Clip Intermodal Sp.zo.o( Poznan)' THEN 'Poznan'
            WHEN Station_Name = 'Esch-sur-Alzette' THEN 'Esch-sur-Alzette'
            WHEN Station_Name = 'Gent-Zeehaven' THEN 'Gent'
            WHEN Station_Name = 'Kiel Schwedenkai KV' THEN 'Kiel'
            WHEN Station_Name = 'Lyon-Guillotiere-Port-Herriot' THEN 'Lyon'
            WHEN Station_Name = 'Rostock Seehafen' THEN 'Rostock'
            WHEN Station_Name = 'Trieste Campo Marzio Rive' THEN 'Trieste'
            WHEN Station_Name = 'Valenton-Local' THEN 'Valenton'
            WHEN Station_Name = 'Zeebrugge-Ramsk.-Brittania-cont' THEN 'Zeebrugge'
            ELSE Station_Name
        END AS Destination_Station,
        Station_Country AS Destination_Country,
        id_gare_parent_station AS id_gare_parent_station_destination,
        gare_frontiere_station AS gare_frontiere_station_destination,
        Station_Latitude AS Destination_Latitude,
        Station_Longitude AS Destination_Longitude,
        id_utilisateur_creation_station AS id_utilisateur_creation_station_destination,
        dateH_creation_station AS dateH_creation_station_destination,
        id_utilisateur_maj_station AS id_utilisateur_maj_station_destination,
        dateH_maj_station AS dateH_maj_station_destination,
        type_gare_station AS type_gare_station_destination,
        top_everysens_existe_station AS top_everysens_existe_station_destination,
        geojson_station AS geojson_station_destination
    FROM public_processed.stations_data
    WHERE Station_Order = '9999'
),
o_d AS (
    SELECT *,
        CONCAT(
            origin.origin_station,
            ', ',
            origin.origin_country
        ) AS Origin_City_Country,
        CONCAT(
            destination.destination_station,
            ', ',
            destination.destination_country
        ) AS Destination_City_Country,
        CONCAT(
            origin.origin_station,
            ' | ',
            destination.destination_station
        ) AS OD_Pair
    FROM origin
        INNER JOIN destination ON origin.id_train_origin = destination.id_train_destination
),
arcs AS (
    SELECT *,
        CASE
            WHEN OD_Pair = 'Champigneulles | Bettembourg' THEN 'Champigneulles'
            WHEN OD_Pair = 'Bettembourg | Champigneulles' THEN 'Champigneulles'
            WHEN OD_Pair = 'Trieste | Bettembourg' THEN 'Trieste'
            WHEN OD_Pair = 'Bettembourg | Trieste' THEN 'Trieste'
            WHEN OD_Pair = 'Zeebrugge | Bettembourg' THEN 'Zeebrugge'
            WHEN OD_Pair = 'Bettembourg | Zeebrugge' THEN 'Zeebrugge'
            WHEN OD_Pair = 'Lyon | Bettembourg' THEN 'Lyon'
            WHEN OD_Pair = 'Bettembourg | Lyon' THEN 'Lyon'
            WHEN OD_Pair = 'Antwerp | Bettembourg' THEN 'Antwerp'
            WHEN OD_Pair = 'Bettembourg | Antwerp' THEN 'Antwerp'
            WHEN OD_Pair = 'Boulou-Perthus | Bettembourg' THEN 'Boulou-Perthus'
            WHEN OD_Pair = 'Bettembourg | Boulou-Perthus' THEN 'Boulou-Perthus'
            WHEN OD_Pair = 'Gent | Bettembourg' THEN 'Gent'
            WHEN OD_Pair = 'Bettembourg | Gent' THEN 'Gent'
            WHEN OD_Pair = 'Valenton | Esch-sur-Alzette' THEN 'Valenton'
            WHEN OD_Pair = 'Esch-sur-Alzette | Valenton' THEN 'Valenton'
            WHEN OD_Pair = 'Kiel | Bettembourg' THEN 'Kiel'
            WHEN OD_Pair = 'Bettembourg | Kiel' THEN 'Kiel'
            WHEN OD_Pair = 'Poznan | Bettembourg' THEN 'Poznan'
            WHEN OD_Pair = 'Bettembourg | Poznan' THEN 'Poznan'
            WHEN OD_Pair = 'Barcelona | Bettembourg' THEN 'Barcelona'
            WHEN OD_Pair = 'Bettembourg | Barcelona' THEN 'Barcelona'
            WHEN OD_Pair = 'Rostock | Bettembourg' THEN 'Rostock'
            WHEN OD_Pair = 'Bettembourg | Rostock' THEN 'Rostock'
            ELSE NULL
        END AS Arc
    FROM o_d
),
variances_bins AS (
    SELECT t0.*,
        CASE
            WHEN t0.Depart_Variance_Mins >= 0 THEN t0.Depart_Variance_Mins
            ELSE 0
        END AS Depart_Variance_Mins_Pos,
        CASE
            WHEN t0.Arrive_Variance_Mins >= 0 THEN t0.Arrive_Variance_Mins
            ELSE 0
        END AS Arrive_Variance_Mins_Pos,
        CASE
            WHEN t0.Depart_Variance_Mins > 0 THEN 1
            ELSE 0
        END AS Late_Departure_Bin,
        CASE
            WHEN t0.Arrive_Variance_Mins > 0 THEN 1
            ELSE 0
        END AS Late_Arrival_Bin,
        CASE
            WHEN t0.Depart_Variance_Mins > 15 THEN 1
            ELSE 0
        END AS Late_Departure_Bin_15,
        CASE
            WHEN t0.Arrive_Variance_Mins > 15 THEN 1
            ELSE 0
        END AS Late_Arrival_Bin_15,
        CASE
            WHEN t0.Actual_Departure IS NULL THEN 0
            ELSE 1
        END AS Actual_Departure_Bin,
        CASE
            WHEN t0.Actual_Arrival IS NULL THEN 0
            ELSE 1
        END AS Actual_Arrival_Bin,
        CASE
            WHEN t0.Origin_Station = 'Bettembourg' THEN 1
            ELSE 0
        END AS BB_Departure_Bin
    FROM (
            SELECT *,
                EXTRACT(
                    EPOCH
                    FROM(Depart_Variance) / 60
                ) AS Depart_Variance_Mins,
                EXTRACT(
                    EPOCH
                    FROM(Arrive_Variance) / 60
                ) AS Arrive_Variance_Mins
            FROM arcs
            WHERE Arc NOT IN ('Valenton', 'Barcelona')
        ) t0
),
calendar_feats AS(
    SELECT *,
        TO_CHAR(Planned_Departure, 'YYYY-MM-DD') AS Planned_Departure_Date,
        TO_CHAR(Planned_Arrival, 'YYYY-MM-DD') AS Planned_Arrival_Date,
        TO_CHAR(Planned_Departure, 'Day') AS Planned_Departure_DOW,
        TO_CHAR(Planned_Arrival, 'Day') AS Planned_Arrival_DOW,
        DATE_PART('week', Planned_Departure) AS Depart_Week_Num
    FROM variances_bins
)
SELECT *
FROM calendar_feats;
--* Creating train_data_initial table
DROP TABLE IF EXISTS public_processed.train_data_initial;
CREATE TABLE public_processed.train_data_initial AS
SELECT *,
    CASE
        WHEN t0.Cancellation_Reason IS NULL THEN 0
        ELSE 1
    END AS Cancelled_Train_Bin
FROM (
        SELECT train_lot.id_train_lot AS IDTRAIN_LOT,
            train_lot.id_train AS IDTRAIN,
            train_lot.lot_num AS lot_num_data_init,
            train_lot.nom_lot AS nom_lot_data_init,
            train_lot.distance AS Journey_Distance,
            train_lot.poids_co2 AS weight_co2,
            train_lot.id_jalon_origine AS IDTRAIN_JALON_origin,
            train_lot.id_jalon_destination AS IDTRAIN_JALON_destination,
            train_lot.dateH_creation AS dateH_creation_data_init,
            train_lot.dateH_maj AS dateH_maj_data_init,
            train_lot.train_position AS train_position_data_init,
            train_lot.incoterm AS incoterm_data_init,
            train_lot.nbre_teu AS nbre_teu_data_init,
            train_lot.capacite_teu AS capacite_teu_data_init,
            train_lot.poids_total AS Train_Weight,
            train_lot.longueur_totale AS Train_Length,
            train_lot.dateH_suppression AS dateH_suppression_data_init,
            CASE
                WHEN train_lot.motif_suppression = 'AUTRE_OPERATEUR' THEN 'Other Operator'
                WHEN train_lot.motif_suppression = 'AUTRE OPÉRATEUR' THEN 'Other Operator'
                WHEN train_lot.motif_suppression = 'FERIE' THEN 'Holiday'
                WHEN train_lot.motif_suppression = 'GREVE' THEN 'Labor Action'
                WHEN train_lot.motif_suppression = 'FORCE_MAJEURE' THEN 'Act of God'
                ELSE train_lot.motif_suppression
            END AS Cancellation_Reason,
            train.train_num AS train_num_data_init,
            train.id_relation AS id_relation_data_init,
            train.sens AS sens_data_init,
            train.poids_maxi AS Max_Weight,
            train.longueur_maxi AS Max_Length,
            train.id_train_parent AS id_train_parent_data_init,
            train_etape.id_train_etape AS id_train_etape_data_init,
            train_etape.type_etape AS type_etape_data_init,
            train_etape.dh_theorique AS dh_theorique_data_init,
            train_etape.h_thorique_ecart AS h_thorique_ecart_data_init,
            train_etape.dh_reelle AS dh_reelle_data_init,
            train_etape.dh_theorique_fin AS dh_theorique_fin_data_init,
            train_etape.dh_reelle_fin AS dh_reelle_fin_data_init
        FROM public_processed.train_lot
            LEFT JOIN public_processed.train ON train_lot.id_train = public_processed.train.id_train
            LEFT JOIN public_processed.train_etape ON public_processed.train_lot.id_train_lot = public_processed.train_etape.id_train_lot
    ) t0;
--* Creating train_data_final table
DROP TABLE IF EXISTS public_processed.train_data_final;
CREATE TABLE public_processed.train_data_final AS WITH advanced_train_data AS (
    SELECT *
    FROM(
            SELECT DISTINCT ON (IDTRAIN) *
            FROM public_processed.train_data_initial
            ORDER BY IDTRAIN
        ) AS train_data_initial
        RIGHT JOIN public_processed.train_od ON train_data_initial.IDTRAIN = public_processed.train_od.id_train_origin
),
train_wagon_count AS (
    SELECT id_train_lot_train_wagon,
        MAX(Wagon_Order) AS wagon_count
    FROM public_processed.wagon_data
    GROUP BY id_train_lot_train_wagon
    ORDER BY id_train_lot_train_wagon
),
train_position AS(
    SELECT id_train_position,
        id_train,
        dateH_position,
        latitude,
        longitude,
        COUNT(id_train_position) OVER (
            PARTITION BY id_train
            ORDER BY dateH_position
        ) -1 AS Event_Order
    FROM public_processed.train_position
),
station_data_merge AS (
    SELECT id_train,
        Train_Distance_KM,
        Train_Total_Time_Mins,
        Train_Travel_Time_Mins,
        Train_Idle_Time_Mins,
        Travel_KM_HR,
        Total_KM_HR
    FROM public_processed.station_data_agg
),
station_data_to_unstack AS (
    SELECT id_train,
        Timestamp_Order,
        Station_Name,
        Plan_Timestamp,
        Actual_Timestamp,
        Time_From_Prior_Plan_Mins,
        Depart_Variance_Mins,
        Arrive_Variance_Mins,
        Travel_Time_Mins,
        Idle_Time_Mins,
        KM_Distance_Event,
        KM_HR_Event
    FROM public_processed.station_data
    ORDER BY id_train,
        Timestamp_Order
),
-- unstacking_data will change depending on max value of Timestamp_Order (now is 23)
station_data_unstack AS (
    SELECT id_train,
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Station_Name
            END
        ) AS "Station_Name_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Station_Name
            END
        ) AS "Station_Name_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Station_Name
            END
        ) AS "Station_Name_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Station_Name
            END
        ) AS "Station_Name_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Station_Name
            END
        ) AS "Station_Name_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Station_Name
            END
        ) AS "Station_Name_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Station_Name
            END
        ) AS "Station_Name_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Station_Name
            END
        ) AS "Station_Name_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Station_Name
            END
        ) AS "Station_Name_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Station_Name
            END
        ) AS "Station_Name_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Station_Name
            END
        ) AS "Station_Name_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Station_Name
            END
        ) AS "Station_Name_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Station_Name
            END
        ) AS "Station_Name_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Station_Name
            END
        ) AS "Station_Name_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Station_Name
            END
        ) AS "Station_Name_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Station_Name
            END
        ) AS "Station_Name_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Station_Name
            END
        ) AS "Station_Name_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Station_Name
            END
        ) AS "Station_Name_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Station_Name
            END
        ) AS "Station_Name_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Station_Name
            END
        ) AS "Station_Name_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Station_Name
            END
        ) AS "Station_Name_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Station_Name
            END
        ) AS "Station_Name_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Station_Name
            END
        ) AS "Station_Name_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Station_Name
            END
        ) AS "Station_Name_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Plan_Timestamp
            END
        ) AS "Plan_Timestamp_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Actual_Timestamp
            END
        ) AS "Actual_Timestamp_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Time_From_Prior_Plan_Mins
            END
        ) AS "Time_From_Prior_Plan_Mins_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Depart_Variance_Mins
            END
        ) AS "Depart_Variance_Mins_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Arrive_Variance_Mins
            END
        ) AS "Arrive_Variance_Mins_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Travel_Time_Mins
            END
        ) AS "Travel_Time_Mins_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN Idle_Time_Mins
            END
        ) AS "Idle_Time_Mins_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN KM_Distance_Event
            END
        ) AS "KM_Distance_Event_23",
        MAX(
            CASE
                WHEN Timestamp_Order = 0 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_0",
        MAX(
            CASE
                WHEN Timestamp_Order = 1 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_1",
        MAX(
            CASE
                WHEN Timestamp_Order = 2 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_2",
        MAX(
            CASE
                WHEN Timestamp_Order = 3 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_3",
        MAX(
            CASE
                WHEN Timestamp_Order = 4 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_4",
        MAX(
            CASE
                WHEN Timestamp_Order = 5 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_5",
        MAX(
            CASE
                WHEN Timestamp_Order = 6 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_6",
        MAX(
            CASE
                WHEN Timestamp_Order = 7 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_7",
        MAX(
            CASE
                WHEN Timestamp_Order = 8 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_8",
        MAX(
            CASE
                WHEN Timestamp_Order = 9 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_9",
        MAX(
            CASE
                WHEN Timestamp_Order = 10 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_10",
        MAX(
            CASE
                WHEN Timestamp_Order = 11 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_11",
        MAX(
            CASE
                WHEN Timestamp_Order = 12 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_12",
        MAX(
            CASE
                WHEN Timestamp_Order = 13 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_13",
        MAX(
            CASE
                WHEN Timestamp_Order = 14 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_14",
        MAX(
            CASE
                WHEN Timestamp_Order = 15 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_15",
        MAX(
            CASE
                WHEN Timestamp_Order = 16 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_16",
        MAX(
            CASE
                WHEN Timestamp_Order = 17 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_17",
        MAX(
            CASE
                WHEN Timestamp_Order = 18 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_18",
        MAX(
            CASE
                WHEN Timestamp_Order = 19 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_19",
        MAX(
            CASE
                WHEN Timestamp_Order = 20 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_20",
        MAX(
            CASE
                WHEN Timestamp_Order = 21 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_21",
        MAX(
            CASE
                WHEN Timestamp_Order = 22 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_22",
        MAX(
            CASE
                WHEN Timestamp_Order = 23 THEN KM_HR_Event
            END
        ) AS "KM_HR_Event_23"
    FROM station_data_to_unstack
    GROUP BY id_train
)
SELECT --advanced_train_data.IDTRAIN_LOT,
    --advanced_train_data.IDTRAIN,
    advanced_train_data.lot_num_data_init,
    advanced_train_data.nom_lot_data_init,
    advanced_train_data.Journey_Distance,
    advanced_train_data.weight_co2,
    advanced_train_data.IDTRAIN_JALON_origin,
    advanced_train_data.IDTRAIN_JALON_destination,
    advanced_train_data.dateH_creation_data_init,
    advanced_train_data.dateH_maj_data_init,
    advanced_train_data.train_position_data_init,
    advanced_train_data.incoterm_data_init,
    advanced_train_data.nbre_teu_data_init,
    advanced_train_data.capacite_teu_data_init,
    advanced_train_data.Train_Weight,
    advanced_train_data.Train_Length,
    advanced_train_data.dateH_suppression_data_init,
    advanced_train_data.Cancellation_Reason,
    advanced_train_data.train_num_data_init,
    advanced_train_data.id_relation_data_init,
    advanced_train_data.sens_data_init,
    advanced_train_data.Max_Weight,
    advanced_train_data.Max_Length,
    advanced_train_data.id_train_parent_data_init,
    advanced_train_data.id_train_etape_data_init,
    advanced_train_data.type_etape_data_init,
    advanced_train_data.dh_theorique_data_init,
    advanced_train_data.h_thorique_ecart_data_init,
    advanced_train_data.dh_reelle_data_init,
    advanced_train_data.dh_theorique_fin_data_init,
    advanced_train_data.dh_reelle_fin_data_init,
    advanced_train_data.Cancelled_Train_Bin,
    train_wagon_count.wagon_count,
    station_data_merge.Train_Distance_KM,
    station_data_merge.Train_Total_Time_Mins,
    station_data_merge.Train_Travel_Time_Mins,
    station_data_merge.Train_Idle_Time_Mins,
    station_data_merge.Travel_KM_HR,
    station_data_merge.Total_KM_HR,
    station_data_unstack.*,
    public_processed.wagon_data_unstack.*
FROM advanced_train_data
    LEFT JOIN train_wagon_count ON advanced_train_data.idtrain_lot = train_wagon_count.id_train_lot_train_wagon
    LEFT JOIN station_data_merge ON advanced_train_data.idtrain = station_data_merge.id_train
    LEFT JOIN station_data_unstack ON advanced_train_data.idtrain = station_data_unstack.id_train
    LEFT JOIN public_processed.wagon_data_unstack ON advanced_train_data.idtrain_lot = public_processed.wagon_data_unstack.idtrain_lot;
---------
SELECT *
FROM public_processed.train_data_final;
--! Modifying station_data TABLE
--* Creating final station_data table
DROP TABLE IF EXISTS public_processed.station_data_final;
CREATE TABLE public_processed.station_data_final AS
SELECT public_processed.train_data_final.id_train,
public_processed.station_data.id_gare,
    public_processed.station_data.Station_Order,
    public_processed.station_data.Station_Name,
    public_processed.station_data.Station_Country,
    public_processed.station_data.Station_Latitude,
    public_processed.station_data.Station_Longitude,
    public_processed.station_data.Actual_Arrival,
    public_processed.station_data.Actual_Departure,
    public_processed.station_data.Schedule,
    public_processed.station_data.Plan_Timestamp,
    public_processed.station_data.Timestamp_Order,
    public_processed.station_data.Depart_Variance,
    public_processed.station_data.Arrive_Variance,
    public_processed.station_data.Time_From_Prior_Planned,
    public_processed.station_data.Actual_Timestamp,
    public_processed.station_data.Time_From_Prior_Actual,
    public_processed.station_data.Depart_Variance_Mins,
    public_processed.station_data.Arrive_Variance_Mins,
    public_processed.station_data.Time_From_Prior_Plan_Mins,
    public_processed.station_data.Time_From_Prior_Actual_Mins,
    public_processed.station_data.Travel_Time_Mins,
    public_processed.station_data.Idle_Time_Mins,
    public_processed.station_data.KM_Distance_Event,
    public_processed.station_data.Cumm_Distance_KM,
    public_processed.station_data.KM_HR_Event,
    public_processed.station_data.Actual_Timestamp_Valid,
    public_processed.station_data.Actual_Timestamp_Perc,
    public_processed.station_data.Cumm_Schedule_Mins,
    public_processed.station_data.Cumm_Actual_Mins,
    public_processed.station_data.Origin_Station,
    public_processed.station_data.Destination_Station,
    public_processed.station_data.Train_Distance_KM,
    public_processed.station_data.Train_Total_Time_Mins,
    public_processed.station_data.Train_Travel_Time_Mins,
    public_processed.station_data.Train_Idle_Time_Mins,
    public_processed.station_data.Planned_Departure_Date,
    public_processed.station_data.Planned_Departure_DOW,
    public_processed.station_data.Planned_Arrival_Date,
    public_processed.station_data.Planned_Arrival_DOW,
    public_processed.station_data.Train_Depart_Variance_Mins,
    public_processed.station_data.Train_Arrive_Variance_Mins,
    public_processed.station_data.Travel_KM_HR,
    public_processed.station_data.Total_KM_HR,
    public_processed.station_data.Stage_Share_Journey_Distance,
    public_processed.station_data.Stage_Share_Journey_Time_Mins,
    public_processed.station_data.Station_City_Country,
    public_processed.station_data.OD_Pair,
    public_processed.station_data.Arc,
    public_processed.station_data.Depart_Variance_Mins_Pos,
    public_processed.station_data.Arrive_Variance_Mins_Pos,
    public_processed.station_data.Late_Departure_Bin,
    public_processed.station_data.Late_Arrival_Bin,
    public_processed.station_data.Late_Departure_Bin_15,
    public_processed.station_data.Late_Arrival_Bin_15,
    public_processed.station_data.Actual_Departure_Bin,
    public_processed.station_data.Actual_Arrival_Bin,
    public_processed.station_data.Origin_Station_Bin,
    public_processed.station_data.Destination_Station_Bin
FROM public_processed.station_data
    INNER JOIN public_processed.train_data_final ON public_processed.station_data.id_train = public_processed.train_data_final.id_train
ORDER BY id_train,
    Station_Order,
    Schedule;
--! Creating incident_data Table
DROP TABLE IF EXISTS public_processed.incident_data;
CREATE TABLE IF NOT EXISTS public_processed.incident_data AS WITH incident_concerne_filtered AS (
    SELECT id_incident,
        id_train_lot
    FROM (
            SELECT public_processed.incident_concerne.id_incident AS id_incident,
                MAX(public_processed.incident_concerne.id_train_lot) AS id_train_lot,
                MAX(
                    public_processed.incident_concerne.annule_trainlot
                ) AS annule_trainlot
            FROM public_processed.incident_concerne
            GROUP BY id_incident
            ORDER BY id_incident
        ) rank
    WHERE annule_trainlot = 1
),
incidents AS (
    SELECT id_incident,
        id_gare,
        type_incident,
        dateH_incident,
        lieu,
        statut,
        statut_commercial,
        statut_financier,
        gravite,
        motif_client,
        commentaire
    FROM public_processed.incidents
)
SELECT incident_concerne_filtered.id_incident AS id_incident,
    incident_concerne_filtered.id_train_lot AS id_train_lot,
    incidents.type_incident AS type_incident,
    incidents.dateH_incident AS dateH_incident,
    incidents.lieu AS lieu,
    incidents.statut AS statut,
    incidents.statut_commercial AS statut_commercial,
    incidents.statut_financier AS statut_financier,
    incidents.gravite AS gravite,
    incidents.motif_client AS motif_client,
    incidents.commentaire AS commentaire
FROM incident_concerne_filtered
    LEFT JOIN incidents ON incident_concerne_filtered.id_incident = incidents.id_incident
WHERE id_train_lot IS NOT NULL;
-----------------
--! Checking final tables
SELECT COUNT(*)
FROM public_processed.train_data_final;
SELECT COUNT(*)
FROM public_processed.station_data_final;
SELECT COUNT(*)
FROM public_processed.wagon_data;
SELECT COUNT(*)
FROM public_processed.incident_data;