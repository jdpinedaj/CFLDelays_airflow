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
--! WAGON_DATA table
DROP TABLE IF EXISTS public_processed.wagon_data;
CREATE TABLE IF NOT EXISTS public_processed.wagon_data AS WITH train_wagon AS (
    SELECT CAST(NULLIF(IDTRAIN_WAGON, 'NaN') AS FLOAT) AS id_train_wagon_train_wagon,
        CAST(NULLIF(IDWAGON, 'NaN') AS FLOAT) AS id_wagon_train_wagon,
        CAST(NULLIF(IDTRAIN_LOT, 'NaN') AS FLOAT) AS id_train_lot_train_wagon,
        CAST(NULLIF(Num_Position, 'NaN') AS FLOAT) AS Wagon_Order,
        CAST(NULLIF(IDUtilisateur_creation, 'NaN') AS FLOAT) AS id_utilisateur_creation_train_wagon,
        CAST(NULLIF(DateH_creation, 'NaN') AS TIMESTAMP) AS dateH_creation_train_wagon,
        CAST(NULLIF(IDUtilisateur_maj, 'NaN') AS FLOAT) AS id_utilisateur_maj_train_wagon,
        CAST(NULLIF(DateH_maj, 'NaN') AS TIMESTAMP) AS dateH_maj_train_wagon,
        CAST(NULLIF(IDWAGON_MODELE, 'NaN') AS FLOAT) AS id_wagon_modele_train_wagon
    FROM public.train_wagon
),
wagon AS (
    SELECT CAST(NULLIF(IDWAGON, 'NaN') AS FLOAT) AS id_wagon_wagon,
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
    SELECT CAST(NULLIF(IDWAGON_MODELE, 'NaN') AS FLOAT) AS id_wagon_modele_wagon_modele,
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
SELECT *
FROM public_processed.wagon_data
ORDER BY id_train_wagon_train_wagon ASC
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
CREATE TABLE public_processed.station_data AS WITH aggregating_station_data AS (
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
    FROM aggregating_station_data
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
)
SELECT *
FROM variances_bins
ORDER BY id_train,
    Station_Order,
    Schedule;
--------
SELECT Station_Order,
    Origin_Station_Bin,
    Destination_Station_Bin
FROM public_processed.station_data;
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
--* Creating train_data table
DROP TABLE IF EXISTS public_processed.train_data;
CREATE TABLE public_processed.train_data AS WITH advanced_train_data AS (
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
--! VOY AQUI
station_data_unstack AS (
    SELECT *
    FROM public_processed.station_data
)
SELECT advanced_train_data.*,
    train_wagon_count.wagon_count,
    station_data_merge.Train_Distance_KM,
    station_data_merge.Train_Total_Time_Mins,
    station_data_merge.Train_Travel_Time_Mins,
    station_data_merge.Train_Idle_Time_Mins,
    station_data_merge.Travel_KM_HR,
    station_data_merge.Total_KM_HR
FROM advanced_train_data
    LEFT JOIN train_wagon_count ON advanced_train_data.idtrain_lot = train_wagon_count.id_train_lot_train_wagon
    LEFT JOIN station_data_merge ON advanced_train_data.idtrain = station_data_merge.id_train;