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
    CAST(NULLIF(Latitude, 'NaN') AS FLOAT) AS latitude,
    CAST(NULLIF(Longitude, 'NaN') AS FLOAT) AS longitude,
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
--! Creating wagon_data table
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
    LEFT JOIN wagon_modele ON wagon.id_wagon_modele_wagon = wagon_modele.id_wagon_modele_wagon_modele;
--! Creating stations_data table
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
        latitude AS Station_Latitude,
        longitude AS Station_Longitude,
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
--! Creating train_od table
DROP TABLE IF EXISTS public_processed.train_od;
CREATE TABLE IF NOT EXISTS public_processed.train_od AS WITH origin AS (
    SELECT id_train_jalon_train_jalon AS id_train_jalon_origin,
        jalon_num_train_jalon AS jalon_num_origin,
        Actual_Arrival AS hr_arrivee_origin,
        Planned_Arrival AS dht_arrivee_origin,
        Planned_Departure AS Planned_Departure,
        Actual_Departure AS Actual_Departure,
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
    WHERE jalon_num_train_jalon = '1'
),
destination AS (
    SELECT id_train_jalon_train_jalon AS id_train_jalon_destination,
        jalon_num_train_jalon AS jalon_num_destination,
        Actual_Arrival AS Actual_Arrival,
        Planned_Arrival AS Planned_Arrival,
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
    WHERE jalon_num_train_jalon = '9999'
)
SELECT *
FROM origin
    INNER JOIN destination ON origin.id_train_origin = destination.id_train_destination;
--! Creating train_data_initial table
DROP TABLE IF EXISTS public_processed.train_data_initial;
CREATE TABLE public_processed.train_data_initial AS
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
    LEFT JOIN public_processed.train_etape ON public_processed.train_lot.id_train_lot = public_processed.train_etape.id_train_lot;
--! Creating train_data table
DROP TABLE IF EXISTS public_processed.train_data;
CREATE TABLE public_processed.train_data AS
SELECT *
FROM(
        SELECT DISTINCT ON (IDTRAIN) *
        FROM public_processed.train_data_initial
        ORDER BY IDTRAIN
    ) AS train_data_initial
    RIGHT JOIN public_processed.train_od ON train_data_initial.IDTRAIN = public_processed.train_od.id_train_origin;
--! Creating station_data_stops table
SELECT id_train_train_jalon AS id_train,
    id_gare_train_jalon AS id_gare,
    Station_Order,
    Station_Name,
    Station_Country,
    Station_Latitude,
    Station_Longitude,
    Actual_Arrival,
    Planned_Arrival,
    LAG(planned_arrival) OVER (
        PARTITION BY id_train_train_jalon
        ORDER BY planned_arrival ASC
    ) Previous_planned_arrival,
    DATE_PART('minute', Actual_Arrival - Planned_Arrival) AS Arrival_Variance,
    Actual_Departure,
    Planned_Departure,
    LAG(Planned_Departure) OVER (
        PARTITION BY id_train_train_jalon
        ORDER BY Planned_Departure ASC
    ) Previous_planned_arrival,
    DATE_PART('minute', Actual_Departure - Planned_Departure) AS Depart_Variance
FROM public_processed.stations_data --WHERE Station_Order NOT IN (1, 9999) --? TODO: CHECK why William emoved those two rows
ORDER BY id_train,
    Station_Order;