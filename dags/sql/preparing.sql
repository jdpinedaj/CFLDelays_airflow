--! Creating a new schema for storing processed tables
CREATE SCHEMA IF NOT EXISTS public_processed;
--! Removing duplicates from keys and storing new tables in the new schema
-- train
DROP TABLE IF EXISTS public_processed.train;
CREATE TABLE IF NOT EXISTS public_processed.train AS
SELECT IDTRAIN AS id_train,
    Train_num AS train_num,
    IDRELATION AS id_relation,
    Sens AS sens,
    IDCONTACT_Responsable AS id_contact_responsable,
    Motif_suppression AS motif_suppression,
    IDTRAINREF_JOUR AS id_train_ref_jour,
    IDUtilisateur_creation AS id_utilisateur_creation,
    DateH_creation AS dateH_creation,
    IDUtilisateur_maj AS id_utilisateur_maj,
    DateH_maj AS dateH_maj,
    IDATE AS idate,
    Commentaire AS commentaire,
    Poids_maxi AS poids_maxi,
    Longueur_maxi AS longueur_maxi,
    DateH_suppression AS dateH_suppression,
    IDSOCIETE_suppression AS id_societe_suppression,
    IDTRAIN_Parent AS id_train_parent,
    Generer_Orfeus AS generer_orfeus
FROM (
        SELECT DISTINCT ON (IDTRAIN) *
        FROM public.train
        ORDER BY IDTRAIN
    ) train;
-- train_etape
DROP TABLE IF EXISTS public_processed.train_etape;
CREATE TABLE IF NOT EXISTS public_processed.train_etape AS
SELECT IDTRAIN_LOT AS id_train_lot,
    IDTRAIN_ETAPE AS id_train_etape,
    Type_etape AS type_etape,
    DH_Theorique AS dh_theorique,
    H_Thorique_ecart AS h_thorique_ecart,
    DH_Reelle AS dh_reelle,
    DH_Theorique_fin AS dh_theorique_fin,
    DH_Reelle_fin AS dh_reelle_fin,
    Commentaire AS commentaire,
    IDUtilisateur_maj AS id_utilisateur_maj,
    DateH_maj AS dateH_maj,
    H_Theorique_ecart1 AS h_theorique_ecart1,
    H_Theorique_ecart2 AS h_theorique_ecart2
FROM (
        SELECT DISTINCT ON (IDTRAIN_LOT) *
        FROM public.train_etape
        ORDER BY IDTRAIN_LOT
    ) train_etape;
-- train_lot
DROP TABLE IF EXISTS public_processed.train_lot;
CREATE TABLE IF NOT EXISTS public_processed.train_lot AS
SELECT IDTRAIN_LOT AS id_train_lot,
    IDTRAIN AS id_train,
    Lot_num AS lot_num,
    DHR_Arrivee AS dhr_arrivee,
    DHT_Arrivee AS dht_arrivee,
    Nom_lot AS nom_lot,
    Commentaire AS commentaire,
    Distance AS distance,
    Poids_CO2 AS poids_co2,
    IDSOCIETE_Expediteur AS id_societe_expediteur,
    IDSOCIETE_Destinataire AS id_societe_destinataire,
    IDJALON_Origine AS id_jalon_origine,
    IDJALON_Destination AS id_jalon_destination,
    Surbooking AS surbooking,
    Objectif_CA AS objectif_ca,
    Objectif_TEU AS objectif_teu,
    DHT_Depart AS dht_depart,
    DHR_Depart AS dhr_depart,
    H_Depart_ecart AS h_depart_ecart,
    H_Arrivee_ecart AS h_arrivee_ecart,
    IDUtilisateur_creation AS id_utilisateur_creation,
    DateH_creation AS dateH_creation,
    IDUtilisateur_maj AS id_utilisateur_maj,
    DateH_maj AS dateH_maj,
    Ref_Expediteur AS ref_expediteur,
    Ref_Destinataire AS ref_destinataire,
    Train_Position AS train_position,
    Incoterm AS incoterm,
    Num_LDV AS num_ldv,
    Objectif_T AS objectif_t,
    Objectif_UTI AS objectif_uti,
    Nbre_TEU AS nbre_teu,
    Capacite_TEU AS capacite_teu,
    Nbre_UTI AS nbre_uti,
    Poids_total AS poids_total,
    Status AS status,
    Longueur_totale AS longueur_totale,
    Voie_categorie AS voie_categorie,
    Voie_vitesse AS voie_vitesse,
    Motif_suppression AS motif_suppression,
    DateH_suppression AS dateH_suppression,
    IDSOCIETE_Client AS id_societe_client,
    DateH_info_fournisseur AS dateH_info_fournisseur,
    Responsable_annulation AS responsable_annulation,
    Top_reservation_interdite AS top_reservation_interdite
FROM (
        SELECT DISTINCT ON (IDTRAIN_LOT) *
        FROM public.train_lot
        ORDER BY IDTRAIN_LOT
    ) train_lot;
-- station
DROP TABLE IF EXISTS public_processed.station;
CREATE TABLE IF NOT EXISTS public_processed.station AS
SELECT Code_UIC AS code_uic,
    IDGARE AS id_gare,
    Code AS code,
    Nom AS nom,
    NomComplet AS nom_complet,
    Code_pays AS code_pays,
    IDGARE_Parent AS id_gare_parent,
    Gare_Frontiere AS gare_frontiere,
    Gare_Marchandises AS gare_marchandises,
    Latitude AS latitude,
    Longitude AS longitude,
    Enr_actif AS enr_actif,
    IDUtilisateur_creation AS id_utilisateur_creation,
    DateH_creation AS dateH_creation,
    IDUtilisateur_maj AS id_utilisateur_maj,
    DateH_maj AS dateH_maj,
    Type_gare AS type_gare,
    Top_Everysens_existe AS top_everysens_existe,
    Geojson AS geojson
FROM (
        SELECT DISTINCT ON (IDGARE) *
        FROM public.station
        ORDER BY IDGARE
    ) station;
-- train_jalon
DROP TABLE IF EXISTS public_processed.train_jalon;
CREATE TABLE IF NOT EXISTS public_processed.train_jalon AS
SELECT IDTRAIN_JALON AS id_train_jalon,
    Jalon_num AS jalon_num,
    Lieu AS lieu,
    DHR_Arrivee AS dhr_arrivee,
    DHT_Arrivee AS dht_arrivee,
    DHT_Depart AS dht_depart,
    DHR_Depart AS dhr_depart,
    IDTRAIN AS id_train,
    IDGARE AS id_gare,
    Code_pays AS code_pays,
    Latitude AS latitude,
    Longitude AS longitude,
    Jalon_passage AS jalon_passage,
    H_Depart_ecart AS h_depart_ecart,
    H_Arrivee_ecart AS h_arrivee_ecart,
    IDUtilisateur_creation AS id_utilisateur_creation,
    DateH_creation AS dateH_creation,
    IDUtilisateur_maj AS id_utilisateur_maj,
    DateH_maj AS dateH_maj,
    Commentaire AS commentaire,
    Distance_origine AS distance_origine,
    H_Arrivee_ecart1 AS h_arrivee_ecart1,
    H_Arrivee_ecart2 AS h_arrivee_ecart2,
    H_Depart_ecart1 AS h_depart_ecart1,
    H_Depart_ecart2 AS h_depart_ecart2,
    DHO_Depart AS dho_depart,
    DHO_Arrivee AS dho_arrivee
FROM (
        SELECT DISTINCT ON (IDTRAIN_JALON) *
        FROM public.train_jalon
        ORDER BY IDTRAIN_JALON
    ) train_jalon;
--! Creating wagon_data table
DROP TABLE IF EXISTS public_processed.wagon_data;
CREATE TABLE IF NOT EXISTS public_processed.wagon_data AS WITH train_wagon AS (
    SELECT IDTRAIN_WAGON AS id_train_wagon_train_wagon,
        IDWAGON AS id_wagon_train_wagon,
        --! TODO: ESTE IDWAGON ESTA EN DDECIMAL Y ESTA DANDO ERROR!
        IDTRAIN_LOT AS id_train_lot_train_wagon,
        Num_Position AS num_position_train_wagon,
        IDUtilisateur_creation AS id_utilisateur_creation_train_wagon,
        DateH_creation AS dateH_creation_train_wagon,
        IDUtilisateur_maj AS id_utilisateur_maj_train_wagon,
        DateH_maj AS dateH_maj_train_wagon,
        IDWAGON_MODELE AS id_wagon_modele_train_wagon
    FROM public.train_wagon
),
wagon AS (
    SELECT IDWAGON AS id_wagon_wagon,
        Code_wagon AS code_wagon_wagon,
        IDGARE_actuelle AS id_gare_actuelle_wagon,
        Frein_regime AS frein_regime_wagon,
        Tare AS tare_wagon,
        ID_Proprietaire AS id_proprietaire_wagon,
        ID_Detenteur AS id_detenteur_wagon,
        ID_AyantDroit AS id_ayantdroit_wagon,
        ID_Maintenance AS id_maintenance_wagon,
        Serie AS serie_wagon,
        Aptitude_circulation AS aptitude_circulation_wagon,
        km_initial AS km_initial_wagon,
        Enr_actif AS enr_actif_wagon,
        IDUtilisateur_creation AS id_utilisateur_creation_wagon,
        DateH_creation AS dateH_creation_wagon,
        IDUtilisateur_maj AS id_utilisateur_maj_wagon,
        DateH_maj AS dateH_maj_wagon,
        IDWAGON_MODELE AS id_wagon_modele_wagon,
        Date_mise_circulation AS date_mise_circulation_wagon,
        Voie_num AS voie_num_wagon,
        Circulation_statut AS circulation_statut_wagon,
        Etat_wagon AS etat_wagon_wagon,
        Commentaire AS commentaire_wagon,
        Capteur_num AS capteur_num_wagon,
        Date_sortie_parc AS date_sortie_parc_wagon,
        Capacite AS capacite_wagon,
        km_actuel AS km_actuel_wagon,
        IDFOURNISSEUR_CONTRAT AS id_fournisseur_contrat_wagon,
        Date_restitution AS date_restitution_wagon,
        Wagon_client AS wagon_client_wagon,
        Application_GPS AS application_gps_wagon,
        Top_GPS_HS AS top_gps_hs_wagon,
        GPS_performance AS gps_performance_wagon
    FROM public.wagon
),
wagon_modele AS (
    SELECT IDWAGON_MODELE AS id_wagon_modele_wagon_modele,
        Modele_wagon AS modele_wagon_wagon_modele,
        Caracteristiques AS caracteristiques_wagon_modele,
        Type_wagon AS type_wagon_wagon_modele,
        Lettres_categories AS lettres_categories_wagon_modele,
        Enr_actif AS enr_actif_wagon_modele,
        IDUtilisateur_creation AS id_utilisateur_creation_wagon_modele,
        DateH_creation AS dateH_creation_wagon_modele,
        IDUtilisateur_maj AS id_utilisateur_maj_wagon_modele,
        DateH_maj AS dateH_maj_wagon_modele,
        Frein_regime AS frein_regime_wagon_modele,
        Vitesse_maxi AS vitesse_maxi_wagon_modele,
        Longueur_HT AS longueur_ht_wagon_modele,
        Nb_Plateaux AS nb_plateaux_wagon_modele,
        Longueur_utile AS longueur_utile_wagon_modele,
        Hauteur_chargt AS hauteur_chargt_wagon_modele,
        Charge_utile AS charge_utile_wagon_modele,
        Charge_TEU AS charge_teu_wagon_modele,
        Charge_essieu AS charge_essieu_wagon_modele,
        Roue_diam AS roue_diam_wagon_modele,
        Courbure_rayon AS courbure_rayon_wagon_modele,
        Plancher_hauteur_rail AS plancher_hauteur_rail_wagon_modele,
        Poche_longueur AS poche_longueur_wagon_modele,
        Nb_essieux AS nb_essieux_wagon_modele,
        Nb_Positions_maxi AS nb_positions_maxi_wagon_modele,
        IDNHM_vide AS id_nhm_vide_wagon_modele,
        IDNHM_Charge AS id_nhm_charge_wagon_modele,
        Tare AS tare_wagon_modele,
        Chargement_mode AS chargement_mode_wagon_modele,
        CentreCout AS centrecout_wagon_modele,
        Regroupement_analytique_wagon AS regroupement_analytique_wagon_wagon_modele
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
        jalon_num AS jalon_num_train_jalon,
        dhr_arrivee AS dhr_arrivee_train_jalon,
        dht_arrivee AS dht_arrivee_train_jalon,
        dht_depart AS dht_depart_train_jalon,
        dhr_depart AS dhr_depart_train_jalon,
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
        nom_complet AS nom_complet_station,
        code_pays AS code_pays_station,
        id_gare_parent AS id_gare_parent_station,
        gare_frontiere AS gare_frontiere_station,
        latitude AS latitude_station,
        longitude AS longitude_station,
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
        lieu_train_jalon AS lieu_origin,
        dhr_arrivee_train_jalon AS hr_arrivee_origin,
        dht_arrivee_train_jalon AS dht_arrivee_origin,
        dht_depart_train_jalon AS dht_depart_origin,
        dhr_depart_train_jalon AS dhr_depart_origin,
        id_train_train_jalon AS id_train_origin,
        id_gare_train_jalon AS id_gare_origin,
        code_pays_train_jalon AS code_pays_origin,
        latitude_train_jalon AS latitude_origin,
        longitude_train_jalon AS longitude_origin,
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
        nom_complet_station AS nom_complet_station_origin,
        code_pays_station AS code_pays_station_origin,
        id_gare_parent_station AS id_gare_parent_station_origin,
        gare_frontiere_station AS gare_frontiere_station_origin,
        gare_marchandises_station AS gare_marchandises_station_origin,
        latitude_station AS latitude_station_origin,
        longitude_station AS longitude_station_origin,
        enr_actif_station AS enr_actif_station_origin,
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
        lieu_train_jalon AS lieu_destination,
        dhr_arrivee_train_jalon AS hr_arrivee_destination,
        dht_arrivee_train_jalon AS dht_arrivee_destination,
        dht_depart_train_jalon AS dht_depart_destination,
        dhr_depart_train_jalon AS dhr_depart_destination,
        id_train_train_jalon AS id_train_destination,
        id_gare_train_jalon AS id_gare_destination,
        code_pays_train_jalon AS code_pays_destination,
        latitude_train_jalon AS latitude_destination,
        longitude_train_jalon AS longitude_destination,
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
        nom_complet_station AS nom_complet_station_destination,
        code_pays_station AS code_pays_station_destination,
        id_gare_parent_station AS id_gare_parent_station_destination,
        gare_frontiere_station AS gare_frontiere_station_destination,
        gare_marchandises_station AS gare_marchandises_station_destination,
        latitude_station AS latitude_station_destination,
        longitude_station AS longitude_station_destination,
        enr_actif_station AS enr_actif_station_destination,
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
SELECT train_lot.id_train_lot AS id_train_lot_data_init,
    train_lot.id_train AS id_train_data_init,
    train_lot.lot_num AS lot_num_data_init,
    train_lot.dhr_arrivee AS hr_arrivee_data_init,
    train_lot.dht_arrivee AS dht_arrivee_data_init,
    train_lot.nom_lot AS nom_lot_data_init,
    train_lot.distance AS distance_data_init,
    train_lot.poids_co2 AS poids_co2_data_init,
    train_lot.id_jalon_origine AS id_jalon_origine_data_init,
    train_lot.id_jalon_destination AS id_jalon_destination_data_init,
    train_lot.dht_depart AS dht_depart_data_init,
    train_lot.dhr_depart AS dhr_depart_data_init,
    train_lot.h_depart_ecart AS h_depart_ecart_data_init,
    train_lot.h_arrivee_ecart AS h_arrivee_ecart_data_init,
    train_lot.dateH_creation AS dateH_creation_data_init,
    train_lot.dateH_maj AS dateH_maj_data_init,
    train_lot.train_position AS train_position_data_init,
    train_lot.incoterm AS incoterm_data_init,
    train_lot.nbre_teu AS nbre_teu_data_init,
    train_lot.capacite_teu AS capacite_teu_data_init,
    train_lot.poids_total AS poids_total_data_init,
    train_lot.longueur_totale AS longueur_totale_data_init,
    train_lot.dateH_suppression AS dateH_suppression_data_init,
    train.train_num AS train_num_data_init,
    train.id_relation AS id_relation_data_init,
    train.sens AS sens_data_init,
    train.poids_maxi AS poids_maxi_data_init,
    train.longueur_maxi AS longueur_maxi_data_init,
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
        SELECT DISTINCT ON (id_train_data_init) *
        FROM public_processed.train_data_initial
        ORDER BY id_train_data_init
    ) AS train_data_initial
    RIGHT JOIN public_processed.train_od ON train_data_initial.id_train_data_init = public_processed.train_od.id_train_origin;
--!