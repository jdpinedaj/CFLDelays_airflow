DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDWAGON_MODELE VARCHAR NOT NULL,
    Modele_wagon VARCHAR NOT NULL,
    Caracteristiques VARCHAR NOT NULL,
    Type_wagon VARCHAR NOT NULL,
    Lettres_categories VARCHAR NOT NULL,
    Enr_actif VARCHAR NOT NULL,
    IDUtilisateur_creation VARCHAR NOT NULL,
    DateH_creation VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    Frein_regime VARCHAR NOT NULL,
    Vitesse_maxi VARCHAR NOT NULL,
    Longueur_HT VARCHAR NOT NULL,
    Longueur_HTp VARCHAR NOT NULL,
    Nb_Plateaux VARCHAR NOT NULL,
    Longueur_utile VARCHAR NOT NULL,
    Hauteur_chargt VARCHAR NOT NULL,
    Charge_utile VARCHAR NOT NULL,
    Charge_TEU VARCHAR NOT NULL,
    Charge_essieu VARCHAR NOT NULL,
    Roue_diam VARCHAR NOT NULL,
    Courbure_rayon VARCHAR NOT NULL,
    Plancher_hauteur_rail VARCHAR NOT NULL,
    Poche_longueur VARCHAR NOT NULL,
    Nb_essieux VARCHAR NOT NULL,
    Charge_AS VARCHAR NOT NULL,
    Charge_ASS VARCHAR NOT NULL,
    Charge_BS VARCHAR NOT NULL,
    Charge_BSS VARCHAR NOT NULL,
    Charge_CS VARCHAR NOT NULL,
    Charge_CSS VARCHAR NOT NULL,
    Charge_DS VARCHAR NOT NULL,
    Charge_DSS VARCHAR NOT NULL,
    Nb_Positions_maxi VARCHAR NOT NULL,
    IDNHM_vide VARCHAR NOT NULL,
    IDNHM_Charge VARCHAR NOT NULL,
    Tare VARCHAR NOT NULL,
    Capacite_Pe VARCHAR NOT NULL,
    Capacite_Pf VARCHAR NOT NULL,
    Capacite_Pg VARCHAR NOT NULL,
    Chargement_mode VARCHAR NOT NULL,
    CentreCout VARCHAR NOT NULL,
    Regroupement_analytique_wagon VARCHAR NOT NULL
);