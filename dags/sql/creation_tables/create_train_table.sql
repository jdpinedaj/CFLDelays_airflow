DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    IDTRAIN VARCHAR NOT NULL,
    Train_num VARCHAR NOT NULL,
    IDRELATION VARCHAR NOT NULL,
    Sens VARCHAR NOT NULL,
    IDCONTACT_Responsable VARCHAR NOT NULL,
    Motif_suppression VARCHAR NOT NULL,
    IDTRAINREF_JOUR VARCHAR NOT NULL,
    IDUtilisateur_creation VARCHAR NOT NULL,
    DateH_creation VARCHAR NOT NULL,
    IDUtilisateur_maj VARCHAR NOT NULL,
    DateH_maj VARCHAR NOT NULL,
    IDATE VARCHAR NOT NULL,
    Commentaire VARCHAR NOT NULL,
    Poids_maxi VARCHAR NOT NULL,
    Longueur_maxi VARCHAR NOT NULL,
    DateH_suppression VARCHAR NOT NULL,
    IDSOCIETE_suppression VARCHAR NOT NULL,
    IDTRAIN_Parent VARCHAR NOT NULL,
    Generer_Orfeus VARCHAR NOT NULL
);