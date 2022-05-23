DROP TABLE IF EXISTS {{ params.destination_table }};
CREATE TABLE IF NOT EXISTS {{ params.destination_table }} AS
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
        FROM {{ params.origin_table }}
        ORDER BY IDTRAIN_JALON
    ) train_jalon;