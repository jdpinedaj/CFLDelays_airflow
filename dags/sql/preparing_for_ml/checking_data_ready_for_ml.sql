SELECT id_train,
    Origin_Station,
    Destination_Station,
    Station_Name_dep,
    Station_Name_arriv,
    Depart_Variance_Mins_dep,
    Arrive_Variance_Mins_arriv
FROM {{ params.table_name }}
ORDER BY id_train;