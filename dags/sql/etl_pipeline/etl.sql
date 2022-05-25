DROP TABLE IF EXISTS cfl.public_etl.df_final_etl;
CREATE TABLE IF NOT EXISTS cfl.public_etl.df_final_etl AS WITH station_departure AS (
    SELECT df_final_for_ml.Station_Name_dep AS station_dep,
        stations_countries.country AS country_dep,
        stations_countries.lat AS lat_dep,
        stations_countries.long AS long_dep
    FROM cfl.public_ready_for_ML.df_final_for_ml
        LEFT JOIN cfl.public_processed.stations_countries ON df_final_for_ml.Station_Name_dep = stations_countries.station
),
station_arrival AS (
    SELECT df_final_for_ml.Station_Name_arriv AS station_arriv,
        stations_countries.country AS country_arriv,
        stations_countries.lat AS lat_arriv,
        stations_countries.long AS long_arriv
    FROM cfl.public_ready_for_ML.df_final_for_ml
        LEFT JOIN cfl.public_processed.stations_countries ON df_final_for_ml.Station_Name_arriv = stations_countries.station
)
SELECT df_final_for_ml.incoterm_data_init AS incoterm,
    df_final_for_ml.max_teu AS max_teu,
    df_final_for_ml.teu_count AS teu_count,
    df_final_for_ml.max_length AS max_length,
    df_final_for_ml.train_length AS train_length,
    df_final_for_ml.train_weight AS train_weight,
    df_final_for_ml.planned_departure_dow AS planned_departure_day,
    df_final_for_ml.planned_arrival_dow AS planned_arrival_day,
    df_final_for_ml.planned_arrival AS planned_arrival,
    df_final_for_ml.depart_week_num AS depart_week_number,
    df_final_for_ml.wagon_count AS wagon_count,
    df_final_for_ml.train_distance_km AS total_distance_trip,
    df_final_for_ml.train_tare_weight AS sum_tares_wagons,
    station_departure.station_dep AS departure_station,
    station_arrival.station_arriv AS arrival_station,
    station_departure.country_dep AS departure_country,
    station_arrival.country_arriv AS arrival_country,
    df_final_for_ml.Depart_Variance_Mins_dep AS departure_delay,
    df_final_for_ml.Arrive_Variance_Mins_arriv AS arrival_delay,
    df_final_for_ml.KM_Distance_Event_arriv AS distance_between_control_stations,
    df_final_for_ml.train_weight / NULLIF(df_final_for_ml.train_length, 0) AS weight_per_length_of_train,
    df_final_for_ml.train_weight / NULLIF(df_final_for_ml.wagon_count, 0) AS weight_per_wagon_of_train,
    -- station_departure.lat_dep AS latitude_station_departure,
    -- station_departure.long_dep AS longitude_station_departure,
    -- station_arrival.lat_arriv AS latitude_station_arrival,
    -- station_arrival.long_arriv AS longitude_station_arrival,
    -- CONCAT(
    --     station_departure.station_dep,
    --     '--',
    --     station_arrival.station_arriv
    -- ) AS departure_arrival_route,
    df_final_for_ml.type_incident AS incident_type,
    df_final_for_ml.gravite AS incident_gravity,
    df_final_for_ml.motif_client AS incident_customer_reason,
    --df_final_for_ml.planned_arrival
FROM cfl.public_ready_for_ML.df_final_for_ml
    LEFT JOIN station_departure ON df_final_for_ml.Station_Name_dep = station_departure.station_dep
    LEFT JOIN station_arrival ON df_final_for_ml.Station_Name_arriv = station_arrival.station_arriv
WHERE cancelled_train_bin = 0 -- Filtering cancelled trains
;