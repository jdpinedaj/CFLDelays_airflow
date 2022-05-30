--! Including information about stations to data
DROP TABLE IF EXISTS cfl.public_etl.df_final_etl;
CREATE TABLE IF NOT EXISTS cfl.public_etl.df_final_etl AS WITH station_departure AS (
    SELECT DISTINCT Station_Name_dep AS station_dep,
        stations_countries.country AS country_dep,
        stations_countries.lat AS lat_dep,
        stations_countries.long AS long_dep
    FROM cfl.public_ready_for_ML.df_final_for_ml
        LEFT JOIN cfl.public_processed.stations_countries ON df_final_for_ml.Station_Name_dep = stations_countries.station
),
station_arrival AS (
    SELECT DISTINCT Station_Name_arriv AS station_arriv,
        stations_countries.country AS country_arriv,
        stations_countries.lat AS lat_arriv,
        stations_countries.long AS long_arriv
    FROM cfl.public_ready_for_ML.df_final_for_ml
        LEFT JOIN cfl.public_processed.stations_countries ON df_final_for_ml.Station_Name_arriv = stations_countries.station
),
joining_tables AS (
    SELECT incoterm_data_init AS incoterm,
        max_teu AS max_teu,
        teu_count AS teu_count,
        max_length AS max_length,
        train_length AS train_length,
        train_weight AS train_weight,
        planned_departure_dow AS planned_departure_day,
        planned_arrival_dow AS planned_arrival_day,
        planned_arrival AS planned_arrival,
        depart_week_num AS departure_week_number,
        wagon_count AS wagon_count,
        train_distance_km AS total_distance_trip,
        train_tare_weight AS sum_tares_wagons,
        station_departure.station_dep AS departure_station,
        station_arrival.station_arriv AS arrival_station,
        station_departure.country_dep AS departure_country,
        station_arrival.country_arriv AS arrival_country,
        Depart_Variance_Mins_dep AS departure_delay,
        Arrive_Variance_Mins_arriv AS arrival_delay,
        KM_Distance_Event_arriv AS distance_between_control_stations,
        train_weight / NULLIF(train_length, 0) AS weight_per_length_of_train,
        train_weight / NULLIF(wagon_count, 0) AS weight_per_wagon_of_train,
        station_departure.lat_dep AS latitude_station_departure,
        station_departure.long_dep AS longitude_station_departure,
        station_arrival.lat_arriv AS latitude_station_arrival,
        station_arrival.long_arriv AS longitude_station_arrival,
        CONCAT(
            station_departure.station_dep,
            '--',
            station_arrival.station_arriv
        ) AS departure_arrival_route,
        -- Filling null values with 'no_incident'
        CASE
            WHEN type_incident IS NULL THEN 'no_incident'
            ELSE type_incident
        END AS incident_type,
        CASE
            WHEN gravite IS NULL THEN 'no_incident'
            ELSE CAST(gravite AS text)
        END AS incident_gravity,
        CASE
            WHEN motif_client IS NULL THEN 'no_incident'
            ELSE motif_client
        END AS incident_customer_reason,
        TO_CHAR(planned_arrival, 'Month') AS month_arrival,
        EXTRACT(
            HOUR
            FROM planned_arrival
        ) AS hour_arrival
    FROM cfl.public_ready_for_ML.df_final_for_ml
        LEFT JOIN station_departure ON Station_Name_dep = station_departure.station_dep
        LEFT JOIN station_arrival ON Station_Name_arriv = station_arrival.station_arriv
    WHERE cancelled_train_bin = 0 -- Filtering cancelled trains
)
SELECT incoterm,
    max_teu,
    teu_count,
    max_length,
    train_length,
    train_weight,
    planned_departure_day,
    planned_arrival_day,
    departure_week_number,
    wagon_count,
    total_distance_trip,
    sum_tares_wagons,
    departure_country,
    arrival_country,
    departure_delay,
    arrival_delay,
    distance_between_control_stations,
    weight_per_length_of_train,
    weight_per_wagon_of_train,
    incident_type,
    incident_gravity,
    incident_customer_reason,
    month_arrival,
    -- arrival_night using hour_arrival
    CASE
        WHEN (
            hour_arrival >= 20
            AND hour_arrival <= 5
        ) THEN 'yes'
        ELSE 'no'
    END AS arrival_night,
    -- peak_time using hour_arrival
    CASE
        WHEN hour_arrival >= 6
        AND hour_arrival <= 9 THEN 'yes'
        WHEN hour_arrival >= 16
        AND hour_arrival <= 19 THEN 'yes'
        ELSE 'no'
    END AS peak_time
FROM joining_tables
WHERE arrival_delay IS NOT NULL -- Removing null values for arrival delay because it will be the feature to predict
;
-------------------------
--! Removing outliers in numerical features
DROP TABLE IF EXISTS cfl.public_etl.df_final_etl_no_outliers;
CREATE TABLE IF NOT EXISTS cfl.public_etl.df_final_etl_no_outliers AS WITH q1_percentiles AS (
    SELECT percentile_cont(0.03) within group (
            order by max_teu
        ) AS q1_max_teu,
        percentile_cont(0.03) within group (
            order by teu_count
        ) AS q1_teu_count,
        percentile_cont(0.03) within group (
            order by max_length
        ) AS q1_max_length,
        percentile_cont(0.03) within group (
            order by train_length
        ) AS q1_train_length,
        percentile_cont(0.03) within group (
            order by train_weight
        ) AS q1_train_weight,
        percentile_cont(0.03) within group (
            order by departure_week_number
        ) AS q1_departure_week_number,
        percentile_cont(0.03) within group (
            order by wagon_count
        ) AS q1_wagon_count,
        percentile_cont(0.03) within group (
            order by total_distance_trip
        ) AS q1_total_distance_trip,
        percentile_cont(0.03) within group (
            order by sum_tares_wagons
        ) AS q1_sum_tares_wagons,
        percentile_cont(0.03) within group (
            order by departure_delay
        ) AS q1_departure_delay,
        percentile_cont(0.03) within group (
            order by arrival_delay
        ) AS q1_arrival_delay,
        percentile_cont(0.03) within group (
            order by distance_between_control_stations
        ) AS q1_distance_between_control_stations,
        percentile_cont(0.03) within group (
            order by weight_per_length_of_train
        ) AS q1_weight_per_length_of_train,
        percentile_cont(0.03) within group (
            order by weight_per_wagon_of_train
        ) AS q1_weight_per_wagon_of_train
    FROM cfl.public_etl.df_final_etl
),
q3_percentiles AS (
    SELECT percentile_cont(0.97) within group (
            order by max_teu
        ) AS q3_max_teu,
        percentile_cont(0.97) within group (
            order by teu_count
        ) AS q3_teu_count,
        percentile_cont(0.97) within group (
            order by max_length
        ) AS q3_max_length,
        percentile_cont(0.97) within group (
            order by train_length
        ) AS q3_train_length,
        percentile_cont(0.97) within group (
            order by train_weight
        ) AS q3_train_weight,
        percentile_cont(0.97) within group (
            order by departure_week_number
        ) AS q3_departure_week_number,
        percentile_cont(0.97) within group (
            order by wagon_count
        ) AS q3_wagon_count,
        percentile_cont(0.97) within group (
            order by total_distance_trip
        ) AS q3_total_distance_trip,
        percentile_cont(0.97) within group (
            order by sum_tares_wagons
        ) AS q3_sum_tares_wagons,
        percentile_cont(0.97) within group (
            order by departure_delay
        ) AS q3_departure_delay,
        percentile_cont(0.97) within group (
            order by arrival_delay
        ) AS q3_arrival_delay,
        percentile_cont(0.97) within group (
            order by distance_between_control_stations
        ) AS q3_distance_between_control_stations,
        percentile_cont(0.97) within group (
            order by weight_per_length_of_train
        ) AS q3_weight_per_length_of_train,
        percentile_cont(0.97) within group (
            order by weight_per_wagon_of_train
        ) AS q3_weight_per_wagon_of_train
    FROM cfl.public_etl.df_final_etl
),
interquartile_range AS (
    SELECT q3_max_teu - q1_max_teu AS iqr_max_teu,
        q3_teu_count - q1_teu_count AS iqr_teu_count,
        q3_max_length - q1_max_length AS iqr_max_length,
        q3_train_length - q1_train_length AS iqr_train_length,
        q3_train_weight - q1_train_weight AS iqr_train_weight,
        q3_departure_week_number - q1_departure_week_number AS iqr_departure_week_number,
        q3_wagon_count - q1_wagon_count AS iqr_wagon_count,
        q3_total_distance_trip - q1_total_distance_trip AS iqr_total_distance_trip,
        q3_sum_tares_wagons - q1_sum_tares_wagons AS iqr_sum_tares_wagons,
        q3_departure_delay - q1_departure_delay AS iqr_departure_delay,
        q3_arrival_delay - q1_arrival_delay AS iqr_arrival_delay,
        q3_distance_between_control_stations - q1_distance_between_control_stations AS iqr_distance_between_control_stations,
        q3_weight_per_length_of_train - q1_weight_per_length_of_train AS iqr_weight_per_length_of_train,
        q3_weight_per_wagon_of_train - q1_weight_per_wagon_of_train AS iqr_weight_per_wagon_of_train
    FROM q1_percentiles,
        q3_percentiles
)
SELECT incoterm,
    max_teu,
    teu_count,
    max_length,
    train_length,
    train_weight,
    planned_departure_day,
    planned_arrival_day,
    departure_week_number,
    wagon_count,
    total_distance_trip,
    sum_tares_wagons,
    departure_country,
    arrival_country,
    departure_delay,
    arrival_delay,
    distance_between_control_stations,
    weight_per_length_of_train,
    weight_per_wagon_of_train,
    incident_type,
    incident_gravity,
    incident_customer_reason,
    month_arrival,
    arrival_night,
    peak_time
FROM (
        SELECT -- NUMERICAL FEATURES
            CASE
                WHEN max_teu < (q1_max_teu - 1.5 * iqr_max_teu)
                OR max_teu > (q3_max_teu + 1.5 * iqr_max_teu) THEN NULL
                ELSE max_teu
            END AS max_teu,
            CASE
                WHEN teu_count < (q1_teu_count - 1.5 * iqr_teu_count)
                OR teu_count > (q3_teu_count + 1.5 * iqr_teu_count) THEN NULL
                ELSE teu_count
            END AS teu_count,
            CASE
                WHEN max_length < (q1_max_length - 1.5 * iqr_max_length)
                OR max_length > (q3_max_length + 1.5 * iqr_max_length) THEN NULL
                ELSE max_length
            END AS max_length,
            CASE
                WHEN train_length < (q1_train_length - 1.5 * iqr_train_length)
                OR train_length > (q3_train_length + 1.5 * iqr_train_length) THEN NULL
                ELSE train_length
            END AS train_length,
            CASE
                WHEN train_weight < (q1_train_weight - 1.5 * iqr_train_weight)
                OR train_weight > (q3_train_weight + 1.5 * iqr_train_weight) THEN NULL
                ELSE train_weight
            END AS train_weight,
            CASE
                WHEN departure_week_number < (
                    q1_departure_week_number - 1.5 * iqr_departure_week_number
                )
                OR departure_week_number > (
                    q3_departure_week_number + 1.5 * iqr_departure_week_number
                ) THEN NULL
                ELSE departure_week_number
            END AS departure_week_number,
            CASE
                WHEN wagon_count < (q1_wagon_count - 1.5 * iqr_wagon_count)
                OR wagon_count > (q3_wagon_count + 1.5 * iqr_wagon_count) THEN NULL
                ELSE wagon_count
            END AS wagon_count,
            CASE
                WHEN total_distance_trip < (
                    q1_total_distance_trip - 1.5 * iqr_total_distance_trip
                )
                OR total_distance_trip > (
                    q3_total_distance_trip + 1.5 * iqr_total_distance_trip
                ) THEN NULL
                ELSE total_distance_trip
            END AS total_distance_trip,
            CASE
                WHEN sum_tares_wagons < (q1_sum_tares_wagons - 1.5 * iqr_sum_tares_wagons)
                OR sum_tares_wagons > (q3_sum_tares_wagons + 1.5 * iqr_sum_tares_wagons) THEN NULL
                ELSE sum_tares_wagons
            END AS sum_tares_wagons,
            CASE
                WHEN departure_delay < (q1_departure_delay - 1.5 * iqr_departure_delay)
                OR departure_delay > (q3_departure_delay + 1.5 * iqr_departure_delay) THEN NULL
                ELSE departure_delay
            END AS departure_delay,
            CASE
                WHEN arrival_delay < (q1_arrival_delay - 1.5 * iqr_arrival_delay)
                OR arrival_delay > (q3_arrival_delay + 1.5 * iqr_arrival_delay) THEN NULL
                ELSE arrival_delay
            END AS arrival_delay,
            CASE
                WHEN distance_between_control_stations < (
                    q1_distance_between_control_stations - 1.5 * iqr_distance_between_control_stations
                )
                OR distance_between_control_stations > (
                    q3_distance_between_control_stations + 1.5 * iqr_distance_between_control_stations
                ) THEN NULL
                ELSE distance_between_control_stations
            END AS distance_between_control_stations,
            CASE
                WHEN weight_per_length_of_train < (
                    q1_weight_per_length_of_train - 1.5 * iqr_weight_per_length_of_train
                )
                OR weight_per_length_of_train > (
                    q3_weight_per_length_of_train + 1.5 * iqr_weight_per_length_of_train
                ) THEN NULL
                ELSE weight_per_length_of_train
            END AS weight_per_length_of_train,
            CASE
                WHEN weight_per_wagon_of_train < (
                    q1_weight_per_wagon_of_train - 1.5 * iqr_weight_per_wagon_of_train
                )
                OR weight_per_wagon_of_train > (
                    q3_weight_per_wagon_of_train + 1.5 * iqr_weight_per_wagon_of_train
                ) THEN NULL
                ELSE weight_per_wagon_of_train
            END AS weight_per_wagon_of_train,
            -- CATEGORICAL FEATURES
            incoterm,
            planned_departure_day,
            planned_arrival_day,
            departure_country,
            arrival_country,
            incident_type,
            incident_gravity,
            incident_customer_reason,
            month_arrival,
            arrival_night,
            peak_time
        FROM cfl.public_etl.df_final_etl,
            q1_percentiles,
            q3_percentiles,
            interquartile_range
    ) removing_outliers
WHERE max_teu IS NOT NULL
    AND teu_count IS NOT NULL
    AND max_length IS NOT NULL
    AND train_length IS NOT NULL
    AND train_weight IS NOT NULL
    AND departure_week_number IS NOT NULL
    AND wagon_count IS NOT NULL
    AND total_distance_trip IS NOT NULL
    AND sum_tares_wagons IS NOT NULL
    AND departure_delay IS NOT NULL
    AND arrival_delay IS NOT NULL
    AND distance_between_control_stations IS NOT NULL
    AND weight_per_length_of_train IS NOT NULL
    AND weight_per_wagon_of_train IS NOT NULL