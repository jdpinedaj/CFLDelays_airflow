from airflow.providers.postgres.hooks.postgres import PostgresHook


#TODO: Read all features, and perform correlation analysis and pps (predictive power score) to remove features in an automated way
def data_from_postgres_to_pandas(postgres_conn_id, dbname, schema_name,
                                 table_name, airflow_home, location_data,
                                 file_name):
    """
    It reads the data from postgres, and then it stores it in a csv file.
    :return:
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    df = hook.get_pandas_df(sql="""
    SELECT teu_count, 
        train_length,
        total_distance_trip,	
        departure_delay,
        arrival_delay,
        distance_between_control_stations,
        weight_per_length_of_train,
        weight_per_wagon_of_train
    FROM {0}.{1}.{2}
    """.format(dbname, schema_name, table_name))
    _save_data(df, airflow_home, location_data, file_name)


def _save_data(df, airflow_home, location_data, file_name):
    df.to_csv(airflow_home + location_data + file_name, index=False)
