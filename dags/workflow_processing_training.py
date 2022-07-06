#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from custom_operators.ExcelToPostgresOperator import ExcelToPostgresOperator
from scripts.loading_data import data_from_postgres_to_pandas
from scripts.training import training_model
from scripts.prediction import predicting

import os
import pandas as pd

#######################
##! 2. Default arguments
#######################

default_args = {
    'owner': 'jdpinedaj',
    'depends_on_past': False,
    'email': ['juandpineda@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

# It is possible to store all those variables as "Variables" within airflow
#SCHEDULE_INTERVAL = '00 11 * * *'
SCHEDULE_INTERVAL = '@once'
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
LOCATION_DATA = '/dags/data/'
LOCATION_MODEL = '/dags/model/'
LOCATION_PREDICT = '/dags/predict/'
POSTGRES_ADDRESS = 'host.docker.internal'
POSTGRES_PORT = 5432
POSTGRES_USERNAME = 'airflow'
POSTGRES_PASSWORD = 'airflow'
POSTGRES_DBNAME = 'cfl'

#* Those values are needed to create the connection to the Postgres database in the airflow UI
# conn = Connection(conn_id='postgres_default',
#                   conn_type='postgres',
#                   host=POSTGRES_ADDRESS,
#                   schema=POSTGRES_DBNAME,
#                   login=POSTGRES_USERNAME,
#                   password=POSTGRES_PASSWORD,
#                   port=POSTGRES_PORT)

# Additional variables
#date = datetime.now().strftime("%Y_%m_%d")

# Functions

# def file_path_tmp(file_name):
#     file_path_tmp = f"{AIRFLOW_HOME}{LOCATION_DATA}{file_name}"
#     return file_path_tmp

#######################
##! 3. Instantiate a DAG
#######################

dag = DAG(dag_id='CFL_delay_prediction_v2_processing_training',
          description='CFL_delay_prediction',
          start_date=datetime(2022, 6, 9),
          schedule_interval=SCHEDULE_INTERVAL,
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

#######################
##! 4. Tasks
#######################

#? 4.1. Checking tables

check_postgres_tables = PostgresOperator(
    task_id='check_postgres_tables',
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/check_postgres_tables.sql',
    params={'table_name': 'cfl.public.stations_countries'},
    dag=dag,
)

#? 4.2. Preprocessing tables

with TaskGroup(
        'preprocessing_tables',
        dag=dag,
) as preprocessing_tables:

    create_public_processed_schema = PostgresOperator(
        task_id="create_public_processed_schema",
        postgres_conn_id='postgres_default',
        sql='sql/create_schema.sql',
        params={'schema_name': 'public_processed'},
        dag=dag,
    )
    preprocess_incident_concerne_table = PostgresOperator(
        task_id="preprocess_incident_concerne_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_incident_concerne_table.sql',
        params={
            'origin_table': 'cfl.public.incident_concerne',
            'destination_table': 'cfl.public_processed.incident_concerne'
        },
        dag=dag,
    )

    preprocess_incidents_table = PostgresOperator(
        task_id="preprocess_incidents_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_incidents_table.sql',
        params={
            'origin_table': 'cfl.public.incidents',
            'destination_table': 'cfl.public_processed.incidents'
        },
        dag=dag,
    )

    preprocess_station_table = PostgresOperator(
        task_id="preprocess_station_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_station_table.sql',
        params={
            'origin_table': 'cfl.public.station',
            'destination_table': 'cfl.public_processed.station'
        },
        dag=dag,
    )

    preprocess_train_etape_table = PostgresOperator(
        task_id="preprocess_train_etape_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_train_etape_table.sql',
        params={
            'origin_table': 'cfl.public.train_etape',
            'destination_table': 'cfl.public_processed.train_etape'
        },
        dag=dag,
    )

    preprocess_train_jalon_table = PostgresOperator(
        task_id="preprocess_train_jalon_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_train_jalon_table.sql',
        params={
            'origin_table': 'cfl.public.train_jalon',
            'destination_table': 'cfl.public_processed.train_jalon'
        },
        dag=dag,
    )

    preprocess_train_lot_table = PostgresOperator(
        task_id="preprocess_train_lot_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_train_lot_table.sql',
        params={
            'origin_table': 'cfl.public.train_lot',
            'destination_table': 'cfl.public_processed.train_lot'
        },
        dag=dag,
    )

    preprocess_train_position_table = PostgresOperator(
        task_id="preprocess_train_position_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_train_position_table.sql',
        params={
            'origin_table': 'cfl.public.train_position',
            'destination_table': 'cfl.public_processed.train_position'
        },
        dag=dag,
    )

    preprocess_train_table = PostgresOperator(
        task_id="preprocess_train_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_train_table.sql',
        params={
            'origin_table': 'cfl.public.train',
            'destination_table': 'cfl.public_processed.train'
        },
        dag=dag,
    )

    preprocess_stations_countries_table = PostgresOperator(
        task_id="preprocess_stations_countries_table",
        postgres_conn_id='postgres_default',
        sql='sql/preprocessing_tables/preprocess_stations_countries_table.sql',
        params={
            'origin_table': 'cfl.public.stations_countries',
            'destination_table': 'cfl.public_processed.stations_countries'
        },
        dag=dag,
    )

    check_postgres_tables >> [
        preprocess_incident_concerne_table, preprocess_incidents_table,
        preprocess_station_table, preprocess_train_etape_table,
        preprocess_train_jalon_table, preprocess_train_lot_table,
        preprocess_train_position_table, preprocess_train_table,
        preprocess_stations_countries_table
    ]

#? 4.3. Merging data - Creating incident_data, station_data, train_data and wagon_data tables and checking data

with TaskGroup(
        'merging_data',
        dag=dag,
) as merging_data:

    joining_tables = PostgresOperator(
        task_id="joining_tables",
        postgres_conn_id='postgres_default',
        sql='sql/joining_tables/joining_tables.sql',
        dag=dag,
    )

    check_incident_data_table = PostgresOperator(
        task_id="check_incident_data_table",
        postgres_conn_id='postgres_default',
        sql='sql/joining_tables/checking_data_table.sql',
        params={'table_name': 'cfl.public_processed.incident_data'},
        dag=dag,
    )

    check_station_data_final_table = PostgresOperator(
        task_id="check_station_data_final_table",
        postgres_conn_id='postgres_default',
        sql='sql/joining_tables/checking_data_table.sql',
        params={'table_name': 'cfl.public_processed.station_data_final'},
        dag=dag,
    )

    check_train_data_final_table = PostgresOperator(
        task_id="check_train_data_final_table",
        postgres_conn_id='postgres_default',
        sql='sql/joining_tables/checking_data_table.sql',
        params={'table_name': 'cfl.public_processed.train_data_final'},
        dag=dag,
    )

    check_wagon_data_table = PostgresOperator(
        task_id="check_wagon_data_table",
        postgres_conn_id='postgres_default',
        sql='sql/joining_tables/checking_data_table.sql',
        params={'table_name': 'cfl.public_processed.wagon_data'},
        dag=dag,
    )

    joining_tables >> [
        check_incident_data_table, check_station_data_final_table,
        check_train_data_final_table, check_wagon_data_table
    ]

#? 4.4. Preparing dataset for ML

with TaskGroup(
        'data_for_ml',
        dag=dag,
) as data_for_ml:

    create_public_ready_for_ml_schema = PostgresOperator(
        task_id="create_public_ready_for_ml_schema",
        postgres_conn_id='postgres_default',
        sql='sql/create_schema.sql',
        params={'schema_name': 'public_ready_for_ml'},
        dag=dag,
    )

    preparing_for_ml = PostgresOperator(
        task_id="preparing_for_ml",
        postgres_conn_id='postgres_default',
        sql='sql/preparing_for_ml/preparing_for_ml.sql',
        dag=dag,
    )

    check_data_ready_for_ml = PostgresOperator(
        task_id="check_data_ready_for_ml",
        postgres_conn_id='postgres_default',
        sql='sql/preparing_for_ml/checking_data_ready_for_ml.sql',
        params={'table_name': 'cfl.public_ready_for_ML.df_final_for_ml'},
        dag=dag,
    )

    create_public_ready_for_ml_schema >> preparing_for_ml >> check_data_ready_for_ml

#? 4.5. ETL process

with TaskGroup(
        'etl_process',
        dag=dag,
) as etl_process:

    create_public_etl_schema = PostgresOperator(
        task_id="create_public_etl_schema",
        postgres_conn_id='postgres_default',
        sql='sql/create_schema.sql',
        params={'schema_name': 'public_etl'},
        dag=dag,
    )

    etl_pipeline = PostgresOperator(
        task_id="etl_pipeline",
        postgres_conn_id='postgres_default',
        sql='sql/etl_pipeline/etl.sql',
        dag=dag,
    )

    check_data_etl = PostgresOperator(
        task_id="check_data_etl",
        postgres_conn_id='postgres_default',
        sql='sql/etl_pipeline/checking_data_etl.sql',
        params={'table_name': 'cfl.public_etl.df_final_etl'},
        dag=dag,
    )

    create_public_etl_schema >> etl_pipeline >> check_data_etl

#? 4.6. ML process

with TaskGroup(
        'ml_process',
        dag=dag,
) as ml_process:

    postgres_to_pandas = PythonOperator(
        task_id="postgres_to_pandas",
        python_callable=data_from_postgres_to_pandas,
        op_kwargs={
            'postgres_conn_id': 'postgres_default',
            'dbname': POSTGRES_DBNAME,
            'schema_name': 'public_etl',
            'table_name': 'df_final_etl_no_outliers',
            'airflow_home': AIRFLOW_HOME,
            'location_data': LOCATION_DATA,
            'file_name': 'df_final_etl.csv'
        },
        dag=dag,
    )

    training_ML_model = PythonOperator(
        task_id="training_ML_model",
        python_callable=training_model,
        op_kwargs={
            'airflow_home': AIRFLOW_HOME,
            'location_data': LOCATION_DATA,
            'file_name': 'df_final_etl.csv',
            'location_model': LOCATION_MODEL,
            'model_name': 'model'
        },
        dag=dag,
    )

    postgres_to_pandas >> training_ML_model

#? 4.7. Predicting

predicting_with_model = PythonOperator(
    task_id="predicting_with_model",
    python_callable=predicting,
    op_kwargs={
        'airflow_home': AIRFLOW_HOME,
        'location_model': LOCATION_MODEL,
        'model_name': 'model',
        'location_prediction': LOCATION_PREDICT,
        'file_name_to_predict': 'data_to_predict.csv',
        'file_name_predicted': 'predicted.csv'
    },
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

# Preprocessing tables and storing them in public_processed schema
check_postgres_tables >> preprocessing_tables

# Joining tables in order to create incident_data, station_data, train_data and wagon_data tables
preprocessing_tables >> merging_data

# Preparing data to train ML models
merging_data >> data_for_ml

# ETL pipeline
data_for_ml >> etl_process

# ML process
etl_process >> ml_process

# Predicting with ML model
ml_process >> predicting_with_model
