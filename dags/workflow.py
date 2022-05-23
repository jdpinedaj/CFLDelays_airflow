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
from custom_operators.ExcelToPostgresOperator import ExcelToPostgresOperator
from scripts.creating_postgres_tables import creating_empty_tables
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
SCHEDULE_INTERVAL = '@once'
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
LOCATION_DATA = '/dags/data/'
LOCATION_QUERIES = '/dags/data/'

#* Those values are needed to create the connection to the Postgres database in the airflow UI
# conn = Connection(conn_id='postgres_default',
#                   conn_type='postgres',
#                   host='host.docker.internal',
#                   schema='airflow',
#                   login='airflow',
#                   password='airflow',
#                   port=5432)

# Additional variables
date = datetime.now().strftime("%Y_%m_%d")

# Functions


def creating_tables():
    creating_empty_tables()


def file_path_tmp(file_name):
    file_path_tmp = f"{AIRFLOW_HOME}{LOCATION_DATA}{file_name}"
    return file_path_tmp


# def generate_delete_tasks(tables_to_delete, dag):
#     tasks = []
#     for table in tables_to_delete:
#         query = f"delete if exists {table}"
#         task = ExcelToPostgresOperator(
#             task_id=f"copying_{table}",
#             target_table=f"cf1.public.{table.upper()}_100rows",
#             dag=dag)
#         tasks.append(task)
#     return tasks

#######################
##! 3. Instantiate a DAG
#######################

dag = DAG(dag_id='CFL_delay_prediction',
          description='CFL_delay_prediction',
          start_date=datetime.now(),
          schedule_interval=SCHEDULE_INTERVAL,
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

#######################
##! 4. Tasks
#######################

#? 4.1. Starting pipeline

start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# delete_tasks = generate_delete_tasks(
#     ['tabla1', 'tabla2'],
#     dag=dag,
# )

creating_postgres_tables = PythonOperator(
    task_id='creating_postgres_tables',
    python_callable=creating_tables,
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag,
)

check_installed_libraries = BashOperator(
    task_id='check_installed_libraries',
    bash_command="pip list",
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag,
)

#? 4.2. Creating empty tables

create_incident_concerne_table = PostgresOperator(
    task_id="create_incident_concerne_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_incident_concerne_table.sql',
    params={'table_name': 'cfl.public.incident_concerne'},
    dag=dag,
)

create_incidents_table = PostgresOperator(
    task_id="create_incidents_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_incidents_table.sql',
    params={'table_name': 'cfl.public.incidents'},
    dag=dag,
)

create_station_table = PostgresOperator(
    task_id="create_station_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_station_table.sql',
    params={'table_name': 'cfl.public.station'},
    dag=dag,
)

create_terminal_table = PostgresOperator(
    task_id="create_terminal_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_terminal_table.sql',
    params={'table_name': 'cfl.public.terminal'},
    dag=dag,
)

create_train_etape_table = PostgresOperator(
    task_id="create_train_etape_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_train_etape_table.sql',
    params={'table_name': 'cfl.public.train_etape'},
    dag=dag,
)

create_train_jalon_table = PostgresOperator(
    task_id="create_train_jalon_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_train_jalon_table.sql',
    params={'table_name': 'cfl.public.train_jalon'},
    dag=dag,
)

create_train_lot_table = PostgresOperator(
    task_id="create_train_lot_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_train_lot_table.sql',
    params={'table_name': 'cfl.public.train_lot'},
    dag=dag,
)

create_train_position_table = PostgresOperator(
    task_id="create_train_position_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_train_position_table.sql',
    params={'table_name': 'cfl.public.train_position'},
    dag=dag,
)

create_train_wagon_table = PostgresOperator(
    task_id="create_train_wagon_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_train_wagon_table.sql',
    params={'table_name': 'cfl.public.train_wagon'},
    dag=dag,
)

create_train_table = PostgresOperator(
    task_id="create_train_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_train_table.sql',
    params={'table_name': 'cfl.public.train'},
    dag=dag,
)

create_wagon_capacite_table = PostgresOperator(
    task_id="create_wagon_capacite_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_wagon_capacite_table.sql',
    params={'table_name': 'cfl.public.wagon_capacite'},
    dag=dag,
)

create_wagon_position_table = PostgresOperator(
    task_id="create_wagon_position_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_wagon_position_table.sql',
    params={'table_name': 'cfl.public.wagon_position'},
    dag=dag,
)

create_wagon_table = PostgresOperator(
    task_id="create_wagon_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_wagon_table.sql',
    params={'table_name': 'cfl.public.wagon'},
    dag=dag,
)

create_wagon_modele_table = PostgresOperator(
    task_id="create_wagon_modele_table",
    postgres_conn_id='postgres_default',
    sql='sql/creation_tables/create_wagon_modele_table.sql',
    params={'table_name': 'cfl.public.wagon_modele'},
    dag=dag,
)

#? 4.3. Populating tables

populate_incident_concerne_table = ExcelToPostgresOperator(
    task_id='populate_incident_concerne_table',
    target_table='cfl.public.incident_concerne',
    file_name='INCIDENT_CONCERNE.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_incidents_table = ExcelToPostgresOperator(
    task_id='populate_incidents_table',
    target_table='cfl.public.incidents',
    file_name='INCIDENTS.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_station_table = ExcelToPostgresOperator(
    task_id='populate_station_table',
    target_table='cfl.public.station',
    file_name='STATION.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_terminal_table = ExcelToPostgresOperator(
    task_id='populate_terminal_table',
    target_table='cfl.public.terminal',
    file_name='TERMINAL.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_train_etape_table = ExcelToPostgresOperator(
    task_id='populate_train_etape_table',
    target_table='cfl.public.train_etape',
    file_name='TRAIN_ETAPE.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_train_jalon_table = ExcelToPostgresOperator(
    task_id='populate_train_jalon_table',
    target_table='cfl.public.train_jalon',
    file_name='TRAIN_JALON.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_train_lot_table = ExcelToPostgresOperator(
    task_id='populate_train_lot_table',
    target_table='cfl.public.train_lot',
    file_name='TRAIN_LOT.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_train_position_table = ExcelToPostgresOperator(
    task_id='populate_train_position_table',
    target_table='cfl.public.train_position',
    file_name='TRAIN_POSITION.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_train_wagon_table = ExcelToPostgresOperator(
    task_id='populate_train_wagon_table',
    target_table='cfl.public.train_wagon',
    file_name='TRAIN_WAGON.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_train_table = ExcelToPostgresOperator(
    task_id='populate_train_table',
    target_table='cfl.public.train',
    file_name='TRAIN.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_wagon_capacite_table = ExcelToPostgresOperator(
    task_id='populate_wagon_capacite_table',
    target_table='cfl.public.wagon_capacite',
    file_name='WAGON_CAPACITE.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_wagon_position_table = ExcelToPostgresOperator(
    task_id='populate_wagon_position_table',
    target_table='cfl.public.wagon_position',
    file_name='WAGON_POSITION.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_wagon_table = ExcelToPostgresOperator(
    task_id='populate_wagon_table',
    target_table='cfl.public.wagon',
    file_name='WAGON.xlsx',
    sheet_name=0,
    identifier='id',
    dag=dag,
)

populate_wagon_modele_table = ExcelToPostgresOperator(
    task_id='populate_wagon_modele_table',
    target_table='cfl.public.wagon_modele',
    file_name='WAGON.xlsx',
    sheet_name=1,
    identifier='id',
    dag=dag,
)

check_postgres_tables = PostgresOperator(
    task_id='check_postgres_tables',
    sql='sql/creation_tables/check_postgres_tables.sql',
    postgres_conn_id='postgres_default',
    dag=dag,
)

#? 4.4. Creating public_processed schema to store processed tables

create_public_processed_schema = PostgresOperator(
    task_id="create_public_processed_schema",
    postgres_conn_id='postgres_default',
    sql='sql/preprocessing_tables/create_public_processed_schema.sql',
    params={'schema_name': '"public_processed"'},
    dag=dag,
)

#? 4.5. Preprocessing tables

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

#? 4.6. Joining tables - Creating incident_data, station_data, train_data and wagon_data tables

joining_tables = PostgresOperator(
    task_id="joining_tables",
    postgres_conn_id='postgres_default',
    sql='sql/joining_tables/joining_tables.sql',
    dag=dag,
)

#? 4.7. Checking incident_data, station_data, train_data and wagon_data tables

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

#? 4.8. Creating public_ready_for_ml schema to store data prepared for training models

create_public_ready_for_ml_schema = PostgresOperator(
    task_id="create_public_ready_for_ml_schema",
    postgres_conn_id='postgres_default',
    sql='sql/preparing_for_ml/create_public_ready_for_ml_schema.sql',
    params={'schema_name': 'public_ready_for_ml'},
    dag=dag,
)
#? 4.9. Preparing dataset for ML

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

#######################
##! 5. Setting up dependencies
#######################

start_pipeline >> creating_postgres_tables

creating_postgres_tables >> check_installed_libraries

# Creating postgres tables

creating_postgres_tables >> [
    create_incident_concerne_table, create_incidents_table,
    create_station_table, create_terminal_table, create_train_etape_table,
    create_train_jalon_table, create_train_lot_table,
    create_train_position_table, create_train_wagon_table, create_train_table,
    create_wagon_capacite_table, create_wagon_position_table,
    create_wagon_table, create_wagon_modele_table
]

# Populating postgres tables

create_incident_concerne_table >> populate_incident_concerne_table
create_incidents_table >> populate_incidents_table
create_station_table >> populate_station_table
create_terminal_table >> populate_terminal_table
create_train_etape_table >> populate_train_etape_table
create_train_jalon_table >> populate_train_jalon_table
create_train_lot_table >> populate_train_lot_table
create_train_position_table >> populate_train_position_table
create_train_wagon_table >> populate_train_wagon_table
create_train_table >> populate_train_table
create_wagon_capacite_table >> populate_wagon_capacite_table
create_wagon_position_table >> populate_wagon_position_table
create_wagon_table >> populate_wagon_table
create_wagon_modele_table >> populate_wagon_modele_table

# Checking postgres tables

[
    populate_incident_concerne_table, populate_incidents_table,
    populate_station_table, populate_terminal_table,
    populate_train_etape_table, populate_train_jalon_table,
    populate_train_lot_table, populate_train_position_table,
    populate_train_wagon_table, populate_train_table,
    populate_wagon_capacite_table, populate_wagon_position_table,
    populate_wagon_table, populate_wagon_modele_table
] >> check_postgres_tables

# Preprocessing tables and storing them in public_processed schema

check_postgres_tables >> create_public_processed_schema

create_public_processed_schema >> [
    preprocess_incident_concerne_table, preprocess_incidents_table,
    preprocess_station_table, preprocess_train_etape_table,
    preprocess_train_jalon_table, preprocess_train_lot_table,
    preprocess_train_position_table, preprocess_train_table
]

# Joining tables in order to create incident_data, station_data, train_data and wagon_data tables

[
    preprocess_incident_concerne_table, preprocess_incidents_table,
    preprocess_station_table, preprocess_train_etape_table,
    preprocess_train_jalon_table, preprocess_train_lot_table,
    preprocess_train_position_table, preprocess_train_table
] >> joining_tables

joining_tables >> [
    check_incident_data_table, check_station_data_final_table,
    check_train_data_final_table, check_wagon_data_table
]

# Preparing data to train ML models

[
    check_incident_data_table, check_station_data_final_table,
    check_train_data_final_table, check_wagon_data_table
] >> create_public_ready_for_ml_schema

create_public_ready_for_ml_schema >> preparing_for_ml >> check_data_ready_for_ml