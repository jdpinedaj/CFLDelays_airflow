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
# date = datetime.now().strftime("%Y_%m_%d")

#######################
##! 3. Instantiate a DAG
#######################

dag = DAG(dag_id='CFL_delay_prediction_v1_creating_tables',
          description='CFL_delay_prediction',
          start_date=datetime(2022, 6, 9),
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

#? 4.2. Creating empty tables

with TaskGroup(
        'create_tables',
        dag=dag,
) as create_tables:

    create_public_schema = PostgresOperator(
        task_id="create_public_schema",
        postgres_conn_id='postgres_default',
        sql='sql/create_schema.sql',
        params={'schema_name': 'public'},
        dag=dag,
    )

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

    create_stations_countries_table = PostgresOperator(
        task_id="create_stations_countries_table",
        postgres_conn_id='postgres_default',
        sql='sql/creation_tables/create_stations_countries_table.sql',
        params={'table_name': 'cfl.public.stations_countries'},
        dag=dag,
    )

    create_public_schema >> [
        create_incident_concerne_table, create_incidents_table,
        create_station_table, create_terminal_table, create_train_etape_table,
        create_train_jalon_table, create_train_lot_table,
        create_train_position_table, create_train_wagon_table,
        create_train_table, create_wagon_capacite_table,
        create_wagon_position_table, create_wagon_table,
        create_wagon_modele_table, create_stations_countries_table
    ]

#? 4.3. Populating tables

with TaskGroup(
        'populate_tables',
        dag=dag,
) as populate_tables:

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

    populate_stations_countries_table = PostgresOperator(
        task_id='populate_stations_countries_table',
        postgres_conn_id='postgres_default',
        sql='sql/creation_tables/populate_stations_countries_table.sql',
        params={'table_name': 'cfl.public.stations_countries'},
        dag=dag,
    )

    check_postgres_tables = PostgresOperator(
        task_id='check_postgres_tables',
        postgres_conn_id='postgres_default',
        sql='sql/creation_tables/check_postgres_tables.sql',
        params={'table_name': 'cfl.public.stations_countries'},
        dag=dag,
    )

    [
        populate_incident_concerne_table, populate_incidents_table,
        populate_station_table, populate_terminal_table,
        populate_train_etape_table, populate_train_jalon_table,
        populate_train_lot_table, populate_train_position_table,
        populate_train_wagon_table, populate_train_table,
        populate_wagon_capacite_table, populate_wagon_position_table,
        populate_wagon_table, populate_wagon_modele_table,
        populate_stations_countries_table
    ] >> check_postgres_tables

#######################
##! 5. Setting up dependencies
#######################

# Starting pipeline
start_pipeline >> create_tables

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
create_stations_countries_table >> populate_stations_countries_table
