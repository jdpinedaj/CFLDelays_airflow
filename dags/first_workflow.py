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
## 3. Instantiate a DAG
#######################

dag = DAG(dag_id='JuanDP_DAG',
          description='This is a DAG for JuanDP',
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
create_train_wagon_table = PostgresOperator(
    task_id="create_train_wagon_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDTRAIN_WAGON VARCHAR NOT NULL,
            IDWAGON VARCHAR NOT NULL,
            IDTRAIN_LOT VARCHAR NOT NULL,
            Num_Position VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            IDWAGON_MODELE VARCHAR NOT NULL,
            Top_wagon_maitre VARCHAR NOT NULL);
        """.format(table_name='cfl.public.train_wagon'),
    dag=dag,
)

create_incident_concerne_table = PostgresOperator(
    task_id="create_incident_concerne_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDINCIDENT_CONCERNE VARCHAR NOT NULL,
            IDINCIDENT VARCHAR NOT NULL,
            IDTRAIN_UTI VARCHAR NOT NULL,
            IDTRAIN_WAGON VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            Unite VARCHAR NOT NULL,
            IDCOMMANDE_UTI VARCHAR NOT NULL,
            IDAGRES VARCHAR NOT NULL,
            IDFACTURE VARCHAR NOT NULL,
            IDTRAIN_LOT VARCHAR NOT NULL,
            IDWAGON VARCHAR NOT NULL,
            Annule_TrainLot VARCHAR NOT NULL,
            IDSOCIETE VARCHAR NOT NULL,
            IDFACTURE_FOUR VARCHAR NOT NULL);
        """.format(table_name='cfl.public.incident_concerne'),
    dag=dag,
)

#? 4.3. Populating tables

populate_train_wagon_table = ExcelToPostgresOperator(
    task_id='populate_train_wagon_table',
    target_table='cfl.public.train_wagon',
    file_name='TRAIN_WAGON.xlsx',
    identifier='id',
    dag=dag,
)

populate_incident_concerne_table = ExcelToPostgresOperator(
    task_id='populate_incident_concerne_table',
    target_table='cfl.public.incident_concerne',
    file_name='INCIDENT_CONCERNE.xlsx',
    identifier='id',
    dag=dag,
)

#######################
## 5. Setting up dependencies
#######################

start_pipeline >> creating_postgres_tables

creating_postgres_tables >> check_installed_libraries

# Creating postgres tables
creating_postgres_tables >> create_train_wagon_table
creating_postgres_tables >> create_incident_concerne_table

# Populating postgres tables
create_train_wagon_table >> populate_train_wagon_table
create_incident_concerne_table >> populate_incident_concerne_table
