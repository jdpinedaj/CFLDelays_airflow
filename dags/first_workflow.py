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
from scripts.hello import hello
from scripts.helper import some_work
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


def say_hello():
    hello()


def check_location_libraries():
    some_work()
    print("It is working!")


def file_path_tmp(file_name):
    file_path_tmp = f"{AIRFLOW_HOME}{LOCATION_DATA}{file_name}"
    return file_path_tmp


# def excel_to_sql(file_name):
#     file_path = f"{AIRFLOW_HOME}{LOCATION_DATA}{file_name}"
#     df = pd.read_excel(file_path)
#     new_name = f"{file_name.split('.')[0]}".lower()
#     df.to_sql(new_name,
#               schema='airflow',
#               con=PostgresHook().get_conn(),
#               if_exists='replace',
#               index=False)

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

start_pipeline = DummyOperator(task_id='start_pipeline', dag=dag)

saying_hello = PythonOperator(
    task_id='saying_hello',
    python_callable=say_hello,
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag,
)

checking_location_libraries = PythonOperator(
    task_id='checking_location_libraries',
    python_callable=check_location_libraries,
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
        """.format(table_name='cfl.public.TRAIN_WAGON_100rows'),
    dag=dag,
)

# getting_train_wagon = PythonOperator(
#     task_id='getting_train_wagon',
#     python_callable=excel_to_sql,
#     op_kwargs={'file_name': 'TRAIN_WAGON.xlsx'},
#     retries=2,
#     retry_delay=timedelta(seconds=15),
#     dag=dag,
# )
# copy_from_train_wagon_table = PostgresOperator(
#     task_id="copy_from_train_wagon_table",
#     postgres_conn_id='postgres_default',
#     sql="""
#             COPY train_wagon FROM '{file_path}' DELIMITER ',' CSV HEADER;
#         """.format(file_path=file_path_tmp('train_wagon.sql')),
#     dag=dag,
# )

copying_train_wagon_table = ExcelToPostgresOperator(
    task_id='copying_train_wagon_table',
    sql="""
            DELETE IF EXISTS FROM train_wagon_100rows;
            COPY train_wagon_100rows FROM '{file_path}' DELIMITER ',' CSV HEADER;
            """.format(file_path=file_path_tmp('train_wagon_100rows.xlsx')),
    target_table='cfl.public.TRAIN_WAGON_100rows',
    #file_path=file_path_tmp('TRAIN_WAGON_100rows.xlsx'),
    #postgres_conn_id='postgres_default',
    identifier='id',
    dag=dag,
)

#######################
## 5. Setting up dependencies
#######################

start_pipeline >> saying_hello
saying_hello >> checking_location_libraries
checking_location_libraries >> check_installed_libraries
checking_location_libraries >> create_train_wagon_table
create_train_wagon_table >> copying_train_wagon_table
