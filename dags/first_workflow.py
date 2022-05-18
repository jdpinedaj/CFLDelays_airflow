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
## 3. Instantiate a DAG
#######################

dag = DAG(
    dag_id='JuanDP_DAG',
    description='This is a DAG for JuanDP',
    start_date=datetime.now(),
    schedule_interval=SCHEDULE_INTERVAL,
    concurrency=5,
    #!TODO: Apparently the template_searchpath is to be used to specify the location of the templates
    template_searchpath=f"{AIRFLOW_HOME}{LOCATION_QUERIES}",
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

#! TODO: Put the queries in independent files.
# create_incident_concerne_table = PostgresOperator(
#     task_id="create_incident_concerne_table",
#     postgres_conn_id='postgres_default',
#     sql='create_incident_concerne_table.sql',
#     params={'table_name': 'cfl.public.incident_concerne'},
#     dag=dag,
# )

create_incidents_table = PostgresOperator(
    task_id="create_incidents_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDINCIDENT VARCHAR NOT NULL,
            Type_incident VARCHAR NOT NULL,
            IDINCIDENT_Pere VARCHAR NOT NULL, 
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL, 
            IDUtilisateur_maj VARCHAR NOT NULL, 
            DateH_maj VARCHAR NOT NULL, 
            DateH_incident VARCHAR NOT NULL,
            Lieu VARCHAR NOT NULL,
            Num_incident VARCHAR NOT NULL, 
            Commentaire VARCHAR NOT NULL, 
            Statut VARCHAR NOT NULL, 
            Gravite VARCHAR NOT NULL, 
            Motif_client VARCHAR NOT NULL,
            IDGARE VARCHAR NOT NULL, 
            Statut_commercial VARCHAR NOT NULL, 
            Statut_financier VARCHAR NOT NULL);
        """.format(table_name='cfl.public.incidents'),
    dag=dag,
)

create_station_table = PostgresOperator(
    task_id="create_station_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            Code_UIC VARCHAR NOT NULL,
            IDGARE VARCHAR NOT NULL,
            Code VARCHAR NOT NULL,
            Nom VARCHAR NOT NULL,
            NomComplet VARCHAR NOT NULL,
            Code_pays VARCHAR NOT NULL,
            IDGARE_Parent VARCHAR NOT NULL,
            Gare_Frontiere VARCHAR NOT NULL,
            Gare_Marchandises VARCHAR NOT NULL,
            Latitude VARCHAR NOT NULL,
            Longitude VARCHAR NOT NULL,
            Enr_actif VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            Type_gare VARCHAR NOT NULL,
            Top_Everysens_existe VARCHAR NOT NULL,
            Geojson VARCHAR NOT NULL);
        """.format(table_name='cfl.public.station'),
    dag=dag,
)

create_terminal_table = PostgresOperator(
    task_id="create_terminal_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDTERMINAL VARCHAR NOT NULL,
            Nom_terminal VARCHAR NOT NULL,
            IDGARE_Desserte VARCHAR NOT NULL,
            Enr_actif VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDSOCIETE VARCHAR NOT NULL,
            Nom_court VARCHAR NOT NULL,
            IDGARE_Reseau VARCHAR NOT NULL,
            Maritime VARCHAR NOT NULL);
        """.format(table_name='cfl.public.terminal'),
    dag=dag,
)

create_train_etape_table = PostgresOperator(
    task_id="create_train_etape_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (   
            id SERIAL PRIMARY KEY,
            IDTRAIN_ETAPE VARCHAR NOT NULL,
            IDTRAIN_LOT VARCHAR NOT NULL,
            Type_etape VARCHAR NOT NULL,
            DH_Theorique VARCHAR NOT NULL,
            H_Thorique_ecart VARCHAR NOT NULL,
            DH_Reelle VARCHAR NOT NULL,
            DH_Theorique_fin VARCHAR NOT NULL,
            DH_Reelle_fin VARCHAR NOT NULL,
            Commentaire VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            H_Theorique_ecart1 VARCHAR NOT NULL,
            H_Theorique_ecart2 VARCHAR NOT NULL);
        """.format(table_name='cfl.public.train_etape'),
    dag=dag,
)

create_train_jalon_table = PostgresOperator(
    task_id="create_train_jalon_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDTRAIN_JALON VARCHAR NOT NULL,
            Jalon_num VARCHAR NOT NULL,
            Lieu VARCHAR NOT NULL,
            DHR_Arrivee VARCHAR NOT NULL,
            DHT_Arrivee VARCHAR NOT NULL,
            DHT_Depart VARCHAR NOT NULL,
            DHR_Depart VARCHAR NOT NULL,
            IDTRAIN VARCHAR NOT NULL,
            IDGARE VARCHAR NOT NULL,
            Code_pays VARCHAR NOT NULL,
            Latitude VARCHAR NOT NULL,
            Longitude VARCHAR NOT NULL,
            Jalon_passage VARCHAR NOT NULL,
            H_Depart_ecart VARCHAR NOT NULL,
            H_Arrivee_ecart VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            Commentaire VARCHAR NOT NULL,
            Distance_origine VARCHAR NOT NULL,
            H_Arrivee_ecart1 VARCHAR NOT NULL,
            H_Arrivee_ecart2 VARCHAR NOT NULL,
            H_Depart_ecart1 VARCHAR NOT NULL,
            H_Depart_ecart2 VARCHAR NOT NULL,
            DHO_Depart VARCHAR NOT NULL,
            DHO_Arrivee VARCHAR NOT NULL);
        """.format(table_name='cfl.public.train_jalon'),
    dag=dag,
)

create_train_lot_table = PostgresOperator(
    task_id="create_train_lot_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDTRAIN_LOT VARCHAR NOT NULL,
            IDTRAIN VARCHAR NOT NULL,
            Lot_num VARCHAR NOT NULL,
            DHR_Arrivee VARCHAR NOT NULL,
            DHT_Arrivee VARCHAR NOT NULL,
            Nom_lot VARCHAR NOT NULL,
            Commentaire VARCHAR NOT NULL,
            Distance VARCHAR NOT NULL,
            Poids_CO2 VARCHAR NOT NULL,
            IDSOCIETE_Expediteur VARCHAR NOT NULL,
            IDSOCIETE_Destinataire VARCHAR NOT NULL,
            IDJALON_Origine VARCHAR NOT NULL,
            IDJALON_Destination VARCHAR NOT NULL,
            Surbooking VARCHAR NOT NULL,
            Objectif_CA VARCHAR NOT NULL,
            Objectif_TEU VARCHAR NOT NULL,
            DHT_Depart VARCHAR NOT NULL,
            DHR_Depart VARCHAR NOT NULL,
            H_Depart_ecart VARCHAR NOT NULL,
            H_Arrivee_ecart VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            Ref_Expediteur VARCHAR NOT NULL,
            Ref_Destinataire VARCHAR NOT NULL,
            Train_Position VARCHAR NOT NULL,
            Incoterm VARCHAR NOT NULL,
            Num_LDV VARCHAR NOT NULL,
            Objectif_T VARCHAR NOT NULL,
            Objectif_UTI VARCHAR NOT NULL,
            Nbre_TEU VARCHAR NOT NULL,
            Capacite_TEU VARCHAR NOT NULL,
            Nbre_UTI VARCHAR NOT NULL,
            Poids_total VARCHAR NOT NULL,
            Status VARCHAR NOT NULL,
            Longueur_totale VARCHAR NOT NULL,
            Voie_categorie VARCHAR NOT NULL,
            Voie_vitesse VARCHAR NOT NULL,
            Motif_suppression VARCHAR NOT NULL,
            DateH_suppression VARCHAR NOT NULL,
            IDSOCIETE_Client VARCHAR NOT NULL,
            DateH_info_fournisseur VARCHAR NOT NULL,
            Responsable_annulation VARCHAR NOT NULL,
            Top_reservation_interdite VARCHAR NOT NULL);
        """.format(table_name='cfl.public.train_lot'),
    dag=dag,
)

create_train_position_table = PostgresOperator(
    task_id="create_train_position_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDTRAIN_POSITION VARCHAR NOT NULL,
            IDTRAIN VARCHAR NOT NULL, 
            DateH_position VARCHAR NOT NULL,
            Latitude VARCHAR NOT NULL, 
            Longitude VARCHAR NOT NULL);
        """.format(table_name='cfl.public.train_position'),
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
        """.format(table_name='cfl.public.train_wagon'),
    dag=dag,
)

create_train_table = PostgresOperator(
    task_id="create_train_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};  
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDTRAIN VARCHAR NOT NULL,
            Train_num VARCHAR NOT NULL,
            IDRELATION VARCHAR NOT NULL,
            Sens VARCHAR NOT NULL,
            IDCONTACT_Responsable VARCHAR NOT NULL,
            Motif_suppression VARCHAR NOT NULL,
            IDTRAINREF_JOUR VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            IDATE VARCHAR NOT NULL,
            Commentaire VARCHAR NOT NULL,
            Poids_maxi VARCHAR NOT NULL,
            Longueur_maxi VARCHAR NOT NULL,
            DateH_suppression VARCHAR NOT NULL,
            IDSOCIETE_suppression VARCHAR NOT NULL,
            IDTRAIN_Parent VARCHAR NOT NULL,
            Generer_Orfeus VARCHAR NOT NULL); 
        """.format(table_name='cfl.public.train'),
    dag=dag,
)

create_wagon_capacite_table = PostgresOperator(
    task_id="create_wagon_capacite_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDWAGON_CAPACITE VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            IDWAGON_MODELE VARCHAR NOT NULL,
            Code_pays VARCHAR NOT NULL,
            Capacite_C VARCHAR NOT NULL,
            Capacite_N VARCHAR NOT NULL,
            Capacite_P VARCHAR NOT NULL);
        """.format(table_name='cfl.public.wagon_capacite'),
    dag=dag,
)

create_wagon_position_table = PostgresOperator(
    task_id="create_wagon_position_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDWAGON_POSITION VARCHAR NOT NULL,
            IDWAGON VARCHAR NOT NULL,
            DateH_position VARCHAR NOT NULL,
            Latitude VARCHAR NOT NULL,
            Longitude VARCHAR NOT NULL,
            Application_GPS VARCHAR NOT NULL,
            Qualifiant VARCHAR NOT NULL,
            Etat_position VARCHAR NOT NULL);
        """.format(table_name='cfl.public.wagon_position'),
    dag=dag,
)

create_wagon_table = PostgresOperator(
    task_id="create_wagon_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDWAGON VARCHAR NOT NULL,
            Code_wagon VARCHAR NOT NULL,
            IDGARE_actuelle VARCHAR NOT NULL,
            Frein_regime VARCHAR NOT NULL,
            Tare VARCHAR NOT NULL,
            ID_Proprietaire VARCHAR NOT NULL,
            ID_Detenteur VARCHAR NOT NULL,
            ID_AyantDroit VARCHAR NOT NULL,
            ID_Maintenance VARCHAR NOT NULL,
            Serie VARCHAR NOT NULL,
            Aptitude_circulation VARCHAR NOT NULL,
            km_initial VARCHAR NOT NULL,
            Enr_actif VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            IDWAGON_MODELE VARCHAR NOT NULL,
            Date_mise_circulation VARCHAR NOT NULL,
            Voie_num VARCHAR NOT NULL,
            Circulation_statut VARCHAR NOT NULL,
            Etat_wagon VARCHAR NOT NULL,
            Commentaire VARCHAR NOT NULL,
            Capteur_num VARCHAR NOT NULL,
            Date_sortie_parc VARCHAR NOT NULL,
            Capacite VARCHAR NOT NULL,
            km_actuel VARCHAR NOT NULL,
            IDFOURNISSEUR_CONTRAT VARCHAR NOT NULL,
            Date_restitution VARCHAR NOT NULL,
            Wagon_client VARCHAR NOT NULL,
            Application_GPS VARCHAR NOT NULL,
            Top_GPS_HS VARCHAR NOT NULL,
            GPS_performance VARCHAR NOT NULL);
        """.format(table_name='cfl.public.wagon'),
    dag=dag,
)

create_wagon_modele_table = PostgresOperator(
    task_id="create_wagon_modelle_table",
    postgres_conn_id='postgres_default',
    sql="""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            IDWAGON_MODELE VARCHAR NOT NULL,
            Modele_wagon VARCHAR NOT NULL,
            Caracteristiques VARCHAR NOT NULL,
            Type_wagon VARCHAR NOT NULL,
            Lettres_categories VARCHAR NOT NULL,
            Enr_actif VARCHAR NOT NULL,
            IDUtilisateur_creation VARCHAR NOT NULL,
            DateH_creation VARCHAR NOT NULL,
            IDUtilisateur_maj VARCHAR NOT NULL,
            DateH_maj VARCHAR NOT NULL,
            Frein_regime VARCHAR NOT NULL,
            Vitesse_maxi VARCHAR NOT NULL,
            Longueur_HT VARCHAR NOT NULL,
            Longueur_HTp VARCHAR NOT NULL,
            Nb_Plateaux VARCHAR NOT NULL,
            Longueur_utile VARCHAR NOT NULL,
            Hauteur_chargt VARCHAR NOT NULL,
            Charge_utile VARCHAR NOT NULL,
            Charge_TEU VARCHAR NOT NULL,
            Charge_essieu VARCHAR NOT NULL,
            Roue_diam VARCHAR NOT NULL,
            Courbure_rayon VARCHAR NOT NULL,
            Plancher_hauteur_rail VARCHAR NOT NULL,
            Poche_longueur VARCHAR NOT NULL,
            Nb_essieux VARCHAR NOT NULL,
            Charge_AS VARCHAR NOT NULL,
            Charge_ASS VARCHAR NOT NULL,
            Charge_BS VARCHAR NOT NULL,
            Charge_BSS VARCHAR NOT NULL,
            Charge_CS VARCHAR NOT NULL,
            Charge_CSS VARCHAR NOT NULL,
            Charge_DS VARCHAR NOT NULL,
            Charge_DSS VARCHAR NOT NULL,
            Nb_Positions_maxi VARCHAR NOT NULL,
            IDNHM_vide VARCHAR NOT NULL,
            IDNHM_Charge VARCHAR NOT NULL,
            Tare VARCHAR NOT NULL,
            Capacite_Pe VARCHAR NOT NULL,
            Capacite_Pf VARCHAR NOT NULL,
            Capacite_Pg VARCHAR NOT NULL,
            Chargement_mode VARCHAR NOT NULL,
            CentreCout VARCHAR NOT NULL,
            Regroupement_analytique_wagon VARCHAR NOT NULL);
        """.format(table_name='cfl.public.wagon_modele'),
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
    sql="""
            SELECT
                schema_name,
                relname,
                pg_size_pretty(table_size) AS size,
                table_size
            FROM (
                SELECT
                    pg_catalog.pg_namespace.nspname AS schema_name,
                    relname,
                    pg_relation_size(pg_catalog.pg_class.oid) AS table_size
                FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
                ) t
            WHERE schema_name = 'public'
            AND relname NOT LIKE '%_pkey'
            AND relname NOT LIKE '%_seq'
            ORDER BY table_size DESC;
        """,
    postgres_conn_id='postgres_default',
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
