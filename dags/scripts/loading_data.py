# import pandas as pd
# import psycopg2
# from sqlalchemy import create_engine, text
# import requests

from airflow.providers.postgres.hooks.postgres import PostgresHook

# # Create the connection
# postgres_str = "postgresql://{username}:{password}@{host}:{port}/{dbname}".format(
#     username={'username'},
#     password={'password'},
#     host={'host'},
#     port={'port'},
#     dbname={'dbname'},
# )

# cnx = create_engine(postgres_str, echo=True)

# def load_data(sql_df):
#     result = cnx.connect().execute(text(sql_df))
#     return pd.DataFrame(result.fetchall(), columns=result.keys())

# sql_df = """
#     SELECT *
#     FROM {0}.{1}.{2}
#     """.format({'dbname'}, {'schema_name'}, {'table_name'})


def load_data(postgres_conn_id, dbname, schema_name, table_name, **kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    df = hook.get_pandas_df(sql="""
    SELECT *
    FROM {0}.{1}.{2}
    """.format({'dbname'}, {'schema_name'}, {'table_name'}))
    return df