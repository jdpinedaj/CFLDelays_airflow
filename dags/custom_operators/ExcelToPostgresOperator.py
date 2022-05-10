from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import os

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
LOCATION_DATA = '/dags/data/'


class ExcelToPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 file_name=None,
                 target_table=None,
                 identifier=None,
                 postgres_conn_id='postgres_default',
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        self.file_name = file_name
        self.target_table = target_table
        self.identifier = identifier
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):

        file_path = f"{AIRFLOW_HOME}{LOCATION_DATA}{self.file_name}"

        source = pd.read_excel(file_path)
        target = PostgresHook(self.postgres_conn_id)

        target_fields = source.columns.tolist()
        rows = source.values.tolist()

        target.insert_rows(self.target_table,
                           rows,
                           target_fields=target_fields,
                           replace_index=self.identifier,
                           replace=True)

        print("Data loaded successfully!")