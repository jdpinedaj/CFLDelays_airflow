from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import os

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
LOCATION_DATA = '/dags/data/'
file_name = 'TRAIN_WAGON_100rows.xlsx'
file_path = f"{AIRFLOW_HOME}{LOCATION_DATA}{file_name}"


class ExcelToPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 sql=None,
                 target_table=None,
                 identifier=None,
                 postgres_conn_id='postgres_default',
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.sql = sql
        self.target_table = target_table
        self.identifier = identifier
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):

        start_date = context['data_interval_start'].strftime(
            '%Y-%m-%d %H:%M:%S')
        end_date = context['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')

        self.sql = self.sql.format(start_date=start_date, end_date=end_date)
        print("sql", self.sql)

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