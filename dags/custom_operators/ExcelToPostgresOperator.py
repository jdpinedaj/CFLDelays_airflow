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
                 sheet_name=None,
                 postgres_conn_id='postgres_default',
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        self.file_name = file_name
        self.target_table = target_table
        self.identifier = identifier
        self.sheet_name = sheet_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):

        self._get_data()
        self._write_data()

        print("Data loaded from Excel!")

    def _get_data(self):

        _file_path = f"{AIRFLOW_HOME}{LOCATION_DATA}{self.file_name}"
        _sheet_name = self.sheet_name
        self._source = pd.read_excel(_file_path, sheet_name=_sheet_name)

    def _write_data(self):
        if self._source is None:
            raise Exception("There is no data")
        self._write_sql()

    def _write_sql(self):

        _target = PostgresHook(self.postgres_conn_id)

        _target_fields = self._source.columns.tolist()
        _rows = self._source.values.tolist()

        _target.insert_rows(self.target_table,
                            _rows,
                            target_fields=_target_fields,
                            replace_index=self.identifier,
                            replace=True)

        print("Data loaded to Postgres!")