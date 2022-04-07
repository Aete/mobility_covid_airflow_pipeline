from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table,
                 postgres_conn_id = 'postgres_local',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id

        
    def execute(self, context):
        postgres = PostgresHook(self.postgres_conn_id)
        
        self.log.info(f"Start the Data quality check on the {self.table}")
        records = postgres.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        self.log.info("Data quality check on the dimension tables passed")
        