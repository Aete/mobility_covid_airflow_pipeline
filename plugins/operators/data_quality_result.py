from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityResultOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 check_sql, 
                 postgres_conn_id = 'postgres_local',
                 *args, **kwargs):

        super(DataQualityResultOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.check_sql = check_sql

        
    def execute(self, context):
        postgres = PostgresHook(self.postgres_conn_id)        
        records = postgres.get_records(self.check_sql)
        self.log.info(records)