from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadAPIOperator(BaseOperator):

    ui_color = '#F98866'
    
    create_sql = """
      CREATE TABLE IF NOT EXISTS {}
        ( 
          id SERIAL,
          test INT
        )
    """
    insert_sql = """
        INSERT INTO {}
        {}
        ;    
    """
    @apply_defaults
    def __init__(self,
                 table,
                 connection_id,   
                 *args, **kwargs):

        super(LoadAPIOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.connection_id = connection_id

    def execute(self, context):
        self.log.info(f"CREATE THE {self.table} TABLE IF NOT EXIST")
        postgres = PostgresHook(self.connection_id)
        formatted_sql = LoadCSVOperator.create_sql.format(self.table)
        postgres.run(formatted_sql)
