from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from io import StringIO

class LoadAppleIndexOperator(BaseOperator):
    ui_color = '#2196f3'

    @apply_defaults
    def __init__(self,
                 postgres_connection_id,
                 s3_connection_id,
                 s3_bucket,
                 key,
                 table,
                 create_sql,
                 insert_sql,
                 drop_sql,
                 *args, **kwargs):
        super(LoadAppleIndexOperator, self).__init__(*args, **kwargs)
        self.postgres_connection_id = postgres_connection_id
        self.s3_connection_id = s3_connection_id
        self.s3_bucket = s3_bucket
        self.key = key
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.drop_sql = drop_sql
        self.table = table

    def execute(self, context):
        
        postgres = PostgresHook(self.postgres_connection_id)
        
        # please run this part when postgres doesn't have the table
        self.log.info(f"Init THE {self.table} TABLE")
        postgres.run(self.drop_sql.format(self.table))
        postgres.run(self.create_sql)

        self.log.info('read s3 key')
        # read a file from s3
        s3_hook = S3Hook(self.s3_connection_id)
        file_content = StringIO(s3_hook.read_key(
            key = self.key,
            bucket_name = self.s3_bucket
        ))

        self.log.info('process')
        # convert file content to pandas dataframe
        df_apple_index = pd.read_csv(file_content)

        # filtering data with region
        df_apple_index = df_apple_index.loc[df_apple_index['region']=='Seoul']

        # remove unnecessary columns
        df_apple_index = df_apple_index.drop(['geo_type','alternative_name', 'sub-region', 'country'], axis = 1)
        
        # stack dataframe to fit to the postgresql table
        df_apple_index = df_apple_index.set_index(['region','transportation_type']).stack().reset_index().rename(columns = {'level_2':'date', 0: 'index'})
        df_apple_index['date'] = pd.to_datetime(df_apple_index['date'])
        self.log.info(df_apple_index.columns)

        self.log.info(f"INSERT DATA")
        for i, row in df_apple_index.iterrows():
            # extract a date
            date = row['date'].date()
            # format a sql query
            query = self.insert_sql % (row['region'], row['transportation_type'] , f'{date.year}-{date.month}-{date.day}', row['index'])
            postgres.run(query)
