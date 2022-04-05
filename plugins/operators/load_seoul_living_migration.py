from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import numpy as np
import psycopg2
from io import StringIO

class LoadSeoulMigration(BaseOperator):
    ui_color = '#2196f3'

    dow = {
        '월' : 'mon',
        '화' : 'tue',
        '수' : 'wed',
        '목' : 'thur',
        '금' : 'fri',
        '토' : 'sat',
        '일' : 'sun'
    }

    columns = {
          '대상연월':'base_month', 
          '요일': 'dayofweek',
          '도착시간':'desti_region_hour',
          '출발 행정동 코드':'origin_region_code',
          '도착 행정동 코드':'desti_region_code',
          '성별': 'sex',
          '나이': 'age_group',
          '이동유형': 'category',
          '평균 이동 시간(분)':'duration',
          '이동인구(합)': 'population'  
    }

    @apply_defaults
    def __init__(self,
                 postgres_connection_id,
                 s3_connection_id,
                 s3_bucket,
                 s3_prefix,
                 table,
                 create_sql,
                 insert_sql,
                 drop_sql,
                 *args, **kwargs):
        super(LoadSeoulMigration, self).__init__(*args, **kwargs)
        self.postgres_connection_id = postgres_connection_id
        self.s3_connection_id = s3_connection_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.drop_sql = drop_sql
        self.table = table

    def execute(self, context):
        postgres = PostgresHook(self.postgres_connection_id)
        self.log.info(f"Init THE {self.table} TABLE")
        postgres.run(self.drop_sql.format(self.table))
        postgres.run(self.create_sql)

        self.log.info('get a csv file list from s3')
        # read a file list from s3
        s3_hook = S3Hook(self.s3_connection_id)
        file_list = s3_hook.list_keys(
            prefix = self.s3_prefix,
            bucket_name = self.s3_bucket
        )
        file_list = [f for f in file_list if f.endswith('csv')]

        self.log.info(file_list)
        self.log.info('DATA PROCESSING & INSERTING')

        for f in file_list[:1]:
            file_content = StringIO(s3_hook.get_key(
                key = file_list[0],
                bucket_name = self.s3_bucket
            ).get()['Body'].read().decode('cp949'))
            df_migration = self.process_data(file_content)

            for i, row in df_migration.iloc[:100, :].iterrows():
                # format a sql query
                query = self.insert_sql % (row['base_month'],
                                           row['dayofweek'],
                                           row['desti_region_hour'],
                                           row['origin_region_code'],
                                           row['desti_region_code'],
                                           row['sex'],
                                           row['age_group'],
                                           row['category'],
                                           row['duration'],
                                           row['population'])
                postgres.run(query)
            self.log.info(f)
        
        self.log.info('DATA PROCESSING & INSERTING END')

    def process_data(self, content):
        # convert file content to pandas dataframe and translate column names to English
        df_migration = pd.read_csv(content, encoding = 'cp949').rename(columns=self.columns)

        df_migration['dayofweek'] = df_migration['dayofweek'].map(self.dow)

        # replace '*' to 'NULL' for postgres
        df_migration['population'] = df_migration['population'].replace('*', psycopg2.extensions.AsIs('NULL'))

        return df_migration
