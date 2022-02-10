from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

# for data processing
import numpy as np
import pandas as pd
import re

# communicate with api
import requests
import json
from urllib.parse import urlencode,quote_plus
import xmltodict

class LoadVaccinationOperator(BaseOperator):

    ui_color = '#F98866'
    url_template = "{}?serviceKey={}&{}&perPage=100"
                
    @apply_defaults
    def __init__(self,
                 table,
                 connection_id,
                 create_sql,
                 insert_sql,
                 drop_sql,
                 url,
                 key,   
                 *args, **kwargs):

        super(LoadVaccinationOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.connection_id = connection_id
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.drop_sql = drop_sql
        self.url = url
        self.key = key

    def execute(self, context):
        postgres = PostgresHook(self.connection_id)
        self.log.info(f"Init THE {self.table} TABLE")
        postgres.run(self.drop_sql.format(self.table))
        postgres.run(self.create_sql)

        self.log.info(f"GET DATA FROM URL")
        # format a url 
        sample_url = LoadVaccinationOperator.url_template.format(self.url,self.key,1)
        # calculate how many pages we have to request
        response = requests.get(sample_url)
        response = json.loads(response.text)
        total_rows = response['totalCount']
        required_page_num = (total_rows // 100) + 1

        # get the vaccination data & the response as a pandas dataframe
        list_result = []
        for i in range(1, required_page_num + 1):
            response = requests.get(LoadVaccinationOperator.url_template.format(self.url,self.key,i))
            df_response = pd.DataFrame(json.loads(response.text)['data'])
            list_result.append(df_response)
        df_vaccination = pd.concat(list_result, ignore_index = True)
        # convert data type
        df_vaccination['baseDate'] = pd.to_datetime(df_vaccination['baseDate'])

        # filter only required columns
        df_vaccination = df_vaccination.loc[:, ['baseDate',
                                                'sido',
                                                'totalFirstCnt',
                                                'totalSecondCnt',
                                                'totalThirdCnt']]

        self.log.info(f"INSERT DATA")
        for i, row in df_vaccination.iterrows():
            date = row['baseDate'].date()
            sido = row['sido']
            formatted_sql = self.insert_sql % (f'{date.year}-{date.month}-{date.day}',
                                f'{sido}',
                                row['totalFirstCnt'],
                                row['totalSecondCnt'],
                                row['totalThirdCnt'])
            postgres.run(formatted_sql)