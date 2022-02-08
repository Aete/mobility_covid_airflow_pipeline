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

class LoadCOVIDAPIOperator(BaseOperator):

    ui_color = '#F98866'
    url_template = "{}?serviceKey={}&"+urlencode({quote_plus('pageNo') : '1',
                                                    quote_plus('numOfRows') : '10',
                                                    quote_plus('startCreateDt'):'20200110',
                                                    quote_plus('endCreateDt'):'20211231'})
                
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

        super(LoadCOVIDAPIOperator, self).__init__(*args, **kwargs)
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
        target_url = LoadCOVIDAPIOperator.url_template.format(self.url,self.key)
        # request and get a response from url
        response = requests.get(target_url)

        # convert the response as a pandas dataframe
        df_response = pd.DataFrame(dict(xmltodict.parse(response.text))['response']['body']['items']['item'])

        self.log.info(f"PROCESS DATA")
        # eliminate the hour, minute, second 
        df_response['stdDay'] = df_response['stdDay'].replace(r'[^0-9]', '',regex = True).str[:-2]
        df_response['stdDay'] = pd.to_datetime(df_response['stdDay'])

        # filter the data by the region
        df_total = df_response.loc[df_response['gubun']=='합계'].loc[:,['stdDay','localOccCnt']].rename(columns = {'localOccCnt': 'total'})
        df_seoul = df_response.loc[df_response['gubun']=='서울'].loc[:,['stdDay','localOccCnt']].rename(columns = {'localOccCnt': 'seoul'})
        df_gyeonggi = df_response.loc[df_response['gubun']=='경기'].loc[:,['stdDay','localOccCnt']].rename(columns = {'localOccCnt': 'gyeonggi'})
        df_incheon = df_response.loc[df_response['gubun']=='인천'].loc[:,['stdDay','localOccCnt']].rename(columns = {'localOccCnt': 'incheon'})

        # merge the dataframes
        df_covid = df_total.merge(df_seoul, on = 'stdDay').merge(df_gyeonggi, on = 'stdDay').merge(df_incheon, on = 'stdDay')
        df_covid = df_covid.rename(columns = {'stdDay':'date'})

        self.log.info(f"INSERT DATA")
        for i, row in df_covid.iterrows():
            # extract a date
            date = row['date'].date()
            # format a sql query
            query = self.insert_sql % (f'{date.year}-{date.month}-{date.day}', row['total'], row['seoul'], row['gyeonggi'], row['incheon'])
            postgres.run(query)