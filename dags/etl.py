from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (LoadCaseOperator, LoadVaccinationOperator, LoadAppleIndexOperator, LoadSeoulMigrationOperator, DataQualityOperator)
from airflow.hooks.S3_hook import S3Hook

from helpers import SqlQueries
from helpers import (key, case_url, vaccination_url, apple_bucket, apple_key)

default_args = {
    'start_date': datetime(2022, 4, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'email_on_retry': False,
    'catchup': False,
    'depends_on_past': False
}

# create a dag
dag = DAG('etl_pipeline',
          default_args=default_args,
          description='Load and transform data tables with Airflow',
          schedule_interval= '@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# data loading part
load_covid_cases = LoadCaseOperator(
  task_id = 'load_covid19_cases',
  table = 'covid_daily_cases',
  connection_id = 'postgres_local',
  create_sql = SqlQueries.covid_cases_create,
  insert_sql = SqlQueries.covid_cases_insert, 
  drop_sql = SqlQueries.drop_table,
  dag = dag,
  url = case_url,
  key = key
)

load_vaccination =  LoadVaccinationOperator(
  task_id = 'load_covid19_vaccination',
  table = 'covid_vaccination',
  connection_id = 'postgres_local',
  create_sql = SqlQueries.covid_vaccination_create,
  insert_sql = SqlQueries.covid_vaccination_insert, 
  drop_sql = SqlQueries.drop_table,
  dag = dag,
  url = vaccination_url,
  key = key
)

load_apple_index = LoadAppleIndexOperator(
    task_id = 'load_apple_index',
    dag = dag,
    table = 'apple_index',
    postgres_connection_id = 'postgres_local',
    s3_connection_id = 'aws_s3_connection',
    create_sql = SqlQueries.apple_index_create,
    insert_sql = SqlQueries.apple_index_insert, 
    drop_sql = SqlQueries.drop_table,
    s3_bucket = apple_bucket,
    key = apple_key
)

load_seoul_living_migration = LoadSeoulMigrationOperator(
    task_id = 'load_seoul_living_migration',
    dag = dag,
    postgres_connection_id = 'postgres_local',
    table = 'seoul_living_migration',
    s3_connection_id = 'aws_s3_connection',
    s3_bucket = apple_bucket,
    s3_prefix = 'seoul_living_migration/2022/02',
    create_sql = SqlQueries.seoul_living_migration_create,
    insert_sql = SqlQueries.seoul_living_migration_insert, 
    drop_sql = SqlQueries.drop_table,    
)

# data quality check part
data_quality_check_case = DataQualityOperator(
    task_id = 'data_check_case',
    dag = dag,
    table = 'covid_daily_cases',
    postgres_conn_id = 'postgres_local')

data_quality_check_apple = DataQualityOperator(
    task_id = 'data_check_apple',
    dag = dag,
    table = 'apple_index',
    postgres_conn_id = 'postgres_local')

data_quality_check_vaccination = DataQualityOperator(
    task_id = 'data_check_vaccination',
    dag = dag,
    table = 'covid_vaccination',
    postgres_conn_id = 'postgres_local')

data_quality_check_migration = DataQualityOperator(
    task_id = 'data_check_migration',
    dag = dag,
    table = 'seoul_living_migration',
    postgres_conn_id = 'postgres_local')

# set an order of the tasks
start_operator >> load_covid_cases
start_operator >> load_vaccination
start_operator >> load_apple_index
start_operator >> load_seoul_living_migration

load_covid_cases >> data_quality_check_case
load_vaccination >> data_quality_check_vaccination
load_apple_index >> data_quality_check_apple
load_seoul_living_migration >> data_quality_check_migration

data_quality_check_case >> end_operator
data_quality_check_vaccination >> end_operator
data_quality_check_apple >> end_operator
data_quality_check_migration >> end_operator
