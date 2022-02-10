from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (LoadCaseOperator, LoadVaccinationOperator)

from helpers import SqlQueries
from helpers import (key, case_url, vaccination_url)

default_args = {
    'start_date': datetime(2022, 2, 7),
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'email_on_retry': False,
    'catchup': False,
    'depends_on_past': False
}

dag = DAG('etl_pipeline',
          default_args=default_args,
          description='Load and transform data tables with Airflow',
          schedule_interval= '@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# load_apple_index = LoadCSVOperator(
#   task_id = 'load_apple_mobility',
#   table = 'apple_mobility_index',
#   connection_id = 'postgres_local' , 
#   dag = dag)

# load_seoul_living_migration = LoadCSVOperator(
#   task_id = 'load_seoul_living_migration',
#   table = 'seoul_living_migration',
#   connection_id = 'postgres_local' ,
#   dag = dag
# )

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

# start_operator >> load_apple_index
# start_operator >> load_seoul_living_migration
start_operator >> load_covid_cases
start_operator >> load_vaccination

# load_seoul_living_migration >> end_operator
# load_apple_index >> end_operator 
load_covid_cases >> end_operator 
load_vaccination >> end_operator