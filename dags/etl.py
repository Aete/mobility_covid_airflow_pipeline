from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (LoadCSVOperator, LoadAPIOperator)

default_args = {
    'start_date': datetime(2022, 2, 6),
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

load_apple_index = LoadCSVOperator(
  task_id = 'load_apple_mobility',
  table = 'apple_mobility_index',
  connection_id = 'postgres_dend' , 
  dag = dag)

load_seoul_living_migration = LoadCSVOperator(
  task_id = 'load_seoul_living_migration',
  table = 'seoul_living_migration',
  connection_id = 'postgres_dend' ,
  dag = dag
)

load_covid_cases = LoadAPIOperator(
  task_id = 'load_covid19_cases',
  table = 'covid_daily_cases',
  connection_id = 'postgres_dend' , 
  dag = dag
)

start_operator >> load_apple_index
start_operator >> load_seoul_living_migration
start_operator >> load_covid_cases

load_seoul_living_migration >> end_operator
load_apple_index >> end_operator 
load_covid_cases >> end_operator 