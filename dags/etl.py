from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


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

start_operator >> end_operator