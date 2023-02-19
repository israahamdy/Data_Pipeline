from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

from airflow.operators.subdag_operator import SubDagOperator
from subdag import LoadDimension_dag

#start_date = datetime.utcnow()

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'Depends_on_past': False,
    'Retries': 3,
    'Retry_delay': timedelta(minutes=5),
    'Catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          #start_date= datetime(2019, 1, 12)
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song-data/A/A/A/',
    json='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='songplays',
    SQL_statement = SqlQueries.songplay_table_insert
)

Dims_task_id = "Load_Dimensions_subdag"
Dims_subdag_task = SubDagOperator(
    subdag=LoadDimension_dag(
        "udac_example_dag",
        Dims_task_id,
        "redshift",
        "aws_credentials",
    ),
    task_id=Dims_task_id,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table=['staging_events','staging_songs','songplays','artists','users','songs','time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> Dims_subdag_task

Dims_subdag_task >> run_quality_checks

run_quality_checks >> end_operator