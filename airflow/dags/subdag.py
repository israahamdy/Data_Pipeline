import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import LoadDimensionOperator
import sql
from helpers import SqlQueries



def LoadDimension_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='users',
    SQL_statement = SqlQueries.user_table_insert,
    append_only=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='songs',
        SQL_statement = SqlQueries.song_table_insert,
        append_only=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='artists',
        SQL_statement = SqlQueries.artist_table_insert,
        append_only=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='time',
        SQL_statement = SqlQueries.time_table_insert,
        append_only=True
    )
    

    return dag
