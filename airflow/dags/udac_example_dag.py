from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableOperator) 
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('etl_task',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

### Create tables only if not exist
create_staging_events = CreateTableOperator(
    task_id='create_staging_events',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_events',
    sql_command=SqlQueries.create_staging_events
)

create_staging_songs = CreateTableOperator(
    task_id='create_staging_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    sql_command=SqlQueries.create_staging_songs
)

create_songplays = CreateTableOperator(
    task_id='create_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    sql_command=SqlQueries.create_songplays
)

create_artists = CreateTableOperator(
    task_id='create_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    sql_command=SqlQueries.create_artists
)

create_songs = CreateTableOperator(
    task_id='create_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    sql_command=SqlQueries.create_songs
)

create_time = CreateTableOperator(
    task_id='create_time',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    sql_command=SqlQueries.create_time
)

create_users = CreateTableOperator(
    task_id='create_users',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    sql_command=SqlQueries.create_users
) 

sync_operator_1 = DummyOperator(task_id='sync_operator_1',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id ="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    region="us-west-2",
    paramater="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id ="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    region="us-west-2",
    paramater="JSON 'auto' COMPUPDATE OFF"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    source=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    clear_table=True,
    source=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    clear_table=True,
    source=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    clear_table=True,
    source=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    clear_table=True,
    source=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 }
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

### Define dependencies for DAG
start_operator >> [create_staging_events, create_staging_songs, create_songplays, \
                    create_artists, create_songs, create_time, create_users] >> sync_operator_1

sync_operator_1 >>[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, \
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator