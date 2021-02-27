from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import create_tables


default_args = {
    'owner': 'dan',
    'depends_on_past':True,
    'start_date': datetime(2021, 2, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default':False,
    'schedule_interval':timedelta(days=30)
}

dag = DAG('udac_example_dag3',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    create_table=create_tables.staging_events_table_create,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    #s3_key='log_data/2018/11/2018-11-12-events.json',
    s3_key='log_data',
    json_path = 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    create_table=create_tables.staging_songs_table_create,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    #s3_key='song_data/A/A/A/TRAAAAK128F9318786.json',
    s3_key='song_data',
    json_path = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="songplays",
    sql=SqlQueries.songplay_table_insert.format("songplays")
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="users",
    append_data=False,
    sql=SqlQueries.user_table_insert.format("users")
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="songs",
    append_data=False,
    sql=SqlQueries.song_table_insert.format("songs")
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="artists",
    append_data=False,
    sql=SqlQueries.artist_table_insert.format("artists")
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="time",
    append_data=False,
    sql=SqlQueries.time_table_insert.format("time")
    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays','artists','songs','users','time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift 

stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table 
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_artist_dimension_table 
load_songplays_table >> load_time_dimension_table 

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
 
run_quality_checks >> end_operator
