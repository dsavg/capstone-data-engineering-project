"""
Dag to stage reddit data sources to Redshift with Airflow.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, S3PartitionCheck,
                       RedshiftOperator)

# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

BUCKET_NAME = "reddit-project-data"
aws_creds = "aws_default"
schema_name = 'reddit'

default_args = {
    'owner':'danai',
    'depends_on_past':False,
    'start_date':datetime(2022, 12, 1, 0, 0, 0, 0),
    'retries':3,
    'max_active_runs':1,
    'retry_delay':timedelta(minutes=5),
    'catchup':True,
    'email_on_failure':False
}


dag = DAG('reddit_stagging',
          default_args=default_args,
          description='Stage data sources to Redshift.',
          schedule_interval='0 15 * * *'  # daily at 7:05 am PST
          )


start_operator = DummyOperator(
    task_id='begin_execution',
    dag=dag
)

# Check if date partition exists in S3 bubcket
reddit_data_sensor = S3PartitionCheck(
    task_id='reddit_s3_data_sensor',
    dag=dag,
    aws_credentials_id=aws_creds,
    s3_bucket=BUCKET_NAME,
    s3_key="reddit-data/date={}/",
    params={
        'end_date': '{{ ds }}'
    }
)

# Create schema if not exists
create_schema = RedshiftOperator(
    task_id='create_reddit_schema',
    dag=dag,
    sql='resources/create_schema.sql',
    params={
        "schema_name":schema_name
    },
    postgres_conn_id="redshift",
    autocommit=True
)

# Define stagging reddit table
create_stagging_table = RedshiftOperator(
    task_id='create_stagging_table',
    dag=dag,
    sql='resources/create_staging_table.sql',
    params={
        "schema_name":schema_name
    },
    postgres_conn_id="redshift",
    autocommit=True
)

# Stage parquet data
stage_reddit_data = StageToRedshiftOperator(
    task_id='stage_reddit_data',
    dag=dag,
    redshift_conn_id="redshift",
    schema='reddit',
    table='staging_reddit_logs',
    aws_credentials_id="aws_credentials",
    s3_bucket="reddit-project-data",
    s3_key="reddit-data/",
    file_type='parquet',
    dt='{{ ds }}'
)

# Load data to reddit.reddit_logs table
load_reddit_data = RedshiftOperator(
    task_id='load_reddit_data',
    dag=dag,
    sql='resources/reddit_logs.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

# Define stagging dates table
create_stagging_dwr_dates_table = RedshiftOperator(
    task_id='create_stagging_dwr_dates_table',
    dag=dag,
    sql='resources/create_staging_dwr_dates_table.sql',
    params={
        "schema_name":schema_name
    },
    postgres_conn_id="redshift",
    autocommit=True
)

# Stage csv data
stage_dwr_dates_data = StageToRedshiftOperator(
    task_id='stage_dwr_dates',
    dag=dag,
    redshift_conn_id="redshift",
    schema='reddit',
    table='staging_dwr_dates',
    aws_credentials_id="aws_credentials",
    s3_bucket="reddit-project-data",
    s3_key="dwr/dates.csv",
    file_type='csv'
)

# Load data to reddit.dwr_dates table
load_dwr_date = RedshiftOperator(
    task_id='load_dwr_date',
    dag=dag,
    sql='resources/dwr_dates_table.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

end_operator = DummyOperator(
    task_id='end_execution',
    dag=dag
)

# Build DAG graph
start_operator >> \
create_schema >> \
reddit_data_sensor \

reddit_data_sensor >> \
create_stagging_table >> \
stage_reddit_data >> \
load_reddit_data

reddit_data_sensor >> \
create_stagging_dwr_dates_table >> \
stage_dwr_dates_data >> \
load_dwr_date

[load_reddit_data,
 load_dwr_date]>> end_operator
