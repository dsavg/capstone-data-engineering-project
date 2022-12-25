"""
Dag to load and transform Reddit data in Redshift with Airflow.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, S3PartitionCheck,
                       RedshiftOperator, DataQualityOperator)

# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

BUCKET_NAME = "reddit-project-data"
aws_creds = "aws_default"
schema_name = 'reddit'

default_args = {
    'owner':'danai',
    'depends_on_past':False,
    'start_date':datetime(2022, 12, 23, 0, 0, 0, 0),
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'email_on_failure':False
}


dag = DAG('reddit_dwr',
          default_args=default_args,
          description='Load and transform Reddit data in Redshift with Airflow',
          schedule_interval='0 0 * * *'  # once an day
          )


start_operator = DummyOperator(
    task_id='begin_execution',
    dag=dag
)

reddit_data_sensor = S3PartitionCheck(
    task_id='reddit_data_sensor',
    dag=dag,
    aws_credentials_id=aws_creds,
    s3_bucket=BUCKET_NAME,
    s3_key="reddit-data/date= {}/",
    params={
        'end_date': '{{ ds }}'
    }
)

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

# Create stagging reddit tables
create_stagging_table = RedshiftOperator(
    task_id='create_stagging_table',
    dag=dag,
    sql='resources/create_stagging_table.sql',
    params={
        "schema_name":schema_name
    },
    postgres_conn_id="redshift",
    autocommit=True
)

stage_reddit_data = StageToRedshiftOperator(
    task_id='stage_reddit_data',
    dag=dag,
    redshift_conn_id="redshift",
    schema='reddit',
    table='staging_reddit_logs',
    aws_credentials_id="aws_credentials",
    s3_bucket="reddit-project-data",
    s3_key="reddit-data/",
    dt='{{ ds }}'
)

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

# Load Fact and Dimension tables
load_creator_snapshot_table = RedshiftOperator(
    task_id='load_creator_snapshot',
    dag=dag,
    sql='resources/creators_snapshot.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

load_creator_dimension_table = RedshiftOperator(
    task_id='load_creator_dimension',
    dag=dag,
    sql='resources/creators_dimension.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

load_posts_snapshot_table = RedshiftOperator(
    task_id='load_posts_snapshot',
    dag=dag,
    sql='resources/posts_snapshot.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

load_posts_dimension_table = RedshiftOperator(
    task_id='load_posts_dimension',
    dag=dag,
    sql='resources/posts_dimension.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

load_subreddit_snapshot_table = RedshiftOperator(
    task_id='load_subreddit_snapshot',
    dag=dag,
    sql='resources/subreddit_snapshot.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

load_subreddit_dimension_table = RedshiftOperator(
    task_id='load_subreddit_dimension',
    dag=dag,
    sql='resources/subreddit_dimension.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

dim_complete = DummyOperator(
    task_id='dim_tables_complete',
    dag=dag
)

load_fact_table = RedshiftOperator(
    task_id='load_fact_table',
    dag=dag,
    sql='resources/fact_table.sql',
    params={
        "schema_name":schema_name,
        "ds":'{{ ds }}'
    },
    postgres_conn_id="redshift",
    autocommit=True
)

# Data quality tests
creator_dimension_table_quality_checks = DataQualityOperator(
    task_id='creator_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_check_dict={
        f'SELECT COUNT(*) FROM {schema_name}.creators_d WHERE creator_id is Null':(0, '='),
        f'SELECT COUNT(*) FROM {schema_name}.creators_d':(0, '>'),
    }
)

posts_dimension_table_quality_checks = DataQualityOperator(
    task_id='posts_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_check_dict={
        f'SELECT COUNT(*) FROM {schema_name}.post_d WHERE post_id is Null':(0, '='),
        f'SELECT COUNT(*) FROM {schema_name}.post_d':(0, '>'),
    }
)

subreddit_dimension_table_quality_checks = DataQualityOperator(
    task_id='subreddit_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_check_dict={
        f'SELECT COUNT(*) FROM {schema_name}.subreddit_d WHERE subreddit_id is Null':(0, '='),
        f'SELECT COUNT(*) FROM {schema_name}.subreddit_d':(0, '>'),
    }
)

fact_table_quality_checks = DataQualityOperator(
    task_id='fact_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_check_dict={
        f"SELECT COUNT(*) FROM {schema_name}.reddit_fact "
        "WHERE (subreddit_id is Null or post_id is Null or creator_id is Null)":(0, '='),
        f"SELECT COUNT(*) FROM {schema_name}.reddit_fact":(0, '>'),
    }
)

end_operator = DummyOperator(
    task_id='end_execution',
    dag=dag
)

# Build DAG graph
start_operator >> \
create_schema >> \
reddit_data_sensor >> \
create_stagging_table >> \
stage_reddit_data >> \
load_reddit_data

load_reddit_data >> [load_creator_snapshot_table,
                     load_posts_snapshot_table,
                     load_subreddit_snapshot_table]

load_creator_snapshot_table >> load_creator_dimension_table >> creator_dimension_table_quality_checks

load_posts_snapshot_table >> load_posts_dimension_table >> posts_dimension_table_quality_checks

load_subreddit_snapshot_table >> load_subreddit_dimension_table >> subreddit_dimension_table_quality_checks

[creator_dimension_table_quality_checks,
 posts_dimension_table_quality_checks,
 subreddit_dimension_table_quality_checks] >> dim_complete

dim_complete >> load_fact_table >> fact_table_quality_checks >> end_operator
