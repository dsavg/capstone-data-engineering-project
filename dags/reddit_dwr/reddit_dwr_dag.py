"""
Dag to create the Reddit data model in Redshift with Airflow.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (RedshiftOperator, DataQualityOperator,
                       RedshiftTableCheck)

# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

BUCKET_NAME = "reddit-project-data"
aws_creds = "aws_default"
schema_name = 'reddit'

default_args = {
    'owner':'danai',
    'depends_on_past':False,
    'start_date': datetime(2022, 11, 30, 0, 0, 0, 0),
    'retries':3,
    'max_active_runs': 1,
    'retry_delay':timedelta(minutes=5),
    'catchup':True,
    'email_on_failure':False
}


dag = DAG('reddit_dwr',
          default_args=default_args,
          description='Data model for Reddit '
                      'data in Redshift with Airflow',
          schedule_interval='0 15 * * *'  # daily at 7:05 am PST
          )


start_operator = DummyOperator(
    task_id='begin_execution',
    dag=dag
)

# Check if reddit.reddit_logs has data
reddit_data_sensor = RedshiftTableCheck(
    task_id='reddit_data_sensor',
    dag=dag,
    redshift_conn_id='redshift',
    schema='reddit',
    table='reddit_logs',
    dt_field='snapshot_date',
    params={
        'end_date': '{{ ds }}'
    }
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
creators_snapshot_table_quality_checks = DataQualityOperator(
    task_id='creator_snapshot_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': "SELECT COUNT(*) "
                     "FROM {{ params.schema_name }}.creators_snapshot "
                     "WHERE snapshot_date = '{{ ds }}'",
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

posts_snapshot_table_quality_checks = DataQualityOperator(
    task_id='posts_snapshot_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': "SELECT COUNT(*) "
                     "FROM {{ params.schema_name }}.posts_snapshot "
                     "WHERE snapshot_date = '{{ ds }}'",
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

subreddits_snapshot_table_quality_checks = DataQualityOperator(
    task_id='subreddits_snapshot_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': "SELECT COUNT(*) "
                     "FROM {{ params.schema_name }}.subreddits_snapshot "
                     "WHERE snapshot_date = '{{ ds }}'",
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

creator_dimension_table_quality_checks = DataQualityOperator(
    task_id='creator_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.creators_d '
                     'WHERE creator_id is Null',
            'operation':'=',
            'value':0
        },
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.creators_d',
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

posts_dimension_table_quality_checks = DataQualityOperator(
    task_id='posts_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.posts_d '
                     'WHERE post_id is Null',
            'operation':'=',
            'value':0
        },
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.posts_d',
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

subreddit_dimension_table_quality_checks = DataQualityOperator(
    task_id='subreddit_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.subreddits_d '
                     'WHERE subreddit_id is Null',
            'operation':'=',
            'value':0
        },
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.subreddits_d',
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

fact_table_quality_checks = DataQualityOperator(
    task_id='fact_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_checks=
    [
        {
            'query': 'SELECT COUNT(*) '
                     'FROM {{ params.schema_name }}.reddit_fact '
                     'WHERE (subreddit_id is Null OR '
                     'post_id is Null OR '
                     'creator_id is Null)',
            'operation':'=',
            'value':0
        },
        {
            'query': "SELECT COUNT(*) "
                     "FROM {{ params.schema_name }}.reddit_fact "
                     "WHERE dt = '{{ ds }}'",
            'operation':'>',
            'value':0
        }
    ],
    params={
        "schema_name":schema_name,
    },
)

end_operator = DummyOperator(
    task_id='end_execution',
    dag=dag
)

# Build DAG graph
start_operator >> \
reddit_data_sensor \

reddit_data_sensor>> [load_creator_snapshot_table,
                     load_posts_snapshot_table,
                     load_subreddit_snapshot_table]

load_creator_snapshot_table >> \
creators_snapshot_table_quality_checks >> \
load_creator_dimension_table >> \
creator_dimension_table_quality_checks

load_posts_snapshot_table >> \
posts_snapshot_table_quality_checks >> \
load_posts_dimension_table >> \
posts_dimension_table_quality_checks

load_subreddit_snapshot_table >> \
subreddits_snapshot_table_quality_checks >> \
load_subreddit_dimension_table >> \
subreddit_dimension_table_quality_checks

[creator_dimension_table_quality_checks,
 posts_dimension_table_quality_checks,
 subreddit_dimension_table_quality_checks] >> load_fact_table

load_fact_table >> \
fact_table_quality_checks >> \
end_operator
