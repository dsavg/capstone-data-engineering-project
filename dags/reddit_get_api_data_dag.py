"""Sparkfy Dag to populate fact and dimension tables and quality check them."""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import RedditΤoS3Operator


# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'danai',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 16, 0, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False
}

dag = DAG('reddit_get_api_data',
          default_args=default_args,
          description='Get top wordlnews subreddits, from Reddit API',
          schedule_interval='0 0 * * *'  # once an day
          )

start_operator = DummyOperator(task_id='begin_execution',
                               dag=dag)

get_reddit_data = RedditΤoS3Operator(task_id='get_reddit_data',
                                     dag=dag,
                                     subreddit_name='worldnews',
                                     subreddit_type='hot',
                                     subreddit_limit=50,
                                     aws_credentials_id="aws_credentials",
                                     s3_bucket="reddit-project-data",
                                     s3_key="raw-json-logs")

start_operator >> get_reddit_data
