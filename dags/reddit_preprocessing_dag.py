"""
Dag to pre-process Reddit data using PySpark, and store them in parquet format on S3.
"""

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
    'start_date': datetime(2022, 12, 23, 0, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False
}

dag = DAG('reddit_get_api_data',
          default_args=default_args,
          description='Fetch top wordlnews subreddits data from Reddit API and load them to S3 with Airflow.',
          schedule_interval='0 0 * * *'  # once an day
          )

start_operator = DummyOperator(task_id='begin_execution',
                               dag=dag)

get_reddit_data = RedditΤoS3Operator(task_id='get_reddit_data',
                                     dag=dag,
                                     subreddit_name='worldnews',
                                     subreddit_type='hot',
                                     subreddit_limit=50,
                                     aws_credentials_id="aws_default",
                                     s3_bucket="reddit-project-data",
                                     s3_key="raw-json-logs")

end_operator = DummyOperator(task_id='end_execution',
                               dag=dag)

start_operator >> get_reddit_data >> end_operator
