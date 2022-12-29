"""
Dag to request Reddit data from API and store them in S3 in JSON format.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import RedditΤoS3Operator
from helpers import SUBREDDIT_NAMES, SUBREDDIT_TYPES


# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'danai',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 1, 0, 0, 0, 0),
    'retries': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False
}

dag = DAG('reddit_get_api_data',
          default_args=default_args,
          description='Fetch top subreddits data from Reddit '
                      'API and load them to S3 with Airflow.',
          # schedule_interval='0 15 * * *'  # daily at 7:05 am PST
          schedule_interval='0 */1 * * *'  # daily at 7:05 am PST
          )

start_operator = DummyOperator(
    task_id='begin_execution',
    dag=dag
)

get_reddit_data = RedditΤoS3Operator(
    task_id='get_reddit_data',
    dag=dag,
    subreddit_names=SUBREDDIT_NAMES,
    subreddit_types=SUBREDDIT_TYPES,
    subreddit_limit=200,
    aws_credentials_id="aws_default",
    s3_bucket="reddit-project-data",
    s3_key="raw-json-logs"
)

end_operator = DummyOperator(
    task_id='end_execution',
    dag=dag
)

start_operator >> \
get_reddit_data >> \
end_operator
