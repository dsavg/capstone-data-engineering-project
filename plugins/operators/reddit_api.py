"""
Operator to ping the Reddit API and store response data in JSON format.

Functionalities:
1. Create and run a SQL COPY statement based on the parameters provided.
2. Ability to distinguish between JSON files.
3. Allows to load timestamped files from S3 based on the execution time
    and run backfills.
"""

import json
import boto3
from datetime import datetime, timezone
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from helpers import SubredditAPI
from airflow.hooks.S3_hook import S3Hook


class RedditΤoS3Operator(BaseOperator):
    """

    """
    ui_color = '#89DA59'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 subreddit_name: str = 'worldnews',
                 subreddit_type: str = 'hot',
                 subreddit_limit: str = 50,
                 aws_credentials_id: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 *args, **kwargs) -> None:
        """

        :param subreddit_name:
        :param subreddit_type:
        :param subreddit_limit:
        :param aws_credentials_id:
        :param s3_bucket:
        :param s3_key:
        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)
        # reddit arguments
        self.subreddit_name = subreddit_name
        self.subreddit_type = subreddit_type
        self.subreddit_limit = subreddit_limit
        # aws arguments
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # get the current utc date for s3 bucket
        now = datetime.now(timezone.utc)
        self.current_date = str(now.date())

    def execute(self, context) -> None:
        """

        :param context:
        :return:
        """
        # use the SubredditAPI class to connect to Reddit API
        self.log.info("Reddit API Auth")
        reddit = SubredditAPI()
        # collect top trending world news data
        self.log.info("Get Reddit data")
        res = reddit.get(self.subreddit_name,
                         self.subreddit_type,
                         self.subreddit_limit)
        posts = res.json()['data']['children']
        # connect to AWS
        self.log.info("Connect to AWS")
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        s3 = boto3.resource('s3',
                            region_name='us-west-2',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key
                            )
        # write data to s3
        s3key = f'{self.s3_key}/reddit-{self.subreddit_name}-{self.subreddit_type}-{self.current_date}.json'
        s3object = s3.Object(self.s3_bucket, s3key)
        self.log.info("Store Reddit data to AWS")
        s3object.put(
            Body=(bytes(json.dumps(posts).encode('UTF-8')))
        )

        # hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        # keys = hook.list_keys(self.s3_bucket)
        # for key in keys:
        #     self.log.info(f"- s3://{self.s3_bucket}/{key}")
