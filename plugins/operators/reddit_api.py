"""Operator to ping the Reddit API and store response data in JSON format on S3.

The class inherits the BaseOperator.
"""
import json
from datetime import datetime, timezone
from typing import List
import boto3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from helpers import SubredditAPI


class RedditΤoS3Operator(BaseOperator):
    """Airflow operator to fetch subreddit data and store in S3."""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 subreddit_names: List[str] = None,
                 subreddit_types: List[str] = None,
                 subreddit_limit: str = 50,
                 aws_credentials_id: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 *args, **kwargs) -> None:
        """
        Class initialization.

        :param subreddit_names: list of subreddit names
        :param subreddit_types: list of subreddit types
        :param subreddit_limit: subreddit limit of requested rows (int)
        :param aws_credentials_id: AWS credentials ID read from Airflow (str)
        :param s3_bucket: S3 Bucket name (str)
        :param s3_key: S3 key/folder (str)
        """
        super().__init__(*args, **kwargs)
        # reddit arguments
        self.subreddit_names = subreddit_names
        self.subreddit_types = subreddit_types
        self.subreddit_limit = subreddit_limit
        # aws arguments
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # get the current utc date for s3 bucket
        now = datetime.now(timezone.utc)
        self.current_date = str(now.date())
        self.hour = now.hour

    def execute(self, context) -> None:
        """Fetch Reddit data and store them in S3."""
        # use the SubredditAPI class to connect to Reddit API
        self.log.info("Reddit API Auth")
        reddit = SubredditAPI()
        # collect top trending food subreddit data
        self.log.info("Get Reddit data")

        result = list()
        for ind, subreddit_name in enumerate(self.subreddit_names):
            for subreddit_type in self.subreddit_types:
                self.log.info(f'Fetching subreddit_type="{subreddit_type}" '
                              f'subreddit_name="{subreddit_name}"')
                res = reddit.get(subreddit_name,
                                 subreddit_type,
                                 self.subreddit_limit)
                posts = res.json()['data']['children']
                result.extend(posts)
            self.log.info('-' * 25 + f'{round(((ind+1)*100)/len(self.subreddit_names), 2)}% completion' + '-' * 25)
        self.log.info(f"Response includes {len(result)} records")
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
        s3key = f'{self.s3_key}/reddit-data-{self.current_date}-{self.hour}.json'
        s3object = s3.Object(self.s3_bucket, s3key)
        self.log.info("Store Reddit data to AWS")
        s3object.put(
            Body=(bytes(json.dumps(result).encode('UTF-8')))
        )
