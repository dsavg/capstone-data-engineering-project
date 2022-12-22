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


class S3PartitionCheck(BaseOperator):
    """

    """
    ui_color = '#DA5984'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 subreddit_name: str = 'worldnews',
                 subreddit_type: str = 'hot',
                 aws_credentials_id: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 *args, **kwargs) -> None:
        """

        :param subreddit_name:
        :param subreddit_type:
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
        # aws arguments
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ds = kwargs['params']['end_date']


    def execute(self, context) -> None:
        """

        :param context:
        :return:
        """
        # connect to AWS
        self.log.info("Connect to AWS")
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        s3 = boto3.resource('s3',
                            region_name='us-west-2',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key
                            )

        s3Bucket = s3.Bucket(self.s3_bucket)
        input_file_path = f"{self.s3_key}/reddit-{self.subreddit_name}-{self.subreddit_type}-{self.ds}.json"

        found = False
        for object_summary in s3Bucket.objects.filter(Prefix=f"{self.s3_key}/"):
            if object_summary.key == input_file_path:
                self.log.info(f"Bucket Found - s3://{self.s3_bucket}/{input_file_path}")
                found = True

        if not found:
            raise ValueError(f"Bucket Not Found - s3://{self.s3_bucket}/{input_file_path}")
