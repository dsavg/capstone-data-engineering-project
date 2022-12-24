"""Operator to check if date partition exists in an S3 path.

The class inherits the BaseOperator.
"""
import boto3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class S3PartitionCheck(BaseOperator):
    """Airflow operator to check if date partition exists in a S3 path."""

    ui_color = '#DA5984'
    template_fields = ("ds",)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 *args, **kwargs) -> None:
        """
        Class initialization.

        :param aws_credentials_id: AWS credentials ID read from Airflow (str)
        :param s3_bucket: S3 Bucket name (str)
        :param s3_key: S3 key/folder (str)
        """
        super().__init__(*args, **kwargs)
        # aws arguments
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key.split('/')[0]
        self.ds = kwargs['params']['end_date']
        self.input_file_path = s3_key


    def execute(self, context) -> None:
        """Check if a specific date partition exists in S3 path."""
        # connect to AWS and S3
        self.log.info("Connect to AWS")
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        s3 = boto3.resource('s3',
                            region_name='us-west-2',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key
                            )
        s3Bucket = s3.Bucket(self.s3_bucket)
        input_file_path = self.input_file_path.format(self.ds)
        self.log.info(input_file_path)
        # loop through S3 path and check if target path exists
        found = False
        for object_summary in s3Bucket.objects.filter(Prefix=f"{self.s3_key}/"):
            if input_file_path in object_summary.key:
                self.log.info(f"Bucket Found - s3://{self.s3_bucket}/{input_file_path}")
                found = True
                break
        # raise error if bucket is not found
        if not found:
            raise FileNotFoundError(f"Bucket Not Found - s3://{self.s3_bucket}/{input_file_path}")
