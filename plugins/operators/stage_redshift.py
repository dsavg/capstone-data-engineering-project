"""Operator to load any parquet formatted files from S3 to Amazon Redshift.

Functionalities:
1. Create and run a SQL COPY statement based on the parameters provided.
2. Ability to distinguish between JSON files.
3. Allows to load timestamped files from S3 based on the execution time
    and run backfills.
"""

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Airflow operator to load JSON formatted files from S3 to Redshift."""

    ui_color = '#BC45C4'
    template_fields = ("dt",)
    copy_sql = """
    COPY {}.{}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 schema: str = "",
                 table: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 file_type:str = "parquet",
                 dt: str = "",
                 *args, **kwargs) -> None:
        """
        Airflow operator to load JSON formatted files from S3 to Redshift.
        :param redshift_conn_id: (Optional) Amazon Redshift connection id
        :param aws_credentials_id: (Optional) AWS credentials id
        :param schema: (Optional) Schema name
        :param table: (Optional) Table name
        :param s3_bucket: (Optional) S3 bucket name
        :param s3_key: (Optional) S3 bucket key
        :param file_type: (Optional) File type, currently supporting `parquet` and `csv`
        :param dt: (Optional) partition date
        """
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.dt = dt
        self.sql = None

    def execute(self, context) -> None:
        """Copy data from s3 to Amazon Redshift."""
        aws_hook = AwsHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # # delete data if exists
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.schema}.{self.table}")
        # render s3 path based on inputs
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        # file type modifications
        if self.file_type == 'parquet':
            s3_path = f"s3://{self.s3_bucket}/{rendered_key}date={self.dt}"
            self.sql = StageToRedshiftOperator.copy_sql+"FORMAT AS PARQUET;"
        elif self.file_type == 'csv':
            s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            self.sql = StageToRedshiftOperator.copy_sql + \
                       "TIMEFORMAT 'auto'\n" +\
                       "IGNOREHEADER 1\n" + "CSV;"
        else:
            raise NameError(f'Unsupport file type "{self.file_type}"')
        # format copy statement from s3 to Amazon Redshift
        formatted_sql = self.sql.format(
            self.schema,
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(formatted_sql)
