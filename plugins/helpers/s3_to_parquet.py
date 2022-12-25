"""Read data from S3, transform in PySpark on EMR, and store them back in S3.

The code is copied to S3 and executed from there on EMR, with the following
responsibilities,
- Create a SparkSession
- Read data from S3, in JSON format
- Unnest columns specified in all_fields
- Add the date field, indicating the date of data retrieval from JSON
- Write data to S3 in parquet. Overwrite, if partition already exist
"""
import argparse
from pyspark.sql import SparkSession

all_fields = (

    "data.author",
    "data.author_fullname",
    "data.author_is_blocked",

    "data.subreddit",
    "data.subreddit_id",
    "data.subreddit_name_prefixed",
    "data.subreddit_subscribers",
    "data.subreddit_type",

    "data.created_utc",
    "data.domain",
    "data.gilded",
    "data.hidden",
    "data.hide_score",
    "data.id",
    "data.is_created_from_ads_ui",
    "data.is_crosspostable",
    "data.is_video",
    "data.likes",
    "data.media.type",
    "data.media.event_id",
    "data.name",
    "data.no_follow",
    "data.num_comments",
    "data.num_crossposts",
    "data.num_reports",
    "data.over_18",
    "data.pinned",
    "data.saved",
    "data.score",
    "data.send_replies",

    "data.title",
    "data.total_awards_received",
    "data.ups",
    "data.upvote_ratio",
    "data.url"
)


def transform_load_data(spark, input_data, output_data, ds):
    """
    Process raw json  data.

    Read Reddit data from json logs stored in s3.
    Write processed Reddit data to parquet files, using PySpark.
    :param spark: SparkSession object
    :param input_data: s3 file path for input data
    :param output_data: s3 file path for output data
    :param ds: snapshot date
    """
    # read raw reddit data json file
    reddit_df = spark.read.json(input_data)
    # extract columns to create table
    reddit_requests_table = (reddit_df
                             .selectExpr(*all_fields,
                                         f"'{ds}' AS date")
                             .dropDuplicates())
    # write songs table to parquet files partitioned by year and artist
    (reddit_requests_table
     .write
     .partitionBy("date")
     .mode("overwrite")
     .option("partitionOverwriteMode", "dynamic")
     .parquet(output_data))


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds",
                        type=str,
                        help="execution date",
                        default="2022-12-01")
    parser.add_argument("--s3_bucket",
                        type=str,
                        help="s3 bubcket",
                        default="reddit-project-data")
    parser.add_argument("--input_file_path",
                        type=str,
                        help="execution year",
                        default="raw-json-logs/reddit-worldnews-hot-2022-12-01.json")
    parser.add_argument("--output_file_path",
                        type=str,
                        help="execution year",
                        default="reddit-data/")
    args = parser.parse_args()
    # get parameters
    ds = args.ds
    s3_bucket = args.s3_bucket
    input_file_path = args.input_file_path
    output_file_path = args.output_file_path
    # initialize spark session on EMR
    spark_session = (SparkSession
             .builder
             .appName("From S3 to parquet")
             .getOrCreate())
    # s3 input/output paths
    s3_input_data = f's3a://{s3_bucket}/{input_file_path}{ds}.json'
    s3_output_data = f's3a://{s3_bucket}/{output_file_path}'
    # execute transformation function
    transform_load_data(spark_session,
                        s3_input_data,
                        s3_output_data,
                        ds)
