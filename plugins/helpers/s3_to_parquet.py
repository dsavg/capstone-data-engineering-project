# pyspark
import logging
import argparse
from pyspark.sql import SparkSession

template_fields = ("ds",)

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

def transform_load_data(spark, input_data, output_data, dt):
    """
    Process raw json  data.

    Read song data from json logs stored in s3.
    Write `songs` and `artists` dimension tables to parquet
    files, using PySpark.

    :param spark: SparkSession object
    :param input_data: s3 file path for input data
    :param output_data: s3 file path for output data
    """
    # read raw reddit data json file
    reddit_df = spark.read.json(input_data)

    # extract columns to create table
    reddit_requests_table = (reddit_df
                             .selectExpr(*all_fields,
                                         f"'{dt}' AS date")
                             .dropDuplicates())

    # write songs table to parquet files partitioned by year and artist
    (reddit_requests_table
     .write
     .partitionBy("date")
     .mode("overwrite")
     .option("partitionOverwriteMode", "dynamic")
     .parquet(output_data))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--dt",
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
    # get parameter
    dt = args.dt
    s3_bucket = args.s3_bucket
    input_file_path = args.input_file_path
    output_file_path = args.output_file_path

    spark = (SparkSession
             .builder
             .appName("From S3 to parquet")
             .getOrCreate())

    s3_input_data = f's3a://{s3_bucket}/{input_file_path}'
    s3_output_data = f's3a://{s3_bucket}/{output_file_path}'

    transform_load_data(spark,
                        s3_input_data,
                        s3_output_data,
                        dt)