import os
import configparser
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# config = configparser.ConfigParser()
# config.read('dl.cfg')
# # set aws configs
# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

all_fields = (
    "data.author",
    "data.author_fullname",
    "data.author_is_blocked",
    "data.created_utc",
    # "data.crosspost_parent",
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
    "data.subreddit",
    "data.subreddit_id",
    "data.subreddit_name_prefixed",
    "data.subreddit_subscribers",
    "data.subreddit_type",
    "data.title",
    "data.total_awards_received",
    "data.ups",
    "data.upvote_ratio",
    "data.url"
)

# Transform Functions
def create_spark_session():
    """
    Create spark session with hadoop aws.

    :return: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

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
    # get file paths to song data file
    reddit_raw_data = input_data + f'data/json_logs/{dt}/reddit-worldnews.json'

    # read song data file
    reddit_df = spark.read.json(reddit_raw_data)

    # extract columns to create songs table
    reddit_requests_table = (reddit_df
                             .selectExpr(*all_fields,
                                         f"'{dt}' AS date")
                             .dropDuplicates())

    # write songs table to parquet files partitioned by year and artist
    (reddit_requests_table
     .write
     .partitionBy("date")
     .mode("overwrite")
     .parquet(output_data + f"data/reddit_data/reddit_data/date={dt}"))


if __name__ == "__main__":
    # create spark session
    spark = create_spark_session()
    # get the current utc date
    now = datetime.now(timezone.utc)
    current_date = str(now.date())
    # current_date = '2022-12-12'
    # load data from json and store them in a parquet table in spark
    s3_input_path = ''
    s3_output_path = ''
    transform_load_data(spark, s3_input_path, s3_output_path, current_date)
