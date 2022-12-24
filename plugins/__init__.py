from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class RedditPlugin(AirflowPlugin):
    name = "reddit_plugin"
    operators = [
        operators.RedditΤoS3Operator,
        operators.S3PartitionCheck,
        operators.StageToRedshiftOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SubredditAPI,
        helpers.JOB_FLOW_OVERRIDES
    ]
