"""
Dag to request Reddit data from API and store them in S3 in JSON format.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import S3PartitionCheck
from helpers import JOB_FLOW_OVERRIDES

# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

BUCKET_NAME = "reddit-project-data"
aws_creds = "aws_default"

default_args = {
    'owner': 'danai',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 16, 0, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False
}


dag = DAG('reddit_preprocessing',
          default_args=default_args,
          description='Load and transform Reddit data in Redshift with Airflow',
          schedule_interval='0 0 * * *'  # once an day
          )

# S3 partition check
reddit_data_sensor = S3PartitionCheck(
    task_id='reddit_s3_data_sensor',
    dag=dag,
    subreddit_name='worldnews',
    subreddit_type='hot',
    aws_credentials_id=aws_creds,
    s3_bucket=BUCKET_NAME,
    s3_key="raw-json-logs",
    params={
        'end_date': '{{ ds }}'
    }
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    dag=dag,
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='aws_credentials',
    region_name="us-west-2"
)

def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

# move pySpark code to s3 to execute on EMR
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": "./plugins/helpers/s3_to_parquet.py",
               "key": "scripts/s3_to_parquet.py",},
)

SPARK_STEPS = [
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                "--dt",
                " {{ params.dt }}",
                "--s3_bucket",
                "{{ params.BUCKET_NAME }}",
                "--input_file_path",
                "{{ params.input_file_path }}",
                "--output_file_path",
                "{{ params.output_file_path }}"
            ],
        },
    },
]

dt = '2022-12-22' # TO DO: remove hardcoded value here
# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    dag=dag,
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_script": "scripts/s3_to_parquet.py",
        "dt": f'{dt}',
        "input_file_path": f"raw-json-logs/reddit-worldnews-hot-{dt}.json",
        "output_file_path": "reddit-data/"
    },

)

last_step = len(SPARK_STEPS) - 1

# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

reddit_data_sensor >> \
create_emr_cluster >> \
script_to_s3 >> \
step_adder >> \
step_checker >> \
terminate_emr_cluster