"""
Dag to pprocess Reddit data using PySpark, and store them in parquet format on S3.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

from operators import S3PartitionCheck
from helpers import JOB_FLOW_OVERRIDES

# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

BUCKET_NAME = "reddit-project-data"
aws_creds = "aws_default"

default_args = {
    'owner':'danai',
    'depends_on_past':False,
    'start_date': datetime(2022, 11, 30, 0, 0, 0, 0),
    'retries': 3,
    'max_active_runs':1,
    'retry_delay':timedelta(minutes=5),
    'catchup':True,
    'email_on_failure':False
}


dag = DAG('reddit_processing',
          default_args=default_args,
          description='Process Reddit JSON logs with PySpark on EMR and '
                      'store them on S3 in parquet format with Airflow.',
          schedule_interval='0 15 * * *'  # daily at 7:05 am PST
          )

# Start operator
start_operator = DummyOperator(task_id='begin_execution',
                               dag=dag)

# S3 partition check
reddit_data_sensor = S3PartitionCheck(
    task_id='reddit_s3_data_sensor',
    dag=dag,
    aws_credentials_id=aws_creds,
    s3_bucket=BUCKET_NAME,
    s3_key="raw-json-logs/reddit-data-{}.json",
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

# Move PySpark code to S3 to execute on EMR
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": "./plugins/helpers/scripts/s3_to_parquet.py",
               "key": "scripts/s3_to_parquet.py",},
)

SPARK_STEPS = [
    {
        "Name": "Process Reddit Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                "--ds",
                "{{ ds }}",
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

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    dag=dag,
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                "key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_script": "scripts/s3_to_parquet.py",
        "input_file_path": "raw-json-logs/reddit-data-",
        "output_file_path": "reddit-data/"
    },

)

last_step = len(SPARK_STEPS) - 1

# Wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', "
                "key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', "
            "key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', "
                "key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# End operator
end_operator = DummyOperator(task_id='end_execution',
                               dag=dag)

start_operator>> \
reddit_data_sensor >> \
script_to_s3 >> \
create_emr_cluster >> \
step_adder >> \
step_checker >> \
terminate_emr_cluster >> \
end_operator
