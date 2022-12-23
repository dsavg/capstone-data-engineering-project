# Capstone project for Udacity's Data Engineering Nanodegree
Project is work in progress, stay tuned...

## 1. Set up
### 1.1. Amazon Web Servises
* Creating an IAM Role on AWS with `AdministratorAccess`, `AmazonRedshiftFullAccess`, and `AmazonS3FullAccess` attached policies [link](https://docs.aws.amazon.com/IAM/latest/UserGuide/intro-structure.html). Save `Access key ID` and the `Secret access key` to use in Airflow later.
* Create the S3 bucket named `reddit-project-data` manually

### 1.2. Reddit API
Follow the [How to Use the Reddit API in Python](https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c) post to set up your credentials for the Reddit API. Ones you've done that, you will need the `client_id`, `client_secret`, `password`, `user_agent`, and `username` for Airflow. 

### 1.3. Airflow
TO DO

## 2. Data Warehouse
### 2.1. DAGs
#### 2.1.1. reddit_get_api_data
![img0](imgs/reddit_get_api_data_dag.png)

#### 2.1.2. reddit_processing_dag
![img1](imgs/reddit_processing_dag.png)

#### 2.1.2. reddit_dwr_dag
TO DO

### 2.2. Custom Operators
[RedditΤoS3Operator](https://github.com/dsavg/capstone-data-engineering-project/blob/master/plugins/operators/reddit_api.py): Operator to get API Reddit data and store them in S3 in JSON format.

[S3PartitionCheck](https://github.com/dsavg/capstone-data-engineering-project/blob/master/plugins/operators/s3_partition_check.py): Operator to check is date partition exists in S3 path.

## Resources
* https://towardsdatascience.com/run-airflow-docker-1b83a57616fb
* https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/
* https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c