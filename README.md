# Capstone project for Udacity's Data Engineering Nanodegree

The goal of this project is to create a Data Warehouse to analyze trending and new subreddits using Airflow.

The project uses the Reddit API to get subreddits and stores them on AWS S3 in JSON format. Data processing happens 
on an EMR cluster on AWS using PySpark and processed data gets stored on AWS S3 in parquet format. Finally, the data 
gets inserted into AWS Redshift, gets denormalized to create fact and dimension tables.

## Set up
* [Set up](https://github.com/dsavg/capstone-data-engineering-project/wiki/Set-up)

## DAGs
* [DAG: reddit_get_api_data](https://github.com/dsavg/capstone-data-engineering-project/wiki/DAG:-reddit_get_api_data)  
* [DAG: reddit_processing](https://github.com/dsavg/capstone-data-engineering-project/wiki/DAG:-reddit_processing)   
* [DAG: reddit_stagging](https://github.com/dsavg/capstone-data-engineering-project/wiki/DAG:-reddit_stagging)  
* [DAG: reddit_dwr](https://github.com/dsavg/capstone-data-engineering-project/wiki/DAG:-reddit_dwr)  

## Airflow Plugins
* [Custom Operators](https://github.com/dsavg/capstone-data-engineering-project/wiki/Custom-Operators)

## Data Model explained
* [Data Warehouse](https://github.com/dsavg/capstone-data-engineering-project/wiki/Datawarehouse-Architecture)
* [Data Model](https://github.com/dsavg/capstone-data-engineering-project/blob/master/scripts/data-model-explained.ipynb)

## Udacity requeirements
* [Data Justifications](https://github.com/dsavg/capstone-data-engineering-project/wiki/Data-Justifications)

## Resources
* https://towardsdatascience.com/run-airflow-docker-1b83a57616fb
* https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/
* https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c