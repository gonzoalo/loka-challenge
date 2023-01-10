from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

import boto3


from datetime import datetime, timedelta

MAIN_SOURCE_BUCKET = "s3://de-tech-assessment-2022/"
DATA_FOLDER = "data/"
DATALAKE_BUCKET = "s3://de-tech-assessment-2022-gonzalo"
RAW_ZONE_FOLDER = "raw_zone/"
STRUCTURE_ZONE_FOLDER = "structure_zone/"


default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def _extract_data(ti, **kwargs):
    s3_link_location = "s3://de-tech-assessment-2022/data/"
    

def fetch_data(ti, **kwargs):
    """
    Fetch data daily from the bucket s3://de-tech-assessment-2022/ to the 
    new bucket s3://de-tech-assessment-2022-gonzalo/ there are a folder called raw_zone 
    for the raw data which is the data at is original state before transfromations.
    2019-06-01-15-29-5-events.json

    Args:
    - ti:
    - kwargs:
    """
    datetime_format = "%Y-%m-%d %H-%M-%S"
    date_tag = datetime.now().strftime(datetime_format)
    test_tag = "2019-06-01-15-29-5"

    session = boto3.Session()




with DAG(dag_id='door2door_dag', default_args=default_args, schedule_interval="@daily", 
    start_date=days_ago(1)) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )
    
    # copy_object = S3CopyObjectOperator(
    # task_id="send_data_to_rawzone",
    # source_bucket_name="de-tech-assessment-2022",
    # dest_bucket_name="de-tech-assessment-2022-gonzalo",
    # source_bucket_key="data/",
    # dest_bucket_key="raw_zone/data/"
    # )

    chain(fetch_data)

