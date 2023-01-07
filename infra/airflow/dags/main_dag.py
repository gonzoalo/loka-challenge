from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

from datetime import datetime, timedelta

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def _extract_data(ti, **kwargs):
    s3_link_location = "s3://de-tech-assessment-2022/data/"
    



with DAG(dag_id='door2door_dag', default_args=default_args, schedule_interval="@daily", 
    start_date=days_ago(1)) as dag:
    
    copy_object = S3CopyObjectOperator(
    task_id="send_data_to_rawzone",
    source_bucket_key="s3://de-tech-assessment-2022/data",
    dest_bucket_key="s3://de-tech-assessment-2022-gonzalo/raw_zone")

    chain(copy_object)

