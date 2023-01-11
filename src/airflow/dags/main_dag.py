from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

import botocore
import re
from datetime import datetime, timedelta

# send this to .env file
MAIN_SOURCE_BUCKET = "de-tech-assessment-2022"
DATA_FOLDER = "data/"
DATALAKE_BUCKET = "de-tech-assessment-2022-gonzalo"
RAW_ZONE_FOLDER = "raw_zone/"
STRUCTURE_ZONE_FOLDER = "structure_zone/"
AWS_DEFAULT_CONN = "aws_default"


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

    datetime_format = "%Y-%m-%d"
    date_tag = datetime.now().strftime(datetime_format)
    test_tag = "2019-06-01"

    s3_hook = S3Hook(aws_conn_id=AWS_DEFAULT_CONN)
    s3_hook.get_conn()

    prefix = f"{DATA_FOLDER}{test_tag}"
    data_objects = s3_hook.list_keys(bucket_name=MAIN_SOURCE_BUCKET, prefix=prefix)

    for data_object in data_objects:
        object_key = re.sub(DATA_FOLDER, '', data_object)
        if re.findall('.json', object_key):
            source_bucket_key = f"s3://{MAIN_SOURCE_BUCKET}/{DATA_FOLDER}{object_key}"
            dest_bucket_key = f"s3://{DATALAKE_BUCKET}/{RAW_ZONE_FOLDER}{object_key}"

            try:
                response = s3_hook.copy_object( 
                    source_bucket_key=source_bucket_key,
                    dest_bucket_key=dest_bucket_key
                )

                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    pass
                else:
                    print(f"The file {object_key} was not properly copied.")
                
            except botocore.exceptions.ClientError as error:
                raise error
            
            except botocore.exceptions.ParamValidationError as error:
                raise ValueError('The parameters you provided are incorrect: {}'.format(error))

default_args = {
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(dag_id='door2door_dag', default_args=default_args, schedule_interval="@daily", 
    start_date=days_ago(1)) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )

    process_data = AWSGlueJobOperator(
        task_id="process_data",
        python_callable=process_data
    )
    job_name = 'example_glue_job'
    submit_glue_job = GlueJobOperator(
        task_id='submit_glue_job',
        job_name=job_name,
        wait_for_completion=False,
        script_location=f's3://{GLUE_EXAMPLE_S3_BUCKET}/etl_script.py',
        iam_role_name=GLUE_CRAWLER_ROLE.split('/')[-1],
        create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 2, 'WorkerType': 'G.1X'},
    )



    chain(fetch_data)
