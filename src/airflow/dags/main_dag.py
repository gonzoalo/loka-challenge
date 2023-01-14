from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago

import botocore
import re
from datetime import datetime, timedelta

# send this to .env file
MAIN_SOURCE_BUCKET = "de-tech-assessment-2022"
DATA_FOLDER = "data/"
DATALAKE_BUCKET = "de-tech-assessment-2022-gonzalo"
RAW_ZONE_FOLDER = "raw_zone/"
STRUCTURE_ZONE_FOLDER = "structure_zone/"
GLUE_JOBS_FOLDER = "glue_jobs"
AWS_DEFAULT_CONN = "aws_default"

GLUE_CRAWLER_NAME="door2door_{}_crawler"
GLUE_ROLE="arn:aws:iam::057286332506:role/AWSGlueServiceRole-door2door2"
GLUE_DATABASE_NAME="door2doordb2"

GLUE_CRAWLER_VEHICLE_CONFIG = {
    'Name': GLUE_CRAWLER_NAME.format("vehicle"),
    'Role': GLUE_ROLE,
    'DatabaseName': GLUE_DATABASE_NAME,
    'Targets': {
        'S3Targets': [
            {
                'Path': f'{DATALAKE_BUCKET}/{STRUCTURE_ZONE_FOLDER}vehicle_table',
            }
        ]
    },
}

GLUE_CRAWLER_OP_CONFIG = {
    'Name': GLUE_CRAWLER_NAME.format("operating_period"),
    'Role': GLUE_ROLE,
    'DatabaseName': GLUE_DATABASE_NAME,
    'Targets': {
        'S3Targets': [
            {
                'Path': f'{DATALAKE_BUCKET}/{STRUCTURE_ZONE_FOLDER}operating_period_table',
            }
        ]
    },
}


def _fetch_data(ti, **kwargs):
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
        python_callable=_fetch_data
    )

    job_name = 'process_data_glue_job'
    process_data_glue_job = GlueJobOperator(
        task_id='process_data_glue_job',
        job_name=job_name,
        wait_for_completion=True,
        script_location=f's3://{DATALAKE_BUCKET}/{GLUE_JOBS_FOLDER}/process_data.py',
        s3_bucket=DATALAKE_BUCKET,
        iam_role_name=GLUE_ROLE.split('/')[-1],
        create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 2, 'WorkerType': 'G.1X'},
    )

    crawl_s3_vehicles = GlueCrawlerOperator(
        task_id='crawl_s3_vehicles',
        config=GLUE_CRAWLER_VEHICLE_CONFIG  ,
        wait_for_completion=False,
    )

    crawl_s3_operating_perriod = GlueCrawlerOperator(
        task_id='crawl_s3_operating_perriod',
        config=GLUE_CRAWLER_OP_CONFIG,
        wait_for_completion=False,
    )

    fetch_data>>process_data_glue_job
    process_data_glue_job>>crawl_s3_vehicles
    process_data_glue_job>>crawl_s3_operating_perriod
