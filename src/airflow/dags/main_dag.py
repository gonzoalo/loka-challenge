from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from utils import _fetch_data
from config import *

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
        script_args=GLUE_JOB_ARGS,
        wait_for_completion=True,
        script_location=f's3://{DATALAKE_BUCKET}/{GLUE_JOBS_FOLDER}process_data.py',
        s3_bucket=DATALAKE_BUCKET,
        iam_role_name=GLUE_ROLE,
        create_job_kwargs={'GlueVersion': '3.0', 'NumberOfWorkers': 2, 'WorkerType': 'G.1X'},
    )

    crawl_s3_vehicles = GlueCrawlerOperator(
        task_id='crawl_s3_vehicles',
        config=GLUE_CRAWLER_VEHICLE_CONFIG,
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
