from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
from config import *
import botocore
import re

def _fetch_data(ti, **kwargs):
    """
    Fetch data daily from the bucket in MAIN_SOURCE_BUCKET to the 
    new bucket in DATA_LAKE_BUCKET where there are a folder for the raw_zone 
    where the data would be stored in the original state before transfromations.
    """

    datetime_format = "%Y-%m-%d"
    date_tag = datetime.now().strftime(datetime_format)

    # Because of the project is set to test with a fixed date we are going to se the 
    # variables 'test_tag' but in a production environment we should use 'date_tag'
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