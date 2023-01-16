MAIN_SOURCE_BUCKET = "de-tech-assessment-2022"
DATA_FOLDER = "data/"
DATALAKE_BUCKET = "de-tech-assessment-2022-gonzalo"
RAW_ZONE_FOLDER = "raw_zone/"
STRUCTURE_ZONE_FOLDER = "structure_zone/"
GLUE_JOBS_FOLDER = "glue_jobs/"
AWS_DEFAULT_CONN = "aws_default"

GLUE_JOB_ARGS = {
    "RAW_DATA": f"s3://{DATALAKE_BUCKET}/{RAW_ZONE_FOLDER}",
    "SAVE_FOLDER": f"s3://{DATALAKE_BUCKET}/{STRUCTURE_ZONE_FOLDER}",
    "TABLE_COLUMN_IDENTIFIER": "on",
    "TABLES_TO_TRANSFORM": [
        {
            "table_name": "vehicle",
            "cast_policies": {
                "at": {
                    "formatted_name": "at",
                    "data_type": "TimeStampType"
                },
                "event": {
                    "formatted_name": "event",
                    "data_type": "StringType"
                },
                "data.id": {
                    "formatted_name": "data_id",
                    "data_type": "StringType"
                },
                "data.location.at": {
                    "formatted_name": "location_at",
                    "data_type": "TimeStampType"
                },
                "data.location.lat": {
                    "formatted_name": "location_lat",
                    "data_type": "DoubleType"
                },
                "data.location.lng": {
                    "formatted_name": "location_lng",
                    "data_type": "DoubleType"
                },
                "on": {
                    "formatted_name": "on",
                    "data_type": "StringType"
                },
                "organization_id": {
                    "formatted_name": "organization_id",
                    "data_type": "StringType"
                }
            },
            "partition_parameter": "at",
            "save_format": "parquet"

        },
        {
            "table_name": "operating_period",
            "cast_policies": {
                "at": {
                    "formatted_name": "at",
                    "data_type": "TimeStampType"
                },
                "event": {
                    "formatted_name": "event",
                    "data_type": "StringType"
                },
                "data.id": {
                    "formatted_name": "data_id",
                    "data_type": "StringType"
                },
                "data.start": {
                    "formatted_name": "operation_start",
                    "data_type": "TimeStampType"
                },
                "data.finish": {
                    "formatted_name": "operating_finish",
                    "data_type": "TimeStampType"
                },
                "on": {
                    "formatted_name": "on",
                    "data_type": "StringType"
                },
                "organization_id": {
                    "formatted_name": "organization_id",
                    "data_type": "StringType"
                }
            },
            "partition_parameter": "at",
            "save_format": "parquet"

        }
    ]
}


GLUE_CRAWLER_NAME = "door2door_{}_crawler"
GLUE_ROLE = "AWSGlueServiceRole-door2door2"
GLUE_DATABASE_NAME = "door2doordb2"

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