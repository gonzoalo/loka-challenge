import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as f
from pyspark.sql.types import StringType,BooleanType,DateType, DoubleType, LongType, IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job
import json


def cast_columns(df: DynamicFrame, cast_policies: dict) -> DynamicFrame:
    """
   Cast the columns for the dimensional model with the cast policies given as an argument.
    Args:
        df (DataFrame): Dataframe of the table to be casted.
        cast_policies (dict): Dictionary of the policies to cast the data in the table.
    Returns:
        df (DataFrame): Resulting dataframe after the cast columns transfromation
    """

    final_columns = []
    for column, cast_policy in cast_policies.items():
        if cast_policy["data_type"] == "IntegerType":
            df = df.withColumn(cast_policy["formatted_name"],f.col(column).cast(IntegerType()))
        elif cast_policy["data_type"] == "BooleanType":
            df = df.withColumn(cast_policy["formatted_name"],f.col(column).cast(BooleanType()))
        elif cast_policy["data_type"] == "DateType":
            df = df.withColumn(cast_policy["formatted_name"],f.col(column).cast(DateType()))
        elif cast_policy["data_type"] == "StringType":
            df = df.withColumn(cast_policy["formatted_name"],f.col(column).cast(StringType()))
        elif cast_policy["data_type"] == "DoubleType":
            df = df.withColumn(cast_policy["formatted_name"],f.col(column).cast(DoubleType()))
        elif cast_policy["data_type"] == "TimeStampType":
            df = df.withColumn(cast_policy["formatted_name"],f.to_timestamp(column))
        final_columns.append(cast_policy["formatted_name"])
    
    return df.select(final_columns)

def null_treatment(df: DynamicFrame, null_policies: dict) -> DynamicFrame:
    """
    Treat the columns with null values given the null treatment policies givcen as an argument.
    Args:
        df (DataFrame): Dataframe of the table to be cleaned.
        null_policies (dict): Dictionary of the policies to treat the null data columns in the table.
    Returns:
        df (DataFrame): Resulting dataframe after the null treatment transfromation
    """
    for column, policy in null_policies.items():
        if policy["null_policy"] == "drop":
            df = df.where(~f.isnull(df[column]))
        elif policy["null_policy"] == 'replace':
            df = df.withColumn(column, f.when(df[column] == '', f.lit(policy["replacement"])).otherwise(df[column]))
    return df


args = getResolvedOptions(
    sys.argv,
    [
        "RAW_DATA",
        "TABLES_TO_TRANSFORM",
        "SAVE_FOLDER",
        "TABLE_COLUMN_IDENTIFIER",
        "SAVE_FORMAT"
    ]
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

raw_data = args["RAW_DATA"]
tables_to_transform = json.loads(args["TABLES_TO_TRANSFORM"])
save_folder = args["SAVE_FOLDER"]
table_column_identifier = args["TABLE_COLUMN_IDENTIFIER"]
save_format = args["SAVE_FORMAT"]

df = spark.read.option("header", "true")\
    .option("inferSchema", "true").json(raw_data)

for table in tables_to_transform:
    
    table_name = table["table_name"]
    cast_policies = table["cast_policies"]
    null_policies = table["null_policies"]
    partition_parameter = table["partition_parameter"]
    current_df = df.where(df[table_column_identifier] == table_name)

    if cast_policies:
        current_df = cast_columns(current_df, cast_policies)
    
    if null_policies:
        current_df = null_treatment(current_df, null_policies)

    # For this project and for the format of the data that is comming 
    # We are going to use for partition a (day, minute) tuple
    current_df = current_df.withColumn("minute", f.minute(f.col(partition_parameter))).\
            withColumn("day", f.dayofmonth(f.col(partition_parameter)))

    current_df.write.partitionBy(["day", "minute"]).mode("append").format(save_format)\
            .save(f"{save_folder}{table_name}_table")
