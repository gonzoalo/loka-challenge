import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as f
from awsglue.context import GlueContext
from awsglue.job import Job


"""
Here we need to do a data transformation and split the data in two tables 
vehicles and operating period

"""

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

raw_data = "s3://de-tech-assessment-2022-gonzalo/raw_zone/"

df = spark.read.option("header", "true")\
    .option("inferSchema", "true").json(raw_data)



df_operating_period = df.where(df.on == 'operating_period')
df_vehicle = df.where(df.on == 'vehicle')

df_vehicle = df_vehicle.select(f.col('at'), f.col('event'), f.col('on'), f.col('organization_id'),
          f.col('data.id'), f.col('data.location.at').alias('location_at'), 
          f.col('data.location.lat').alias('latitude'), 
          f.col('data.location.lng').alias('longitude'))

df_vehicle = df_vehicle.withColumn('minute', f.minute(f.col('at'))).\
            withColumn('day', f.dayofmonth(f.col('at')))

df_vehicle.write.partitionBy(['day', 'minute']).mode('append').format('parquet')\
            .save("s3://de-tech-assessment-2022-gonzalo/structure_zone/vehicle_table")

df_operating_period = df_operating_period.select(f.col('at'), f.col('event'), 
            f.col('on'), f.col('organization_id'), f.col('data.id'), f.col('data.start'), 
            f.col('data.finish'))

df_operating_period = df_operating_period.withColumn('minute', f.minute(f.col('at'))).\
            withColumn('day', f.dayofmonth(f.col('at')))

df_operating_period.write.partitionBy(['day', 'minute']).mode('append').format('parquet')\
            .save("s3://de-tech-assessment-2022-gonzalo/structure_zone/operating_period_table")

