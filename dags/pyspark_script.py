import os
import configparser

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T
from pyspark.sql.functions import monotonically_increasing_id

os.environ["AWS_ACCESS_KEY_ID"]= 'AKIAXBIZ76BF6X6GJ24O'
os.environ["AWS_SECRET_ACCESS_KEY"]= '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'


spark = SparkSession.builder.appName("My Py App").getOrCreate()

df = spark.read.json("s3a://fpmacedo/match_results/match_results.json")

df.printSchema()
cols = ("_corrupt_record")
df = df.drop("_corrupt_record")
df = df.dropna()
df.show(5)

# create timestamp column from original timestamp column
get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType())
# create timestamp column from original timestamp column
get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType())
# create datetime column from original timestamp column
get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
get_hour = udf(lambda x: x.hour, T.IntegerType()) 
get_day = udf(lambda x: x.day, T.IntegerType()) 
get_week = udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
get_month = udf(lambda x: x.month, T.IntegerType()) 
get_year = udf(lambda x: x.year, T.IntegerType()) 
get_weekday = udf(lambda x: x.weekday(), T.IntegerType())
    
df = df.withColumn("timestamp", get_timestamp(df.data_unix))
df = df.withColumn('start_time', get_datetime(df.data_unix))
df = df.withColumn("hour", get_hour(df.timestamp))
df = df.withColumn("day", get_day(df.timestamp))
df = df.withColumn("week", get_week(df.timestamp))
df = df.withColumn("month", get_month(df.timestamp))
df = df.withColumn("year", get_year(df.timestamp))
df = df.withColumn("weekday", get_weekday(df.timestamp))

# extract columns to create time table
time_columns = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
# write time table to parquet files partitioned by year and month
time_table = df[time_columns]
time_table.write.partitionBy('year', 'month').parquet(os.path.join("s3a://fpmacedo/tables/", 'time.parquet'), 'overwrite')
print("--- time.parquet completed ---")