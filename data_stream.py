import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unix_timestamp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


# TODO Create a schema for incoming resources
# schema
schema = StructType([StructField('crime_id', StringType(), True),
                     StructField('original_crime_type_name', StringType(), True),
                     StructField('call_date_time', StringType(), True),
                     StructField('address', StringType(), True),
                     StructField('disposition', StringType(), True)])


# TODO create a spark udf to convert time to YYYYmmDDhh format
psf.udf(StringType())
def udf_convert_time(timestamp):
    get_datetime = datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:00')
    return get_datetime

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    print("Spark function started")
    df = spark\
         .readStream\
         .format("kafka")\
         .option("kafka.bootstrap.servers", "localhost:9092")\
         .option("subscribe", "service-calls")\
         .option("maxOffsetsPerTrigger", 200)\
         .option("startingOffsets", "earliest")\
         .option('failOnDataLoss', False)\
         .load()

    print("DF created")
    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")
   
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")
 
    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # TODO get different types of original_crime_type_name in 60 minutes interval
    counts_df = distinct_table\
        .withWatermark('call_datetime', '20 seconds')\
        .groupBy(distinct_table.original_crime_type_name, window(distinct_table.call_datetime, "1 hour"))\
        .count()
        
    counts_df.isStreaming

    ############### Testing counts #################
#     query1 = (counts_df
#              .writeStream
#              .format("console")
#              .queryName('counts')
#              .outputMode("complete")
#              .start()
#              .awaitTermination())

    # TODO use udf to convert timestamp to right format on a call_date_time column
    conv =  udf (lambda z: datetime.strptime(str(udf_convert_time(z)), '%Y-%m-%d %H:%M:00'), TimestampType())
    converted_df = distinct_table\
        .withWatermark('call_datetime', '20 seconds')\
        .withColumn('datetimeFormat', conv(distinct_table['call_datetime']))\
        .withColumn('desired_format', date_format(col('datetimeFormat'),'YYYYmmDDhh'))
 
    ############### Testing time #################
#     query2 = (converted_df
#              .writeStream
#              .format("console")
#              .outputMode("append")
#              .queryName('timechange')
#              .start()
#              .awaitTermination())
       

    # TODO apply aggregations using windows function to see how many calls occurred in 2 day span
    calls_per_2_days = distinct_table\
        .withWatermark("call_datetime", "20 seconds")\
        .groupBy(
            psf.window(distinct_table.call_datetime, "20 seconds", "5 seconds"),
            distinct_table.original_crime_type_name
            )\
        .count()

   ############### Testing calls #################

#     query3 = (calls_per_2_days
#          .writeStream
#          .format("console")
#          .outputMode("complete")
#          .queryName('calls')
#          .start()
#          .awaitTermination())
        
    # TODO write output stream
    query = distinct_table\
           .writeStream\
           .format('console')\
           .option("kafka.bootstrap.servers", "localhost:9092")\
           .option('topic', "service-calls")\
           .option("checkpointLocation", "/home/workspace/consumer_server.py")\
           .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = SparkSession.builder \
        .master("local") \
        .appName("Project1") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()