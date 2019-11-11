import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("offense_date", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("common_location", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("crime_id", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("address", StringType(), True),
    StructField("original_crime_type_name", StringType(), True)
])
# creating a JSON schema
# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    d = parse_date(timestamp)
    return str(d.strftime('%y%m%d%H'))
    
def run_spark_job(spark):
# TODO Create Spark Configuration
# Create Spark configurations with max offset of 200 per trigger

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "call-centre") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetPerTrigger", "200") \
    .load()

    df.printSchema()

# key[binary]
# value[binary]
# topic[string]
# partition[int]
# offset[long]
# timestamp[long]
# timestampType[int] 

    kafka_df = df.selectExpr("CAST(value AS STRING)")
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("CALL-CENTRE"))\
        .select("CALL-CENTRE.*")

    # service_table.printSchema()

    distinct_table = service_table \
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # distinct_table.printSchema()


    
    counts_df = distinct_table \
        .withWatermark("call_datetime", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_datetime, "10 minutes", "5 minutes"),
            distinct_table.original_crime_type_name
            ).count()

    # counts_df.printSchema()


    query = counts_df \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()
     

    # TODO attach a ProgressReporter
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName('Kafka_Spark_Structured_Streaming') \
        .getOrCreate()
    
    spark.sparkContext.getConf().getAll()
    logger.info("Spark started")
    
    run_spark_job(spark)

    spark.stop()
