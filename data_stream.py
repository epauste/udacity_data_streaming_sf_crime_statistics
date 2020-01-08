import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False), # "183642658"
    StructField("original_crime_type_name", StringType(), False), # "Fight No Weapon Dv"
    StructField("report_date", StringType(), False), # "2018-12-30T00:00:00.000"
    StructField("call_date", StringType(), False), # "2018-12-30T00:00:00.000"
    StructField("offense_date", StringType(), False), # "2018-12-30T00:00:00.000"
    StructField("call_time", StringType(), False), # "17:59" 
    StructField("call_date_time", StringType(), False), # "2018-12-30T17:59:00.000"
    StructField("disposition", StringType(), False), # "HAN"
    StructField("address", StringType(), False), # "1100 Block Of Mcallister St"
    StructField("city", StringType(), False), # "San Francisco"
    StructField("state", StringType(), False), # "CA"
    StructField("agency_id", StringType(), False), # "1"
    StructField("address_type", StringType(), False), # "Premise Address"
    StructField("common_location", StringType(), True), # "Twin Peaks Viewpoint, Sf"
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'com.udacity.project.sfcrimes') \
        .option('startingOffsets', 'latest') \
        .option('maxOffsetsPerTrigger', 1000) \
        .option('stopGracefullyOnShutdown', 'true') \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    service_table.printSchema()
   
    
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select('original_crime_type_name', 'disposition').distinct()
    distinct_table.printSchema()    
    
    # count the number of original crime type
    agg_df = distinct_table.groupBy('original_crime_type_name').count().orderBy('count', ascending=False)    
    agg_df.printSchema()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode('complete') \
        .format('console') \
        .queryName('batch-ingestion-agg-distinct-oriiginal-crime-type') \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    # add_df => Stream, radio_code_df => static, left outer so streaming record still returned in no static match
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition, "left_outer")
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.default.parallelism", 500) \
        .config("spark.sql.shuffle.partitions", 5) \
        .config("spark.ui.port", 3000) \
        .getOrCreate()
       

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
