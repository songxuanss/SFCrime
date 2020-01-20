import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField("original_crime_type_name", StringType(), True),
    StructField("disposition", StringType(), True)
])


def run_spark_job(spark):
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port

    df = spark \
        .readStream \
        .format("kafka")\
        .option("subscribe", "ppp3")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("maxRatePerPartition", 100) \
        .option("maxOffsetsPerTrigger", 10) \
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", "false")\
        .load()

    df.printSchema()
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(psf.col("original_crime_type_name"), psf.col("disposition"))

    # count the number of original crime type
    agg_df = distinct_table.groupBy(distinct_table.original_crime_type_name, distinct_table.disposition).count().alias("agg")

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream.queryName("agg query").outputMode("complete").format("console")\
        .trigger(once=True).option("checkPointLocation", "/home/pauls/checkpoint").start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition").alias("radio")

    join_query = agg_df.join(radio_code_df, "disposition").writeStream.outputMode("complete").format("console").start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
