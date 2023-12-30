"""Script for processing kafka streams 
#############################################################
import pyspark and its configuration
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, MapType
# Reference: https://denisecase.github.io/starting-spark
import os
from typing import List, Optional
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour
import pyspark


print(pyspark.__version__)



   
def create_or_get_spark() -> SparkSession:
    app_name = "KafkaElectionAnalysis"
    cluster_manager="spark://164.92.85.68:7077"
    packages = "164.92.85.68"
    
    """_summary_

    Args:
        app_name (str): Name of the spark application
        jars (str): List of jars needs to be installed before running spark application
        cluster_manager (str, optional): cluster manager Defaults to "local[*]".

    Returns:
        SparkSession: returns spark session
    """
    jars = ",".join(packages)
     # Initialize SparkSession
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.jars.packages", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0"
        )
        .master("spark://164.92.85.68:7077")
        .getOrCreate()
    )
    return spark
# create_or_get_spark()



def define_read_Stream():
# Define schemas for Kafka topics
    spark = create_or_get_spark()
    # print(spark)
    vote_schema = StructType([
            StructField("voter_id", StringType(), True),
            StructField("candidate_id", StringType(), True),
            StructField("voting_time", TimestampType(), True),
            StructField("voter_name", StringType(), True),
            StructField("party_affiliation", StringType(), True),
            StructField("biography", StringType(), True),
            StructField("campaign_platform", StringType(), True),
            StructField("photo_url", StringType(), True),
            StructField("candidate_name", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("registration_number", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postcode", StringType(), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("cell_number", StringType(), True),
            StructField("picture", StringType(), True),
            StructField("registered_age", IntegerType(), True),
            StructField("vote", IntegerType(), True)
        ])
    # print(vote_schema)

    #  # Read data from Kafka 'votes_topic' and process it
    KAFKA_BOOTSTRAP_SERVER = "164.92.85.68" + ":9092"
    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", "voters_topic") \
        .option("startingOffsets", "earliest") \
        .option(
            "key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer",
        ) \
        .option(
            "value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer",
        ) \
        .option("failOnDataLoss", False) \
        .load() 
        # .selectExpr("CAST(value AS STRING)") \
        # .select(from_json(col("value"), vote_schema).alias("data")) \
        # .select("data.*")
    print(votes_df)
define_read_Stream()

#  # Data preprocessing: type casting and watermarking
# votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
#         .withColumn('vote', col('vote').cast(IntegerType()))
# enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")