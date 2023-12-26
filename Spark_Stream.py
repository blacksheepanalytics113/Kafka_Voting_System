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



    # Initialize SparkSession
spark = (SparkSession.builder
    .appName("KafkaElectionAnalysis") 
    .master("local[1]")   
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")  
    .config("spark.jars",
            "C:/Users/user/Desktop/Kafka_Voting_System/jar/postgresql-42.7.1.jar")
    .config("spark.sql.adaptive.enabled", "false")
    # .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.local.LocalFs")
    # .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.local.LocalFs")
    .getOrCreate())
# print(spark)
# print("First SparkContext:" {spark.keys()})



# Define schemas for Kafka topics
# vote_schema = StructType([
#         StructField("voter_id", StringType(), True),
#         StructField("candidate_id", StringType(), True),
#         StructField("voting_time", TimestampType(), True),
#         StructField("voter_name", StringType(), True),
#         StructField("party_affiliation", StringType(), True),
#         StructField("biography", StringType(), True),
#         StructField("campaign_platform", StringType(), True),
#         StructField("photo_url", StringType(), True),
#         StructField("candidate_name", StringType(), True),
#         StructField("date_of_birth", StringType(), True),
#         StructField("gender", StringType(), True),
#         StructField("nationality", StringType(), True),
#         StructField("registration_number", StringType(), True),
#         StructField("address", StructType([
#             StructField("street", StringType(), True),
#             StructField("city", StringType(), True),
#             StructField("state", StringType(), True),
#             StructField("country", StringType(), True),
#             StructField("postcode", StringType(), True)
#         ]), True),
#         StructField("email", StringType(), True),
#         StructField("phone_number", StringType(), True),
#         StructField("cell_number", StringType(), True),
#         StructField("picture", StringType(), True),
#         StructField("registered_age", IntegerType(), True),
#         StructField("vote", IntegerType(), True)
#     ])
# # print(vote_schema)

#  # Read data from Kafka 'votes_topic' and process it
# KAFKA_BOOTSTRAP_SERVER = "164.92.85.68" + ":9092"
# votes_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
#     .option("subscribe", "voters_topic") \
#     .option("startingOffsets", "earliest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), vote_schema).alias("data")) \
#     .select("data.*")
# print(votes_df)



#  # Data preprocessing: type casting and watermarking
# votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
#         .withColumn('vote', col('vote').cast(IntegerType()))
# enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")