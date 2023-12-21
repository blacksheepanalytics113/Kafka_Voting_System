"""Script for processing kafka streams 
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
            .appName("ElectionAnalysis")
             .master("local[*]")  # Use local Spark execution with all available cores
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")  # Spark-Kafka integration
             .config("spark.jars",
                     "C:/Users/user/Desktop/Kafka_Voting_System/jar/postgresql-42.7.1")  # PostgreSQL driver
             .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
             .getOrCreate())
print(spark)

             