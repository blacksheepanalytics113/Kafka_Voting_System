from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# import pyspark
#
# print(pyspark.__version__) # to check the version of pyspark

if __name__ == "__main__":
    # Initialize SparkSession
    spark = (SparkSession.builder
         .appName("TechKafkaElectionAnalysis")
         .master("spark://164.92.85.68:7077")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
         .config("spark.jars",
                     "C:/Users/user/Desktop/Kafka_Voting_System/jar/postgresql-42.7.1.jar")
         .getOrCreate())
   

    # Define schemas for Kafka topics
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

    # Read data from Kafka 'votes_topic' and process it
    # votes_df = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "votes_topic") \
    #     .option("startingOffsets", "earliest") \
    #     .load() \
    #     .selectExpr("CAST(value AS STRING)") \
    #     .select(from_json(col("value"), vote_schema).alias("data")) \
    #     .select("data.*")