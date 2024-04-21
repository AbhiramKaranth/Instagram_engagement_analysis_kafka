from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as spark_sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sqlite3
import pandas as pd
import os

# Define the path for the database
db_directory = os.path.join(os.path.dirname(__file__), '..', 'database')
if not os.path.exists(db_directory):
    os.makedirs(db_directory)
db_path = os.path.join(db_directory, 'engagement_data.db')

# Define a function to write dataframe to SQLite DB
def write_to_sqlite(df, epoch_id):
    # Convert Spark DataFrame to Pandas DataFrame
    # First, extract start and end from window column
    df = df.withColumn("window_start", col("window.start").cast("string")) \
           .withColumn("window_end", col("window.end").cast("string")) \
           .drop("window")
           
    pandas_df = df.toPandas()

    # Connection to SQLite Database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop the table if it exists and recreate
    cursor.execute("DROP TABLE IF EXISTS engagement_data;")
    cursor.execute("""
        CREATE TABLE engagement_data (
            window_start TEXT UNIQUE,
            window_end TEXT UNIQUE,
            team TEXT,
            interactions INTEGER,
            average_followers REAL,
            engagement_rate REAL
        );
    """)

    # Insert Data into SQLite DB
    pandas_df.to_sql('engagement_data', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

# Create the Spark session
spark = SparkSession \
    .builder \
    .appName("WindowedKafkaConsumerEngagement") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Define the schema of the incoming data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("comments", IntegerType(), True),
    StructField("followers", LongType(), True),
    StructField("team", StringType(), True)
])

# Create DataFrame representing the stream of input lines from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fcbarcelona,realmadrid,valenciacf") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Convert timestamp to TimestampType
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Calculate engagement rate
engagement_rate = df \
    .groupBy(window(col("timestamp"), "30 seconds"), "team") \
    .agg(
        spark_sum(col("likes") + col("comments")).alias("interactions"),
        avg("followers").alias("average_followers")
    ) \
    .withColumn("engagement_rate", 100 * col("interactions") / col("average_followers"))

# Start the query and write to SQLite using foreachBatch
query = engagement_rate \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_sqlite) \
    .start()

query.awaitTermination()
