from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("user-processing") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Input schema

input_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("session_start", TimestampType()),
    StructField("session_end", TimestampType()),
    StructField("total_amount", DoubleType()),
])

# Read streaming parquet

user_df = spark.readStream \
    .format("parquet") \
    .schema(input_schema) \
    .load("/app/data/output/users")

user_df = user_df.withWatermark("session_start", "30 seconds")

# ================================= AVERAGE ORDER VALUE, AVERAGE PURCHASE SESSION DURATION ===========================

metrics_df = user_df.groupBy(
    window(col("session_start"), "2 minutes").alias("time_window")
).agg(
    sum("total_amount").cast("long").alias("total_amount"),
    sum(
        unix_timestamp("session_end") - unix_timestamp("session_start")
    ).cast("long").alias("total_session_duration_seconds"),

    count("*").cast("long").alias("session_count")
)

metrics_df = metrics_df.select(
    col("time_window.start").alias("window_start"),
    col("total_amount"),
    col("total_session_duration_seconds"),
    col("session_count")
)



# ================================ SESSION SPEND DISTRIBUTION =============================================

# Create histogram buckets

new_session_df = user_df.withColumn(
    "bucket_start",
    ((floor(col("total_amount") / 20)) * 20).cast("int")
).groupBy(
    "bucket_start", 
    window(col("session_start"), "2 minutes").alias("time_window")
).agg(
    count("*").cast("long").alias("session_count")
).withColumn(
    "bucket_end",
    (col("bucket_start") + lit(20)).cast("int")
).withColumn(
    "bucket_label",
    concat(                                                    # concat takes columns, lit creates column with literal value
        col("bucket_start").cast("string"),
        lit("-"),
        col("bucket_end").cast("string")
    )
).select(
    col("time_window.start").alias("window_start"), "bucket_start", "bucket_end", "bucket_label", "session_count"
)

# Start streaming query

query = metrics_df.writeStream \
    .format("parquet") \
    .trigger(processingTime="1 minute") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoint/users/gold/averages/console") \
    .option("path", "/app/data/gold_output/users/averages") \
    .start()


query1 = metrics_df.writeStream \
    .format("console") \
    .trigger(processingTime="1 minute") \
    .outputMode("complete") \
    .option("checkpointLocation", "/app/data/checkpoint/users/gold/averages/parquet") \
    .start()


query2 = new_session_df.writeStream \
    .format("parquet") \
    .option("path", "/app/data/gold_output/users/spend_distribution") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoint/users/gold/spend_distribution") \
    .trigger(processingTime="1 minute") \
    .start()

spark.streams.awaitAnyTermination()