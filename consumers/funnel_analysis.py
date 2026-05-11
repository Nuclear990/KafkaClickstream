from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("funnel") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


source_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "clickstream") \
    .load()

schema = StructType([
    StructField("event_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("product_id", IntegerType()),
    StructField("time_generated", StringType()),
    StructField("time_received", StringType())
])

events_df = source_df.selectExpr(
    "CAST(value AS STRING) as json_data"
).select(
    from_json(col("json_data"), schema).alias("data")
).select("data.*")