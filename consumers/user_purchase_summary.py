from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("user_purchase_summary") \
    .master("local[2]") \
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
    StructField("time_generated", TimestampType()),
    StructField("time_received", TimestampType())
])


events_df = source_df.selectExpr(
    "CAST(value AS STRING) as json_string"
).select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")


events_df = events_df.withWatermark(
    "time_generated",
    "10 seconds"
).filter(
    col("event_type") == "purchase"
)


product_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("cost", DoubleType()),
])


products_df = spark.read \
    .format("csv") \
    .schema(product_schema) \
    .option("header", "true") \
    .load("/app/data/static/products.csv")


joined_df = events_df.alias("e").join(
    products_df.alias("p"),
    on="product_id",
    how="inner"
)


summary_df = joined_df.groupBy(
    session_window(col("e.time_generated"), "5 seconds"),
    col("e.user_id")
).agg(
    sum(col("p.cost")).alias("total_amount")
)


clean_df = summary_df.select(
    col("user_id"),
    col("session_window.start").alias("session_start"),
    col("session_window.end").alias("session_end"),
    col("total_amount")
)


def print_batch(batch_df, batch_id):

    print(f"\n========== BATCH {batch_id} ==========\n")

    batch_df.show(
        truncate=False,
        vertical=True
    )

    batch_df.write \
        .mode("append") \
        .parquet("/app/data/output/users")


query = clean_df.writeStream \
    .outputMode("append") \
    .foreachBatch(print_batch) \
    .option(
        "checkpointLocation",
        "/app/data/checkpoint/users/silver"
    ) \
    .start()


query.awaitTermination()