from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    TimestampType, DoubleType
)
from pyspark.sql.functions import (
    col, count, sum, round,
    from_json
)



spark = SparkSession.builder \
    .appName("product_summary") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


source_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "clickstream") \
    .load()



event_schema = StructType([
    StructField("event_id",       IntegerType()),
    StructField("user_id",        IntegerType()),
    StructField("event_type",     StringType()),
    StructField("product_id",     IntegerType()),
    StructField("time_generated", TimestampType()),
    StructField("time_received",  TimestampType()),
])



events_df = source_df.selectExpr(
    "CAST(value AS STRING) as json_string"
).select(
    from_json(col("json_string"), event_schema).alias("data")
).select("data.*") \
 .withWatermark("time_generated", "10 seconds") \
 .filter(col("event_type") == "purchase")



product_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("name",       StringType()),
    StructField("category",   StringType()),
    StructField("cost",       DoubleType()),
])

products_df = spark.read \
    .format("csv") \
    .schema(product_schema) \
    .option("header", "true") \
    .load("/app/products.csv")



joined_df = events_df.alias("e").join(
    products_df.alias("p"),
    on="product_id",
    how="inner"
)


def write_summaries(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    product_summary_df = batch_df.groupBy(
        col("product_id"),
        col("name"),
        col("category"),
    ).agg(
        count("event_id").alias("num_purchases"),
        round(sum("cost"), 2).alias("revenue")
    )

    category_summary_df = product_summary_df.groupBy(
        col("category")
    ).agg(
        count("product_id").alias("num_products"),
        sum("num_purchases").alias("num_purchases"),
        round(sum("revenue"), 2).alias("revenue")
    ).select(
        col("category"),
        col("num_products"),
        col("num_purchases"),
        col("revenue")
    )


    print(f"\n========== BATCH {batch_id} — Product Summary ==========\n")
    product_summary_df.show(truncate=False)

    print(f"\n========== BATCH {batch_id} — Category Summary ==========\n")
    category_summary_df.show(truncate=False)

   
    product_summary_df.write \
        .mode("append") \
        .parquet("/app/data/output/product")

    category_summary_df.write \
        .mode("append") \
        .parquet("/app/data/output/category")


query = joined_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_summaries) \
    .option(
        "checkpointLocation",
        "/app/data/checkpoint/product"
    ) \
    .start()

query.awaitTermination()