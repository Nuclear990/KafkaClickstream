from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType
)
from pyspark.sql.functions import col, count, from_json

import pandas as pd


# Spark session

spark = SparkSession.builder \
    .appName("funnel-asp") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Read clickstream events from Kafka

source_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "clickstream") \
    .load()

schema = StructType([
    StructField("event_id",        IntegerType()),
    StructField("user_id",         IntegerType()),
    StructField("event_type",      StringType()),
    StructField("product_id",      IntegerType()),
    StructField("time_generated",  TimestampType()),
    StructField("time_received",   TimestampType()),
])


events_df = source_df.selectExpr(
    "CAST(value AS STRING) as json_data"
).select(
    from_json(col("json_data"), schema).alias("data")
).select("data.*")


# Watermark + deduplication

events_df = events_df \
    .withWatermark("time_generated", "30 seconds") \
    .dropDuplicates(["event_id"])


# Output / state schemas for ASP

output_schema = StructType([
    StructField("user_id",              IntegerType()),
    StructField("highest_funnel_stage", IntegerType()),
    StructField("event_time",           TimestampType()),
])

state_schema = StructType([
    StructField("highest_funnel_stage", IntegerType()),
])


# ---------------------------------------------------------------------------
# Event-type → funnel stage mapping
# Using max() semantics so out-of-order / skipped events are handled
# correctly and the stage never regresses within a session.
# ---------------------------------------------------------------------------

STAGE_MAP = {
    "search":       1,
    "page_view":    1,
    "product_view": 2,
    "add_to_cart":  3,
    "purchase":     4,
}


# Arbitrary Stateful Processing

def update_funnel_state(key, pdf_iter, state):
    """
    Maintains the highest funnel stage reached by each user.

    Timeout behaviour
    -----------------
    When a processing-time timeout fires, Spark calls this function
    with an empty iterator and state.hasTimedOut == True.  We remove
    the state entry and yield nothing so the expired session disappears
    cleanly from downstream aggregations.
    """

    user_id = key[0]

    # ── Timeout: session has been inactive for 30 minutes ──────────────────
    if state.hasTimedOut:
        state.remove()
        return                          # yield nothing

    # ── Restore existing state ─────────────────────────────────────────────
    current_stage = state.get[0] if state.exists else 0

    latest_event_time = None

    # ── Process incoming micro-batch rows ──────────────────────────────────
    for pdf in pdf_iter:

        # Sort within the chunk so we process events chronologically
        pdf = pdf.sort_values("time_generated")

        for _, row in pdf.iterrows():

            latest_event_time = row["time_generated"]

            new_stage = STAGE_MAP.get(row["event_type"], 0)

            # Only ever advance; never regress within a session
            current_stage = max(current_stage, new_stage)

    # ── Guard: nothing useful to emit ─────────────────────────────────────
    if latest_event_time is None:
        # Persist whatever state we already had and reset the timeout
        state.update((current_stage,))
        state.setTimeoutDuration(1800)
        return

    # ── Persist updated state ──────────────────────────────────────────────
    state.update((current_stage,))

    # Reset inactivity timer (30 minutes)
    state.setTimeoutDuration(1800)

    # ── Emit one row per user per micro-batch ──────────────────────────────
    yield pd.DataFrame({
        "user_id":              [user_id],
        "highest_funnel_stage": [current_stage],
        "event_time":           [latest_event_time],
    })

# Apply ASP

user_df = events_df \
    .groupBy("user_id") \
    .applyInPandasWithState(
        func=update_funnel_state,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="update",
        timeoutConf="ProcessingTimeTimeout",
    )


# ---------------------------------------------------------------------------
# No second aggregation here.
#
# Chaining a groupBy().agg() onto the ASP output causes Spark to require a
# watermark on the groupBy key column (highest_funnel_stage), which is an
# integer — not a timestamp — so the watermark can never be satisfied and
# the query fails regardless of output mode.
#
# Instead, we stream user_df (one row per user per micro-batch, keyed by
# user_id) directly into foreachBatch and do the count + funnel maths there
# on the plain Pandas/Spark batch, which has no streaming-mode restrictions.
# ---------------------------------------------------------------------------



def safe_div(a, b):
    """Return (a / b) * 100 rounded to 2 d.p., or None when b == 0."""
    if not b:
        return None
    return round((a / b) * 100, 2)


# foreachBatch sink: compute and display funnel analytics

def funnel_analysis(batch_df, batch_id):
    """
    Receives a micro-batch of (user_id, highest_funnel_stage, event_time) rows
    — one row per active user updated in this micro-batch.

    We count users at each stage here (plain batch operation, no streaming
    aggregation restrictions) then compute cumulative funnel metrics.

    highest_funnel_stage records the *furthest* stage a user reached,
    so cumulative counts at each stage are built by summing upward:

        stage_1_total = count(stage=1) + count(stage=2) + count(stage=3) + count(stage=4)
    """

    if batch_df.rdd.isEmpty():
        return

    # Count users per highest_funnel_stage within this batch
    stage_counts_df = batch_df \
        .groupBy("highest_funnel_stage") \
        .agg(count("user_id").alias("user_count"))

    # Build a dict with a safe default of 0 for missing stages
    counts = {
        row["highest_funnel_stage"]: row["user_count"]
        for row in stage_counts_df.collect()
    }

    # Cumulative users at each funnel stage
    stage4 = counts.get(4, 0)
    stage3 = counts.get(3, 0) + stage4
    stage2 = counts.get(2, 0) + stage3
    stage1 = counts.get(1, 0) + stage2

    stage_totals = {1: stage1, 2: stage2, 3: stage3, 4: stage4}

    rows = []
    for stage, total in stage_totals.items():
        next_total = stage_totals.get(stage + 1, 0)
        rows.append((
            stage,
            total,
            safe_div(total - next_total, total),    # drop-off rate
            safe_div(next_total,         total),    # conversion rate to next stage
        ))

    op_df = spark.createDataFrame(
        rows,
        ["funnel_stage", "user_count", "dropoff_rate_pct", "conversion_rate_pct"],
    )

    print(f"\n=== Funnel Analysis — batch {batch_id} ===")
    op_df.show(truncate=False)


# ---------------------------------------------------------------------------
# Start streaming query
#
# user_df comes from applyInPandasWithState with outputMode="update", which
# emits rows only for users that were updated in the current micro-batch —
# exactly what we want.  foreachBatch has no output-mode restriction of its
# own, so we use "append" (the default / least surprising choice here).
# ---------------------------------------------------------------------------

query = user_df.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/funnel_checkpoint") \
    .foreachBatch(funnel_analysis) \
    .start()

query.awaitTermination()