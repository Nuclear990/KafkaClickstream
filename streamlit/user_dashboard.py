import streamlit as st
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import builtins

st.set_page_config(
    page_title="User Dashboard",
    layout="wide"
)
# st.autorefresh(interval=60000, key="refresh")

st.title("User Dashboard")
st.caption("Updates every minute • Displays data from the last 2 hours")
st.markdown("""
<hr style="margin-bottom: 2px;">
<hr style="margin-top: 0px;">
""", unsafe_allow_html=True)

# Spark session

@st.cache_resource
def get_spark():
    spark = SparkSession.builder \
        .appName("streamlit-dashboard") \
        .master("local[2]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark

spark = get_spark()

def read(schema, source):
    df = spark.read \
        .schema(schema) \
        .parquet(source)
    
    return None if df.limit(1).count() == 0 else df
# ================================= AVERAGE ORDER VALUE, AVERAGE PURCHASE SESSION DURATION ===========================

st.header("Overall Metrics")

averages_schema = StructType([
    StructField("window_start", TimestampType()),
    StructField("total_amount", LongType()),
    StructField("total_session_duration_seconds", LongType()),
    StructField("session_count", LongType())
])

# Read parquet data
try:
    df = read(averages_schema, "/app/data/gold_output/users/averages")

    if df==None:
        st.warning("Waiting for streaming data...")
    else:
        # Keep only data from last 2 hours

        two_hours_ago = datetime.now() - timedelta(hours=2)

        filtered_df = df.filter(
            col("window_start") >= current_timestamp() - expr("INTERVAL 2 HOURS")
        )

        # AOV
        agg_df = filtered_df.agg(
            sum("total_amount").alias("total_amount"),
            sum("session_count").alias("session_count")
        )

        row = agg_df.collect()[0]

        if row["session_count"] > 0:
            aov = builtins.round(row["total_amount"] / row["session_count"],2)
        else:
            aov = 0

        # APSD
        agg_df = filtered_df.agg(
            sum("total_session_duration_seconds").alias("total_session_duration_seconds"),
            sum("session_count").alias("session_count")
        )

        row = agg_df.collect()[0]

        if row["session_count"] > 0:
            apsd = builtins.round(row["total_session_duration_seconds"] / row["session_count"], 2)
        else:
            apsd = 0

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Average Order Value", f"${aov}")

        with col2:
            st.metric("Average Purchase Session Duration", f"{apsd} sec")

        st.markdown("---")


except Exception as e:
    st.warning("Waiting for streaming data...")
    st.error(str(e))


# ================================ SESSION SPEND DISTRIBUTION =============================================

st.header("Session Spend Distribution")


distribution_schema = StructType([
    StructField("window_start", TimestampType()),
    StructField("bucket_start", IntegerType()),
    StructField("bucket_end", IntegerType()),
    StructField("bucket_label", StringType()),
    StructField("session_count", LongType())
])

# Read parquet data
try:
    df = read(distribution_schema, "/app/data/gold_output/users/spend_distribution")

    if df==None:
        st.warning("Waiting for streaming data...")
    else:
        # Keep only data from last 2 hours

        two_hours_ago = datetime.now() - timedelta(hours=2)

        filtered_df = df.filter(
            col("window_start") >= current_timestamp() - expr("INTERVAL 2 HOURS")
        )

        # Re-aggregate bucket counts across all windows

        histogram_df = filtered_df.groupBy(
            "bucket_start",
            "bucket_end",
            "bucket_label"
        ).agg(
            sum("session_count").alias("session_count")
        )

        # Convert to pandas

        pdf = histogram_df.toPandas()

        # Empty state

        if pdf.empty:
            st.warning("No spend distribution data available from the last 2 hours.")
        else:
            # Create histogram/bar chart

            min_bucket = pdf["bucket_start"].min()
            max_bucket = pdf["bucket_start"].max()

            all_buckets = pd.DataFrame({
                "bucket_start": range(min_bucket, max_bucket + 20, 20)
            })

            all_buckets["bucket_end"] = all_buckets["bucket_start"] + 20

            all_buckets["bucket_label"] = (
                all_buckets["bucket_start"].astype(str)
                + "-"
                + all_buckets["bucket_end"].astype(str)
            )

            pdf = all_buckets.merge(
                pdf,
                on=["bucket_start", "bucket_end", "bucket_label"],
                how="left"
            )

            pdf["session_count"] = pdf["session_count"].fillna(0).astype(int)

            pdf = pdf.sort_values("bucket_start")

            fig = px.bar(
                pdf,
                x="bucket_label",
                y="session_count",
                labels={
                    "bucket_label": "Spend Range",
                    "session_count": "Session Count"
                }
            )

            fig.update_layout(
                xaxis_title="Spend Bucket",
                yaxis_title="Number of Sessions",
                bargap=0.05
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

            # Metrics

            total_sessions = int(pdf["session_count"].sum())
            active_buckets = (pdf["session_count"] > 0).sum()

            col1, col2 = st.columns(2)

            with col1:
                st.metric("Total Sessions", total_sessions)

            with col2:
                st.metric("Active Buckets", active_buckets)

            st.markdown("---")

except Exception as e:
    st.warning("Waiting for streaming data...")
    st.error(str(e))