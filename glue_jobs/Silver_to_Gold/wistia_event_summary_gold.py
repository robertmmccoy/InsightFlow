import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# --------------------------------------------------
# Glue Boilerplate
# --------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Paths
# --------------------------------------------------
EVENTS_PATH = (
    "s3://insightflow-silver-robert/wistia/events/"
)

MEDIA_PATH = (
    "s3://insightflow-silver-robert/wistia/media/"
)

OUTPUT_PATH = (
    "s3://insightflow-gold-robert/wistia/event_summary/"
)

# --------------------------------------------------
# Read Silver Tables
# --------------------------------------------------
events_df = spark.read.parquet(EVENTS_PATH)
media_df = spark.read.parquet(MEDIA_PATH)

print(f"Events rows: {events_df.count()}")

print(f"Media rows: {media_df.count()}")

# --------------------------------------------------
# Aggregate Event Metrics
# --------------------------------------------------
event_summary_df = (
    events_df
    .groupBy(
        "media_id",
        "media_name"
    )
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("visitor_key").alias(
            "unique_visitors"
        ),
        F.avg("percent_viewed").alias(
            "avg_percent_viewed"
        ),
        F.max("percent_viewed").alias(
            "max_percent_viewed"
        ),
        F.min("percent_viewed").alias(
            "min_percent_viewed"
        )
    )
    .withColumnRenamed(
        "media_name",
        "video_name"
    )
)

# --------------------------------------------------
# Add Refresh Date
# --------------------------------------------------
event_summary_df = (
    event_summary_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Reorder Columns
# --------------------------------------------------
event_summary_df = event_summary_df.select(
    "media_id",
    "video_name",
    "total_events",
    "unique_visitors",
    "avg_percent_viewed",
    "max_percent_viewed",
    "min_percent_viewed",
    "refresh_date"
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    event_summary_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Successfully wrote Gold table to {OUTPUT_PATH}"
)

job.commit()