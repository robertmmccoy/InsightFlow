import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
MEDIA_PATH = (
    "s3://insightflow-silver-robert/wistia/media/"
)

ENGAGEMENT_PATH = (
    "s3://insightflow-silver-robert/wistia/engagement/"
)

EVENTS_PATH = (
    "s3://insightflow-silver-robert/wistia/events/"
)

EVENT_SUMMARY_PATH = (
    "s3://insightflow-gold-robert/wistia/event_summary/"
)

OUTPUT_PATH = (
    "s3://insightflow-gold-robert/wistia/video_summary/"
)

# --------------------------------------------------
# Read Data
# --------------------------------------------------
media_df = spark.read.parquet(MEDIA_PATH)

engagement_df = spark.read.parquet(ENGAGEMENT_PATH)

events_df = spark.read.parquet(EVENTS_PATH)

event_summary_df = spark.read.parquet(
    EVENT_SUMMARY_PATH
)

print(f"Media rows: {media_df.count()}")
print(f"Engagement rows: {engagement_df.count()}")
print(f"Events rows: {events_df.count()}")

# --------------------------------------------------
# Top Country Per Video
# --------------------------------------------------
country_counts = (
    events_df
    .groupBy(
        "media_id",
        "country"
    )
    .agg(
        F.count("*").alias("country_events")
    )
)

window_spec = Window.partitionBy(
    "media_id"
).orderBy(
    F.desc("country_events")
)

top_country_df = (
    country_counts
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(
        F.col("rn") == 1
    )
    .drop(
        "rn",
        "country_events"
    )
    .withColumnRenamed(
        "country",
        "top_country"
    )
)

# --------------------------------------------------
# Build Video Summary
# --------------------------------------------------
video_summary_df = (
    media_df
    .join(
        engagement_df,
        "media_id",
        "left"
    )
    .join(
        event_summary_df.select(
            "media_id",
            "video_name",
            "total_events",
            "unique_visitors",
            "avg_percent_viewed"
        ),
        "media_id",
        "left"
    )
    .join(
        top_country_df,
        "media_id",
        "left"
    )
)

# --------------------------------------------------
# Add Refresh Date
# --------------------------------------------------
video_summary_df = (
    video_summary_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Select Final Columns
# --------------------------------------------------
video_summary_df = video_summary_df.select(
    "media_id",
    "video_name",
    "duration",
    "hashed_id",
    "plays",
    "loads",
    "play_rate",
    "hours_watched",
    "engagement_score",
    "total_events",
    "unique_visitors",
    "avg_percent_viewed",
    "top_country",
    "refresh_date"
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    video_summary_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Successfully wrote {OUTPUT_PATH}"
)

job.commit()