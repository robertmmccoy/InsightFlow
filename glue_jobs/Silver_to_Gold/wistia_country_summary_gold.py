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

OUTPUT_PATH = (
    "s3://insightflow-gold-robert/wistia/country_summary/"
)

# --------------------------------------------------
# Read Silver Events
# --------------------------------------------------
events_df = spark.read.parquet(EVENTS_PATH)

print(f"Events rows: {events_df.count()}")

# --------------------------------------------------
# Country Summary
# --------------------------------------------------
country_summary_df = (
    events_df
    .groupBy("country")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("visitor_key").alias(
            "unique_visitors"
        ),
        F.countDistinct("media_id").alias(
            "videos_watched"
        ),
        F.avg("percent_viewed").alias(
            "avg_percent_viewed"
        ),
        F.max("percent_viewed").alias(
            "max_percent_viewed"
        )
    )
)

country_summary_df = (
    country_summary_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    country_summary_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Successfully wrote Gold table to {OUTPUT_PATH}"
)

job.commit()