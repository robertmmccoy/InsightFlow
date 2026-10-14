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
CALENDLY_PATH = (
    "s3://insightflow-silver-robert/calendly/"
)

OUTPUT_PATH = (
    "s3://insightflow-gold-robert/calendly/daily_booking_summary/"
)

# --------------------------------------------------
# Read Silver Calendly
# --------------------------------------------------
calendly_df = spark.read.parquet(CALENDLY_PATH)

print(f"Calendly Rows: {calendly_df.count()}")

# --------------------------------------------------
# Daily Booking Summary
# --------------------------------------------------
daily_booking_df = (
    calendly_df
    .withColumn(
        "booking_date",
        F.to_date("event_start_time")
    )
    .groupBy("booking_date")
    .agg(
        F.count("*").alias("total_bookings")
    )
)

# --------------------------------------------------
# Refresh Date
# --------------------------------------------------
daily_booking_df = (
    daily_booking_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    daily_booking_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Successfully wrote Gold table to {OUTPUT_PATH}"
)

job.commit()