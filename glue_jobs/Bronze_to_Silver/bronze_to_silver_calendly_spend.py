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
# S3 Paths
# --------------------------------------------------
BRONZE_PATH = (
    "s3://insightflow-bronze-robert/"
    "calendly_events/spend/"
)

SILVER_PATH = (
    "s3://insightflow-silver-robert/"
    "calendly/spend/"
)

# --------------------------------------------------
# Read Bronze
# --------------------------------------------------
bronze_df = (
    spark.read
    .json(BRONZE_PATH)
)

print(f"Bronze Row Count: {bronze_df.count()}")

# --------------------------------------------------
# Deduplicate
#
# Business Key:
# spend_date + channel
#
# Keep newest source file
# --------------------------------------------------
window_spec = Window.partitionBy(
    "spend_date",
    "channel"
).orderBy(
    F.col("source_file").desc()
)

silver_df = (
    bronze_df
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(
    F.col("rn") == 1
    )
    .drop("rn")
)

print(f"Silver Row Count: {silver_df.count()}")
# --------------------------------------------------
# Reorder Columns
# --------------------------------------------------
silver_df = silver_df.select(
    "spend_date",
    "channel",
    "spend",
    "source_file",
    "ingested_at"
)
# --------------------------------------------------
# Write Silver
# --------------------------------------------------
(
silver_df.write
    .mode("overwrite")
    .parquet(SILVER_PATH)
)

print(
f"Successfully wrote Silver data to: {SILVER_PATH}"
)

job.commit()