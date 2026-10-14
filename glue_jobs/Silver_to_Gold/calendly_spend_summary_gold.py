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
SPEND_PATH = (
    "s3://insightflow-silver-robert/calendly/spend/"
)

OUTPUT_PATH = (
    "s3://insightflow-gold-robert/calendly/spend_summary/"
)

# --------------------------------------------------
# Read Silver Spend
# --------------------------------------------------
spend_df = spark.read.parquet(SPEND_PATH)

print(f"Spend Rows: {spend_df.count()}")

# --------------------------------------------------
# Spend Summary
# --------------------------------------------------
spend_summary_df = (
    spend_df
    .groupBy(
        "spend_date",
        "channel"
    )
    .agg(
        F.sum("spend")
        .alias("total_spend")
    )
)

# --------------------------------------------------
# Refresh Date
# --------------------------------------------------
spend_summary_df = (
    spend_summary_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    spend_summary_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)
print(
    f"Successfully wrote Gold table to {OUTPUT_PATH}"
)

job.commit()