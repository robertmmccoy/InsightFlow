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
CRM_PATH = (
    "s3://insightflow-silver-robert/crm_events/"
)

OUTPUT_PATH = (
    "s3://insightflow-gold-robert/crm/status_summary/"
)

# --------------------------------------------------
# Read Silver CRM
# --------------------------------------------------
crm_df = spark.read.parquet(CRM_PATH)
print(f"CRM Rows: {crm_df.count()}")

# --------------------------------------------------
# Status Summary
# --------------------------------------------------
status_summary_df = (
    crm_df
    .groupBy("status_label")
    .agg(
        F.countDistinct("lead_id")
        .alias("total_leads")
    )
)

# --------------------------------------------------
# Refresh Date
# --------------------------------------------------
status_summary_df = (
    status_summary_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    status_summary_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Successfully wrote Gold table to {OUTPUT_PATH}"
)

job.commit()