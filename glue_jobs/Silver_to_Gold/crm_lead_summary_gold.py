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
    "s3://insightflow-gold-robert/crm/lead_summary/"
)

# --------------------------------------------------
# Read Silver CRM
# --------------------------------------------------
crm_df = spark.read.parquet(CRM_PATH)

print(f"CRM Rows: {crm_df.count()}")

# --------------------------------------------------
# Lead Summary
# --------------------------------------------------
lead_summary_df = (
    crm_df
    .groupBy(
        "lead_source",
        "status_label",
        "cf_source_funnel",
        "cf_timeline",
        "cf_salary_range"
    )
    .agg(
        F.countDistinct("lead_id").alias(
            "total_leads"
        ),
        F.max("lead_date_created").alias(
            "latest_lead_created"
        ),
        F.max("lead_date_updated").alias(
            "latest_lead_updated"
        )
    )
)

# --------------------------------------------------
# Refresh Date
# --------------------------------------------------
lead_summary_df = (
    lead_summary_df
    .withColumn(
        "refresh_date",
        F.current_date()
    )
)

# --------------------------------------------------
# Write Gold
# --------------------------------------------------
(
    lead_summary_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(
    f"Successfully wrote Gold table to {OUTPUT_PATH}"
)

job.commit()