import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------- 1. CRM JSON SCHEMA (custom.* fields are plain strings) ----------

event_data_schema = StructType([
    StructField("date_updated", StringType(), True),
    StructField("created_by_name", StringType(), True),
    StructField("id", StringType(), True),
    StructField("status_label", StringType(), True),
    StructField("source", StringType(), True),
    StructField("updated_by", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("status_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("date_created", StringType(), True),
    StructField("organization_id", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("addresses", ArrayType(StringType()), True),
    StructField("updated_by_name", StringType(), True),
    StructField("contact_ids", ArrayType(StringType()), True),

    # custom.* fields as plain strings
    StructField("custom.cf_JHukP9d2Q7x9GqAONrvYsyZPRPTB3YVrd8FNrytULm8", StringType(), True),
    StructField("custom.cf_o9EOtES0A8In9L4qeP8a749RrrjvuDyf7m4TIYVAWsK", StringType(), True),
    StructField("custom.cf_am3UgCUhyM5iNDtAPL84enDjUrZx1JsyVZ9uD9TbYwG", StringType(), True),
    StructField("custom.cf_DeLLGkRULjyldwA4CA5umg5YrhOT6y0Ex44eycTadS2", StringType(), True),
    StructField("custom.cf_7ZWtXzOYPfLduhXb8fCM0m6L2KawUuubVHq12dKgvI9", StringType(), True),
    StructField("custom.cf_IrgdpYtm7icLYVgVe2luKRrGI5xXsdBCKL7z5sEtTXJ", StringType(), True),
    StructField("custom.cf_Wdliv5pLvi89rA68CDtcsHLKeiIuwJp8fpHHaSIm5eJ", StringType(), True),
    StructField("custom.cf_CbimGr7Rok3jLexgIykOOyRbMYGGHuAnIGIZ4rTXEIs", StringType(), True),
    StructField("custom.cf_jr2d8YqWeVF2GF8UqYTLjnOhCF9N61vUOnTTHrwPcPz", StringType(), True),
    StructField("custom.cf_HR0ji6nGFk5vMcsINNzeo3azcmlNtkfXxbbnQPvFfqN", StringType(), True),
])

event_schema = StructType([
    StructField("id", StringType(), True),
    StructField("date_created", StringType(), True),
    StructField("date_updated", StringType(), True),
    StructField("organization_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("api_key_id", StringType(), True),
    StructField("oauth_client_id", StringType(), True),
    StructField("oauth_scope", StringType(), True),
    StructField("object_type", StringType(), True),
    StructField("object_id", StringType(), True),
    StructField("lead_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("changed_fields", ArrayType(StringType()), True),
    StructField("meta", StringType(), True),
    StructField("data", event_data_schema, True),
    StructField("previous_data", StringType(), True),
])

root_schema = StructType([
    StructField("subscription_id", StringType(), True),
    StructField("event", event_schema, True),
])

# ---------- 2. READ JSON FROM BRONZE ----------

bronze_path = "s3://insightflow-bronze-robert/crm_events/"

raw_df = spark.read.schema(root_schema).json(bronze_path)

# ---------- 3. FLATTEN ----------

df = raw_df.select(
    "subscription_id",
    F.col("event.id").alias("event_id"),
    F.col("event.date_created").alias("event_date_created"),
    F.col("event.date_updated").alias("event_date_updated"),
    F.col("event.organization_id").alias("event_organization_id"),
    F.col("event.user_id").alias("event_user_id"),
    F.col("event.request_id").alias("event_request_id"),
    F.col("event.object_type").alias("event_object_type"),
    F.col("event.object_id").alias("event_object_id"),
    F.col("event.lead_id").alias("lead_id"),
    F.col("event.action").alias("event_action"),

    F.col("event.data.display_name").alias("display_name"),
    F.col("event.data.status_label").alias("status_label"),
    F.col("event.data.status_id").alias("status_id"),
    F.col("event.data.source").alias("lead_source"),
    F.col("event.data.contact_ids").alias("contact_ids"),
    F.col("event.data.date_created").alias("lead_date_created"),
    F.col("event.data.date_updated").alias("lead_date_updated"),

    # custom fields
    F.col("event.data.`custom.cf_JHukP9d2Q7x9GqAONrvYsyZPRPTB3YVrd8FNrytULm8`").alias("cf_timeline"),
    F.col("event.data.`custom.cf_o9EOtES0A8In9L4qeP8a749RrrjvuDyf7m4TIYVAWsK`").alias("cf_salary_range"),
    F.col("event.data.`custom.cf_am3UgCUhyM5iNDtAPL84enDjUrZx1JsyVZ9uD9TbYwG`").alias("cf_source_funnel")
)

# ---------- 4. WRITE TO SILVER (crm_events) ----------

silver_path = "s3://insightflow-silver-robert/crm_events/"

df.write.mode("overwrite").format("parquet").save(silver_path)

job.commit()