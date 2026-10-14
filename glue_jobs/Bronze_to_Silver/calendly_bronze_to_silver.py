import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, input_file_name, explode_outer
from datetime import datetime

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bronze_path = "s3://insightflow-bronze-robert/calendly_events/"
silver_path = "s3://insightflow-silver-robert/calendly/"

df = spark.read.json(bronze_path)

# Flatten questions_and_answers (take first Q&A)
qa = explode_outer(col("payload.questions_and_answers")).alias("qa")

df_flat = (
    df
    .withColumn("qa", qa)
    .select(
        col("event").alias("event_type"),
        col("created_at").alias("webhook_created_at"),
        col("created_by").alias("webhook_created_by"),

        # Invitee fields
        col("payload.name").alias("invitee_name"),
        col("payload.email").alias("invitee_email"),
        col("payload.first_name").alias("invitee_first_name"),
        col("payload.last_name").alias("invitee_last_name"),
        col("payload.timezone").alias("invitee_timezone"),
        col("payload.uri").alias("invitee_uri"),
        col("payload.invitee_scheduled_by").alias("invitee_scheduled_by"),

        # Event fields
        col("payload.event").alias("event_uri"),
        col("payload.scheduled_event.name").alias("event_name"),
        col("payload.scheduled_event.start_time").alias("event_start_time"),
        col("payload.scheduled_event.end_time").alias("event_end_time"),
        col("payload.scheduled_event.event_type").alias("event_type_uri"),
        col("payload.scheduled_event.status").alias("event_status"),

        # Q&A fields
        col("qa.question").alias("question_1"),
        col("qa.answer").alias("answer_1"),

        # Metadata
        input_file_name().alias("raw_file_key"),
        lit(datetime.utcnow().isoformat()).alias("ingestion_timestamp")
    )
)

df_flat.write.mode("overwrite").parquet(silver_path)

job.commit()