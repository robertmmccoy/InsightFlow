import sys
import json
import boto3
import requests
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
col,
explode_outer
)
# -----------------------------
# Glue Init
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# -----------------------------
# CONFIG
# -----------------------------
API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"
MEDIA_IDS = [
    "8hunphufxp",
    "9k4tbcdfg0"
]
BRONZE_BUCKET = "insightflow-bronze-robert"
BRONZE_KEY = "wistia/bronze/bronze.ndjson"
SILVER_BASE = "s3://insightflow-silver-robert/wistia/"
headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "accept": "application/json",
    "X-Wistia-API-Version": "2026-07"
}
s3 = boto3.client("s3")
# -----------------------------
# Helpers
# -----------------------------
def safe_json(response):
    try:
        return response.json()
    except Exception:
        return {}
# -----------------------------
# Metadata
# -----------------------------
def fetch_media_metadata(media_id):
    url = f"https://api.wistia.com/modern/medias/{media_id}"
    r = requests.get(url, headers=headers)
    return safe_json(r)
# -----------------------------
# Engagement Summary
# -----------------------------
def fetch_media_engagement(media_id):
    url = f"https://api.wistia.com/modern/stats/medias/{media_id}"
    r = requests.get(url, headers=headers)
    return safe_json(r)
# -----------------------------
# Events
# -----------------------------
#def fetch_media_events(media_id):
#    all_events = []
#    for page in [1, 2, 3]:
#        url = "https://api.wistia.com/modern/stats/events"
#        params = {
#            "media_id": media_id,
#            "page": page,
#            "per_page": 100
#        }
#        r = requests.get(
#            url,
#            headers=headers,
#            params=params
#        )
#        data = safe_json(r)
#        print(
#            f"MEDIA={media_id} "
#            f"PAGE={page} "
#            f"TYPE={type(data)} "
#            f"RECORDS={len(data)}"
#        )
#        if isinstance(data, list):
#            all_events.extend(data)
#            print(
#            f"MEDIA={media_id} FINAL_TOTAL={len(all_events)}"
#            )
#    return all_events

def fetch_media_events(media_id):
    all_events = []
    page = 1
    while True:
        url = "https://api.wistia.com/modern/stats/events"
        params = {
        "media_id": media_id,
        "page": page,
        "per_page": 100
        }
  
        r = requests.get(
        url,
        headers=headers,
        params=params
        )
        
#        print(f"MEDIA={media_id} PAGE={page}")
        
        data = safe_json(r)
        
        if isinstance(data, list):
            print(f"RECORDS={len(data)}")

        # Events API returns LIST
        if not isinstance(data, list):
            break
        if len(data) == 0:
            break
        all_events.extend(data)
        
#        print(f"TOTAL_EVENTS={len(all_events)}")
        
        page += 1
        
    return all_events
# -----------------------------
# Bronze Extraction
# -----------------------------
bronze_records = []
for media_id in MEDIA_IDS:
    metadata = fetch_media_metadata(media_id)
    engagement = fetch_media_engagement(media_id)
    events = fetch_media_events(media_id)
    bronze_records.append({
    "media_id": media_id,
    "metadata": metadata,
    "engagement": engagement,
    "events": events,
    "ingested_at": datetime.utcnow().isoformat()
})
# -----------------------------
# Write Bronze NDJSON
# -----------------------------
    ndjson = "\n".join(
    json.dumps(record)
    for record in bronze_records
    )
    s3.put_object(
    Bucket=BRONZE_BUCKET,
    Key=BRONZE_KEY,
    Body=ndjson
)
bronze_path = f"s3://{BRONZE_BUCKET}/{BRONZE_KEY}"
# -----------------------------
# Read Bronze
# -----------------------------
df = spark.read.option(
    "mode",
    "PERMISSIVE"
).json(bronze_path)
# ==================================================
# SILVER MEDIA
# ==================================================
df_media = df.select(
    col("media_id"),
    col("metadata.name").alias("title"),
    col("metadata.hashed_id").alias("hashed_id"),
    col("metadata.duration").alias("duration"),
    col("metadata.created").alias("created_at"),
    col("metadata.folder.name").alias("project_name"),
    col("ingested_at")
)
df_media.write.mode(
"overwrite"
).parquet(
SILVER_BASE + "media/"
)
# ==================================================
# SILVER ENGAGEMENT
# ==================================================
df_engagement = df.select(
    col("media_id"),
    col("engagement.play_count").alias("plays"),
    col("engagement.load_count").alias("loads"),
    col("engagement.play_rate").alias("play_rate"),
    col("engagement.hours_watched").alias("hours_watched"),
    col("engagement.visitors").alias("visitors"),
    col("engagement.engagement").alias("engagement_score"),
    col("ingested_at")
)
df_engagement.write.mode(
"overwrite"
).parquet(
SILVER_BASE + "engagement/"
)
# ==================================================
# SILVER EVENTS
# ==================================================
df_events = df.withColumn(
    "event",
    explode_outer(col("events"))
)
df_events = df_events.select(
    col("media_id"),
    col("event.event_key").alias("event_key"),
    col("event.visitor_key").alias("visitor_key"),
    col("event.media_id").alias("event_media_id"),
    col("event.media_name").alias("media_name"),
    col("event.percent_viewed").alias("percent_viewed"),
    col("event.ip").alias("ip"),
    col("event.country").alias("country"),
    col("event.region").alias("region"),
    col("event.city").alias("city"),
    col("event.received_at").alias("event_timestamp"),
    col("ingested_at")
)
df_events.write.mode(
"overwrite"
).parquet(
SILVER_BASE + "events/"
)
job.commit()