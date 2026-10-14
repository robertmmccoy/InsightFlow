import requests
import json
import boto3
from datetime import datetime

BASE_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/"
)

INDEX_URL = BASE_URL + "file_index.json"

BRONZE_BUCKET = "insightflow-bronze-robert"
BRONZE_KEY = "calendly_events/spend/bronze.ndjson"

s3 = boto3.client("s3")

# --------------------
# Read index
# --------------------
index_data = requests.get(INDEX_URL).json()

files = index_data["files"]

bronze_records = []

# --------------------
# Read each spend file
# --------------------
for file_name in files:

    file_url = BASE_URL + file_name

    spend_rows = requests.get(file_url).json()

    for row in spend_rows:

        bronze_records.append({
            "spend_date": row["date"],
            "channel": row["channel"],
            "spend": row["spend"],
            "source_file": file_name,
            "ingested_at": datetime.utcnow().isoformat()
        })

# --------------------
# Write NDJSON
# --------------------
ndjson = "\n".join(
    json.dumps(record)
    for record in bronze_records
)

s3.put_object(
    Bucket=BRONZE_BUCKET,
    Key=BRONZE_KEY,
    Body=ndjson
)

print(
    f"Wrote {len(bronze_records)} spend rows"
)