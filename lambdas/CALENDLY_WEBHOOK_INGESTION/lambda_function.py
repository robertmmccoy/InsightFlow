import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')

BRONZE_BUCKET = "insightflow-bronze-robert"
BRONZE_PREFIX = "calendly_events/"

def lambda_handler(event, context):
    try:
        body = event.get("body", "{}")
        data = json.loads(body)

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        file_id = str(uuid.uuid4())
        key = f"{BRONZE_PREFIX}{timestamp}_{file_id}.json"

        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Calendly event stored", "key": key})
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }