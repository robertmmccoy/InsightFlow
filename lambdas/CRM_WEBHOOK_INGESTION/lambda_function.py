import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')

BRONZE_BUCKET = "insightflow-bronze-robert"   # bucket name
BRONZE_PREFIX = "crm_events/"          # folder inside the bucket

def lambda_handler(event, context):
    try:
        print("EVENT RAW:", event)
        print("BODY RAW:", event.get("body"))

        # 1. Parse incoming CRM webhook payload
        body = event.get("body", "{}")
        data = json.loads(body)

        # 2. Create unique filename
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        file_id = str(uuid.uuid4())
        key = f"{BRONZE_PREFIX}{timestamp}_{file_id}.json"

        # 3. Write raw event to S3 Bronze
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        # 4. Return success
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "CRM event stored", "key": key})
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }