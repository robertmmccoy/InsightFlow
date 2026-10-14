import json
import boto3
import os
import uuid

stepfunctions = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

def lambda_handler(event, context):
    executions_started = 0
    print(json.dumps(event))
    for record in event["Records"]:
        # --------------------------------------------------
        # Parse SQS Message
        # --------------------------------------------------
        body = json.loads(record["body"])

        s3_record = body["Records"][0]
    
        bucket_name = (
            s3_record["s3"]["bucket"]["name"]
        )

        object_key = (
            s3_record["s3"]["object"]["key"]
        )

        print(
            f"Bucket: {bucket_name}"
        )

        print(
            f"Object Key: {object_key}"
        )

        # --------------------------------------------------
        # Step Function Payload
        # --------------------------------------------------
        payload = {
            "bucket_name": bucket_name,
            "object_key": object_key
        }

        # --------------------------------------------------
        # Start Step Function
        # --------------------------------------------------
        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=str(uuid.uuid4()),
            input=json.dumps(payload)
        )

        print(
            f"Started Execution: "
            f"{response['executionArn']}"
        )

        executions_started += 1

    return {
        "statusCode": 200,
        "executions_started": executions_started
    }