import json
import boto3
import urllib.request

s3 = boto3.client("s3")
sns = boto3.client("sns")

OUTPUT_BUCKET = "insightflow-silver-robert"
OUTPUT_PREFIX = "crm_enriched"

TOPIC_ARN = (
"arn:aws:sns:us-east-1:403722869877:crm-new-lead-alerts"
)

def lambda_handler(event, context):
    # -----------------------------------
    # Input from Step Function
    # -----------------------------------
    bucket_name = event["bucket_name"]
    object_key = event["object_key"]

    print(f"Reading {bucket_name}/{object_key}")

    # -----------------------------------
    # Read CRM Event File
    # -----------------------------------

    crm_response = s3.get_object(
        Bucket=bucket_name,
        Key=object_key
    )

    crm_json = json.loads(
        crm_response["Body"]
        .read()
    .decode("utf-8")
    )

    crm_event = crm_json["event"]
    crm_data = crm_event["data"]

    lead_id = crm_event["lead_id"]
    
    # -----------------------------------
    # Lead Owner Lookup
    # -----------------------------------
    
    # print(f"Lead ID: {lead_id}")

    owner_url = (
        "https://dea-lead-owner.s3.us-east-1.amazonaws.com/"
        f"{lead_id}.json"
    )

    # print(f"Lookup URL: {owner_url}")
    try:
        with urllib.request.urlopen(owner_url) as response:

            owner_json = json.loads(
            response.read().decode("utf-8")
            )
    except HTTPError:

        print(
            f"Lead owner file not available yet. "
            f"Status Code: {e.code}"
        )

        raise Exception(
        "OWNER_FILE_NOT_READY"
        )

    # -----------------------------------
    # Create Final Enriched Record
    # -----------------------------------

    enriched_record = {

        "lead_id": lead_id,

        "display_name":
            crm_data.get("display_name"),

        "status_label":
            crm_data.get("status_label"),

        "date_created":
            crm_data.get("date_created"),

        "lead_email":
            owner_json.get("lead_email"),

        "lead_owner":
            owner_json.get("lead_owner"),

        "funnel":
        owner_json.get("funnel")
    }

    # -----------------------------------
    # Write Enriched JSON
    # -----------------------------------

    output_key = (
        f"{OUTPUT_PREFIX}/"
        f"crm_enriched_{lead_id}.json"
    )

    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=output_key,
        Body=json.dumps(
        enriched_record,
        indent=2
        ),
        ContentType="application/json"
    )

    # -----------------------------------
    # Send SNS Email
    # -----------------------------------

    message = f"""
        New Lead Alert

        Name: {enriched_record['display_name']}
        Lead ID: {enriched_record['lead_id']}
        Created Date: {enriched_record['date_created']}
        Label: {enriched_record['status_label']}
        Email: {enriched_record['lead_email']}
        Lead Owner: {enriched_record['lead_owner']}
        Funnel: {enriched_record['funnel']}
    """

    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject="New Lead Alert",
        Message=message
    )

    return {
        "statusCode": 200,
        "body": enriched_record
    }