import os
import json
import time
import uuid
import boto3
from botocore.exceptions import ClientError

table_name="NetworkEvents"
local_region = "east"
remote_region = "west"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def update_handler_with_concurrencyCheck(updated_status,updated_version,current_version):
    retVal = false
    try:
        table.update_item(
                        Key={"event_id": event_id},
                        UpdateExpression="SET #st = :st, version = :new_version",
                        ConditionExpression="version = :expected_version",
                        ExpressionAttributeNames={"#st": "status"},
                        ExpressionAttributeValues={
                            ":st": updated_status,
                            ":new_version": updated_version,
                            ":expected_version": current_version
                        }
                    )
         retVal = True
        
    except ClientError as e:
                # Check if it's a conditional check failure (version mismatch)
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    retVal = false
    return retVal
                    
                
    

def read_region(region,key):

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key, ConsistentRead=True)
    return response.get('Item')

def stale_read_handler(key):
    retVal = false
    item_from_local_read = read_region(local_region,key)
    item_from_external_read = read_region(remote_region,key)
    if item_from_local_read.get('version', 0) > item_from_external_read.get('version', 0)
       retVal = true
    return retVal


def lambda_handler(event, context):
    """
    This function:
    1. Processes INSERT/MODIFY events from the DynamoDB stream.
    2. Reads 'event_id', 'event_type', and 'version' from the stream record.
    3. Uses an optimistic concurrency approach to update the item:
       - ConditionExpression ensures 'version' hasn't changed.
       - On conflict, it retries up to max_retries times.
    4. After a successful update, creates a new 'Event B' item.
    """

    for record in event.get("Records", []):
        
        new_image = record["dynamodb"].get("NewImage", {})
        event_id = new_image.get("event_id", {}).get("S")
        event_type = new_image.get("event_type", {}).get("S")
        
        current_version = new_image.get("version", {}).get("N")
        originating_region = new_image.get("region", {}).get("S")
        if event_type == "eventA":
            read_success = stale_read_handler(event_id)
            if(read_success):
                # Prepare the new version and updated data
                updated_version = current_version + 1
                updated_status = "processed"
        
                max_retries = 3
                success = False
                attempts = 0
                
                while attempts < max_retries:
                    success = update_handler_with_concurrencyCheck(updated_status,updated_version,current_version)
                    if(success)
                       new_event_id = str(uuid.uuid4())
                        new_item = {
                            "event_id": new_event_id,
                            "event_type": "EventB",
                            "source_event_id": event_id,
                            "version": 0,          # Start at version 0 for the new item
                            "status": "new"
                        }
                        table.put_item(Item=new_item)
                        break;
                    # Re-read the item to get the latest version
                    latest_item = table.get_item(Key={"event_id": event_id}).get("Item", {})
                    latest_version = latest_item.get("version", 0)

                    # Prepare to retry
                    current_version = latest_version
                    updated_version = current_version + 1
                    attempts += 1
                    time.sleep(1)  # simple backoff

    return {"statusCode": 200, "body": json.dumps("Stream processing complete.")}
