import os
import json
import time
import uuid
import boto3
from botocore.exceptions import ClientError

table_name="NetworkEvents"
local_region = "east"
remote_region = "west"

dynamodb_local = boto3.resource("dynamodb")
table_local = dynamodb_local.Table(table_name)

def update_handler(item):
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
                    
                
    

def read_remote_region(id):
    dynamodb_remote = boto3.resource('dynamodb', region_name=remote_region)
    table_remote = dynamodb_remote.Table(table_name)
    response = table.get_item(Key=id, ConsistentRead=True)
    return response.get('Item')

def stale_read_handler(id):
    retVal = false
    item_from_local_read = read_region(local_region,key)
    item_from_remoteregion_read = read_region(remote_region,key)
    if item_from_local_read.get('version') > item_from_remoteregion_read.get('version')
       retVal = true
    return retVal

def handleLocalUpdateForEvent(id,region):
    item_local = table_local.get_item(Key=id, ConsistentRead=True)
    item_remote = read_remote_region(id)
    retVal = False
    if item_local.get('version') > item_remote.get('version')
        current_version = item.get('version')
        updated_version = current_version + 1
        updated_status = "processed"
        try:
            table_local.update_item(
                    Key={"id": id},
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
                        retVal = False
    else
        
    return retVal



    
    read_success = stale_read_handler(id)
    if read_success:
        # Prepare the new version and updated data
        updated_version = current_version + 1
        updated_status = "processed"

        max_retries = 3
        success = False
        attempts = 0
        
        while attempts < max_retries:
            success = update_handler(updated_status,updated_version,current_version)
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


def lambda_handler(event, context):
    # we will have each items having version , region as attributes in global table
    for record in event.get("Records", []):
        
        new_image = record["dynamodb"].get("NewImage", {})
        id = new_image.get("id", {}).get("S")
        event_type = new_image.get("event_type", {}).get("S")
        
        #current_version = new_image.get("version", {}).get("N")
        region = new_image.get("region", {}).get("S")
        if region == local_region and event_type == "EventA":
            handleLocalUpdateForEvent(id,region)
        else
            handleReplicationUpdate(id)

    return {"statusCode": 200, "body": json.dumps("Stream processing complete.")}
