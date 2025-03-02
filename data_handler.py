import os
import json
import time
import uuid
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)



def read_from_region(region, table_name, key):
    
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    response = table.get_item(Key=key, ConsistentRead=True)
    return response.get('Item')

def multiRegion_read(table_name, key, regions):
    
    results = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(read_from_region, region, table_name, key): region for region in regions}
        for future in as_completed(futures):
            region = futures[future]
            try:
                result = future.result()
                if result:
                    print(f"Region {region} returned: {result}")
                    results.append(result)
            except Exception as e:
                print(f"Error reading from region {region}: {e}")

    if not results:
        return None

    # For demonstration, assume each item has a 'version' attribute.
    # Choose the result with the highest version number.
    quorum_item = max(results, key=lambda x: int(x.get('version', 0)))
    return quorum_item

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
        if record.get("eventName") not in ["INSERT", "MODIFY"]:
            continue

        new_image = record["dynamodb"].get("NewImage", {})
        event_id = new_image.get("event_id", {}).get("S")
        event_type = new_image.get("event_type", {}).get("S", "EventA")
        
        # Default to version 0 if missing
        current_version_str = new_image.get("version", {}).get("N", "0")
        current_version = int(current_version_str)

        print(f"Processing item: event_id={event_id}, type={event_type}, version={current_version}")

        # Prepare the new version and updated data
        updated_version = current_version + 1
        updated_status = "processed"
        
        max_retries = 3
        success = False
        attempts = 0
        
        while not success and attempts < max_retries:
            try:
                # 1) Optimistic concurrency update
                # ConditionExpression ensures no one else has updated 'version' in the meantime.
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
                print(f"Successfully updated item {event_id} to version {updated_version}.")
                success = True

            except ClientError as e:
                # Check if it's a conditional check failure (version mismatch)
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    print(f"Conflict detected for {event_id}; retrying...")
                    # Re-read the item to get the latest version
                    latest_item = table.get_item(Key={"event_id": event_id}).get("Item", {})
                    latest_version = latest_item.get("version", 0)

                    # Prepare to retry
                    current_version = latest_version
                    updated_version = current_version + 1
                    attempts += 1
                    time.sleep(1)  # simple backoff
                else:
                    # Some other error occurred
                    print(f"Error updating item {event_id}: {str(e)}")
                    break

        if success:
            # 2) Create a new 'Event B' item (or transform from A to B)
            new_event_id = str(uuid.uuid4())
            new_item = {
                "event_id": new_event_id,
                "event_type": "EventB",
                "source_event_id": event_id,
                "version": 0,          # Start at version 0 for the new item
                "status": "new"
            }
            try:
                table.put_item(Item=new_item)
                print(f"Created new EventB: {new_event_id}, from source: {event_id}")
            except Exception as e:
                print(f"Error creating new EventB {new_event_id}: {str(e)}")
        else:
            print(f"Failed to update item {event_id} after {max_retries} attempts.")

    return {"statusCode": 200, "body": json.dumps("Stream processing complete.")}
