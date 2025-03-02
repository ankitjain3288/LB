implementation to create the aws architecture to have two region, where we have dynamo db global table 
where each region has its own replica. Each replica has db sreams enabled and each stream is subscribed to
lambda function in each region to handle the event update task.

Below are the steps:


