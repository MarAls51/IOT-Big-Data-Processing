import boto3
import pandas as pd
import json
from uuid import uuid4

# some simulated some api

KINESIS_STREAM_NAME = "iot-device-data"
REGION = "us-east-1"
CSV_FILE = "aruba.csv" # smart home iot device data

MAX_RETRIES = 3
RETRY_DELAY = 1    

kinesis_client = boto3.client("kinesis", region_name=REGION)

df = pd.read_csv(
    CSV_FILE,
    header=None,
    names=["date", "time", "room", "state"]
)

for _, row in df.iterrows():
    event = {
        "date": row["date"],
        "time": row["time"],
        "room": row["room"],
        "state": row["state"]
    }

    partition_key = str(uuid4())

    retries = 0
    while retries <= MAX_RETRIES:
        try:
            kinesis_client.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=json.dumps(event),
                PartitionKey=partition_key
            )
            print(f"Sent event: {event}")
            break 
        except ClientError as e:
            retries += 1
            print(f"Error sending record (attempt {retries}): {e}")
            if retries > MAX_RETRIES:
                print("Max retries reached, skipping this record.")
            else:
                time.sleep(RETRY_DELAY)