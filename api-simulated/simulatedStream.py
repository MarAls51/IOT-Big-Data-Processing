import boto3
import pandas as pd
import json
from uuid import uuid4

KINESIS_STREAM_NAME = "iot-device-data"
REGION = "us-east-1"
CSV_FILE = "aruba.csv"  # smart home IoT device data
BATCH_SIZE = 500

kinesis_client = boto3.client("kinesis", region_name=REGION)

df = pd.read_csv(CSV_FILE, header=None, names=["date", "time", "room", "state"])

records = []
for _, row in df.iterrows():
    event = {
        "date": row["date"],
        "time": row["time"],
        "room": row["room"],
        "state": row["state"]
    }
    print("event", event)
    records.append({
        "Data": json.dumps(event),
        "PartitionKey": str(uuid4())
    })

    if len(records) == BATCH_SIZE:
        kinesis_client.put_records(StreamName=KINESIS_STREAM_NAME, Records=records)
        records = []

if records:
    kinesis_client.put_records(StreamName=KINESIS_STREAM_NAME, Records=records)