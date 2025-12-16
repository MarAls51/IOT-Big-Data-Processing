import dlt
from pyspark.ssql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType 


kenesis_config = {
    "streamName": "iot-device-data"
    "region": "us-east-1",
    "serviceCredential":"aws_kinesis_credentials", # I'm using an IAM role which is referenced by this databricks credential.
    "initialPosition":"earliest"
} 