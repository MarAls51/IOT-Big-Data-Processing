import dlt
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

schema = StructType([
    StructField("date", StringType()),
    StructField("time", StringType()),
    StructField("room", StringType()),
    StructField("state", StringType())
])

kinesis_config = {
    "streamName": "iot-device-data",
    "region": "us-east-1",
    "serviceCredential":"aws_kinesis_credentials",  # I'm using an AWS IAM role which is referenced by this databricks credential.
    "initialPosition":"earliest"
}

@dlt.table(
    name="bronze_iot_events",
    comment="Raw IoT events ingested from Kinesis",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_iot_events():
    raw = (
        spark.readStream
        .format("kinesis")
        .options(**kinesis_config)
        .load()
    )

    parsed = (
        raw.select(
            from_json(col("data").cast("string"), schema).alias("json")
        )
        .select("json.*")
    )

    return parsed