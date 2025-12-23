import dlt
from pyspark.sql.functions import concat_ws, to_timestamp, col, trim, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="silver_iot_events",
    comment="Efficiently deduplicated IoT events",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def silver_iot_events():
    df = dlt.read_stream("bronze_iot_events")
    
    df = df.withColumn(
        "event_datetime",
        to_timestamp(concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    ).withColumn("state", trim(col("state"))).drop("time")
    
    return df.dropDuplicates(["room", "event_datetime"])
