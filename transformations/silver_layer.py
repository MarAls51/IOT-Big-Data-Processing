import dlt
from pyspark.sql.functions import concat_ws, to_timestamp, col, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="silver_iot_events",
    comment="Optimized Silver IoT events with single datetime column, deduplicated and partitioned",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"    
    }
)
def silver_iot_events():
    df = dlt.read("bronze_iot_events")
    
    df = df.withColumn(
        "event_datetime",
        to_timestamp(concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    ).drop("date", "time")
    
    window = Window.partitionBy("room", "event_datetime").orderBy("event_datetime")
    df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    
    df = df.withColumn("event_date", col("event_datetime").cast("date"))
    
    return df