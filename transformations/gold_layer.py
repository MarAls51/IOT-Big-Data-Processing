import dlt
from pyspark.sql.functions import col, count, hour

# -------------------------------
# Gold Table 1: Daily summary
# -------------------------------
@dlt.table(
    name="gold_room_daily_summary",
    comment="Daily summary of IoT events per room",
    table_properties={"quality": "gold"}
)
def gold_room_daily_summary():
    df = dlt.read("silver_iot_events")

    df = df.filter(col("event_datetime").isNotNull() & col("state").isNotNull())

    daily_summary = (
        df.groupBy("room", "date")
          .agg(count("*").alias("event_count"))
    )
    
    return daily_summary


# -------------------------------
# Gold Table 2: Hourly summary
# -------------------------------
@dlt.table(
    name="gold_room_hourly_summary",
    comment="Hourly summary of IoT events per room",
    table_properties={"quality": "gold"}
)
def gold_room_hourly_summary():
    df = dlt.read("silver_iot_events")
    
    df = df.filter(col("event_datetime").isNotNull() & col("state").isNotNull())

    df = df.withColumn("hour_of_day", hour(col("event_datetime")))

    hourly_summary = (
        df.groupBy("room", "date", "hour_of_day")
          .agg(count("*").alias("event_count"))
    )
    
    return hourly_summary

