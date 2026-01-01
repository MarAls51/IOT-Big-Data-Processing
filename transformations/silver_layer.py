import dlt
import mlflow
from pyspark.sql.functions import concat_ws, to_timestamp, col, trim, max as spark_max, collect_list, size
from pyspark.sql.window import Window

predict_anomaly_udf = mlflow.pyfunc.spark_udf(
    spark, 
    model_uri="models:/home_security_model/production"
)

@dlt.table(
    name="silver_iot_events",
    comment="Efficiently deduplicated IoT events with proper timestamps",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def silver_iot_events():
    df = dlt.read_stream("bronze_iot_events")
    
    df = df.withColumn(
        "event_datetime",
        to_timestamp(concat_ws(" ", col("date"), trim(col("time"))), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    ).withColumn(
        "state", trim(col("state"))
    ).drop("time")
    
    df = df.withWatermark("event_datetime", "1 hour").dropDuplicates(["sensor", "state", "event_datetime"])
    
    return df

@dlt.table(
    name="security_alerts",
    comment="Real-time security and health anomalies detected via ML inference"
)
def security_alerts():
    df = dlt.read_stream("silver_iot_events")

    df = df.withColumn("state_flag", (col("state") == "ON").cast("integer"))

    df_pivot = (
        df.groupBy("event_datetime")
          .pivot("sensor")  # one column per sensor
          .agg(spark_max("state_flag"))  # if multiple events per timestamp, take max
    )
    
    # Fill missing sensor values with 0
    sensor_cols = [c for c in df_pivot.columns if c != "event_datetime"]
    for c in sensor_cols:
        df_pivot = df_pivot.fillna({c: 0})

    seq_window = Window.orderBy("event_datetime").rowsBetween(-59, 0)
    
    df_sequences = (
        df_pivot
        .withColumn("sequence", collect_list(array(*sensor_cols)).over(seq_window))
        .filter(size(col("sequence")) == 60)
    )

    df_with_predictions = df_sequences.withColumn(
        "is_anomaly",
        predict_anomaly_udf(col("sequence"))
    )

    return df_with_predictions.filter(col("is_anomaly") == True)

