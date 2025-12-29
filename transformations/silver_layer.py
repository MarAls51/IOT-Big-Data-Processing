import dlt
import mlflow
from pyspark.sql.functions import concat_ws, to_timestamp, col, trim, collect_list, array, size
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
    
    seq_window = Window.partitionBy("sensor").orderBy("event_datetime").rowsBetween(-59, 0)
    
    df = df.withColumn("state_flag", (col("state") == "ON").cast("integer"))
    
    df_sequences = df.withColumn("sequence", collect_list("state_flag").over(seq_window)).filter(size(col("sequence")) == 60)
    
    df_with_predictions = df_sequences.withColumn(
        "is_anomaly",
        predict_anomaly_udf(col("sequence"))
    )
    
    return df_with_predictions.filter(col("is_anomaly") == True)

