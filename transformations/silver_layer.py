import dlt
import mlflow
from pyspark.sql.functions import concat_ws, to_timestamp, col, trim, struct

predict_anomaly_udf = mlflow.pyfunc.spark_udf(spark, model_uri="models:/home_security_model/production")

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

@dlt.table(
    name="security_alerts",
    comment="Real-time security and health anomalies detected via ML inference",
)

def security_alerts():
    df = dlt.read_stream("silver_iot_events")
    
    df_with_predictions = df.withColumn(
        "is_anomaly", 
        predict_anomaly_udf(struct("room", "state"))
    )
    
    return df_with_predictions.filter(col("is_anomaly") == "ON")
