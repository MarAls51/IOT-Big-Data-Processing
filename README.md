This project implements a high-availability IoT data platform designed for real-time anomaly detection and large-scale analytics using a modern Medallion Lakehouse architecture on AWS and Databricks. By integrating AWS Kinesis with Databricks Delta Live Tables, the pipeline optimizes the "Real-Time vs. Cost" trade-off through a hybrid orchestration strategy, the Bronze and Silver layers operate on a continuous streaming basis to ingest raw binary state events and apply an in-stream Machine Learning UDF for sub-second emergency alerting, while the Gold analytical layer utilizes triggered batch processing to generate daily and hourly room-level aggregations for Tableau dashboards. This design ensures mission-critical operational response times while minimizing cloud compute costs for non-urgent business metrics, demonstrating a production-grade approach to data quality

<img width="1191" height="577" alt="iot-data-medallion" src="https://github.com/user-attachments/assets/b68f5054-1dad-42e4-8935-d2c8073833ca" />


Dataset used: https://zenodo.org/records/15708568


