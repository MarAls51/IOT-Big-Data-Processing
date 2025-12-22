This project showcases an end-to-end real-time IoT data analytics pipeline built using a modern lakehouse architecture on AWS and Databricks. It integrates AWS Kinesis, Databricks Delta Live Tables, Delta Lake, Amazon S3, Unity Catalog, and Tableau to simulate a production-grade streaming workflow from raw device events to analytics-ready datasets and dashboards.

Incoming IoT device events are continuously streamed through Kinesis, ingested into Databricks, and stored in a Bronze Delta table with minimal transformation to preserve raw data fidelity. The Silver layer cleans and enriches the data by merging date and time fields into a unified timestamp, trimming malformed state values, and deduplicating events to produce a high-quality, analytics-ready event stream. From this refined dataset, Gold-layer tables are generated with daily and hourly aggregations per room, enabling efficient analysis of device activity patterns and time-based usage trends. These curated aggregates are designed for direct consumption by BI tools such as Tableau, supporting interactive dashboards and scalable analytics while demonstrating best practices in streaming data engineering, governance, and cloud-native lakehouse design.

<img width="1191" height="577" alt="iot-data-medallion" src="https://github.com/user-attachments/assets/b68f5054-1dad-42e4-8935-d2c8073833ca" />


Dataset used: https://zenodo.org/records/15708568

