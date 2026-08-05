# Delta Lake Analytics Engine

A production-grade Lakehouse data pipeline built on Databricks using PySpark and Delta Lake.

## Architecture & Features
* **PySpark ETL:** Ingests and parses nested JSON transactional order logs into strongly typed DataFrames.
* **Schema Evolution:** Implemented `mergeSchema` on Delta Lake tables to handle upstream schema drift dynamically.
* **Performance Optimization:** Applied `OPTIMIZE ... ZORDER BY` on high-cardinality keys (`customer_id`) for data skipping.
* **Audit & CDC:** Utilized Delta Lake transaction log history for time-travel auditing and Structured Streaming read endpoints.

## Tech Stack
* **Platform:** Databricks
* **Engine:** PySpark (Spark SQL, Structured Streaming)
* **Storage:** Delta Lake
