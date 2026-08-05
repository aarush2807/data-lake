{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1785900176390,
     "inputWidgets": {},
     "nuid": "c3fdf6c0-d430-4800-ad1d-562aa42828f6",
     "showTitle": false,
     "startTime": 1785900170785,
     "submitTime": 1785900170741,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>order_id</th><th>customer_id</th><th>order_datetime</th><th>total_item_quantity</th><th>order_total</th><th>items</th></tr></thead><tbody><tr><td>1001</td><td>CUST_001</td><td>2026-08-01T10:15:00.000Z</td><td>3</td><td>125.5</td><td>List(List(P_01, Running Shoes, 1, 85.5), List(P_02, Socks Pack, 2, 20.0))</td></tr><tr><td>1002</td><td>CUST_002</td><td>2026-08-02T14:30:00.000Z</td><td>1</td><td>45.0</td><td>List(List(P_03, Water Bottle, 1, 45.0))</td></tr><tr><td>1003</td><td>CUST_001</td><td>2026-08-04T09:00:00.000Z</td><td>2</td><td>210.0</td><td>List(List(P_04, Fitness Tracker, 1, 150.0), List(P_01, Running Shoes, 1, 60.0))</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         1001,
         "CUST_001",
         "2026-08-01T10:15:00.000Z",
         3,
         125.5,
         [
          [
           "P_01",
           "Running Shoes",
           1,
           85.5
          ],
          [
           "P_02",
           "Socks Pack",
           2,
           20.0
          ]
         ]
        ],
        [
         1002,
         "CUST_002",
         "2026-08-02T14:30:00.000Z",
         1,
         45.0,
         [
          [
           "P_03",
           "Water Bottle",
           1,
           45.0
          ]
         ]
        ],
        [
         1003,
         "CUST_001",
         "2026-08-04T09:00:00.000Z",
         2,
         210.0,
         [
          [
           "P_04",
           "Fitness Tracker",
           1,
           150.0
          ],
          [
           "P_01",
           "Running Shoes",
           1,
           60.0
          ]
         ]
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "order_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "order_datetime",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "total_item_quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "order_total",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "items",
         "type": "{\"containsNull\":true,\"elementType\":{\"fields\":[{\"metadata\":{},\"name\":\"product_id\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"product_name\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"quantity\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"unit_price\",\"nullable\":true,\"type\":\"double\"}],\"type\":\"struct\"},\"type\":\"array\"}"
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType\n",
    "from datetime import datetime\n",
    "\n",
    "# Initialize Spark session (automatically available in Databricks notebooks)\n",
    "spark = SparkSession.builder.appName(\"SalesOrdersTest\").getOrCreate()\n",
    "\n",
    "# Define schema matching Databricks retail sales_orders dataset\n",
    "schema = StructType([\n",
    "    StructField(\"order_id\", IntegerType(), True),\n",
    "    StructField(\"customer_id\", StringType(), True),\n",
    "    StructField(\"order_datetime\", TimestampType(), True),\n",
    "    StructField(\"total_item_quantity\", IntegerType(), True),\n",
    "    StructField(\"order_total\", DoubleType(), True),\n",
    "    StructField(\"items\", ArrayType(\n",
    "        StructType([\n",
    "            StructField(\"product_id\", StringType(), True),\n",
    "            StructField(\"product_name\", StringType(), True),\n",
    "            StructField(\"quantity\", IntegerType(), True),\n",
    "            StructField(\"unit_price\", DoubleType(), True)\n",
    "        ])\n",
    "    ), True)\n",
    "])\n",
    "\n",
    "# Create sample data\n",
    "data = [\n",
    "    (\n",
    "        1001, \n",
    "        \"CUST_001\", \n",
    "        datetime(2026, 8, 1, 10, 15, 0), \n",
    "        3, \n",
    "        125.50, \n",
    "        [\n",
    "            {\"product_id\": \"P_01\", \"product_name\": \"Running Shoes\", \"quantity\": 1, \"unit_price\": 85.50},\n",
    "            {\"product_id\": \"P_02\", \"product_name\": \"Socks Pack\", \"quantity\": 2, \"unit_price\": 20.00}\n",
    "        ]\n",
    "    ),\n",
    "    (\n",
    "        1002, \n",
    "        \"CUST_002\", \n",
    "        datetime(2026, 8, 2, 14, 30, 0), \n",
    "        1, \n",
    "        45.00, \n",
    "        [\n",
    "            {\"product_id\": \"P_03\", \"product_name\": \"Water Bottle\", \"quantity\": 1, \"unit_price\": 45.00}\n",
    "        ]\n",
    "    ),\n",
    "    (\n",
    "        1003, \n",
    "        \"CUST_001\", \n",
    "        datetime(2026, 8, 4, 9, 0, 0), \n",
    "        2, \n",
    "        210.00, \n",
    "        [\n",
    "            {\"product_id\": \"P_04\", \"product_name\": \"Fitness Tracker\", \"quantity\": 1, \"unit_price\": 150.00},\n",
    "            {\"product_id\": \"P_01\", \"product_name\": \"Running Shoes\", \"quantity\": 1, \"unit_price\": 60.00}\n",
    "        ]\n",
    "    )\n",
    "]\n",
    "\n",
    "# Create DataFrame\n",
    "df = spark.createDataFrame(data, schema)\n",
    "\n",
    "# Display result\n",
    "display(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1785900297056,
     "inputWidgets": {},
     "nuid": "0fc73845-ec0b-453a-b756-08f1239b5c20",
     "showTitle": false,
     "startTime": 1785900283169,
     "submitTime": 1785900283134,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Successfully created Delta Table: sales_orders_gold\n"
     ]
    }
   ],
   "source": [
    "# Write DataFrame to Delta Lake table\n",
    "df.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .option(\"overwriteSchema\", \"true\") \\\n",
    "    .saveAsTable(\"sales_orders_gold\")\n",
    "\n",
    "print(\"Successfully created Delta Table: sales_orders_gold\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1785900316394,
     "inputWidgets": {},
     "nuid": "6b86476b-d7ab-40b3-b521-698b61fdac57",
     "showTitle": false,
     "startTime": 1785900315673,
     "submitTime": 1785900315640,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>order_id</th><th>total_price</th></tr></thead><tbody><tr><td>101</td><td>1</td><td>150.0</td></tr><tr><td>102</td><td>2</td><td>89.5</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101,
         1,
         150.0
        ],
        [
         102,
         2,
         89.5
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "order_id",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "total_price",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "data = [\n",
    "    {\"order_id\": 1, \"customer_id\": 101, \"total_price\": 150.0},\n",
    "    {\"order_id\": 2, \"customer_id\": 102, \"total_price\": 89.5}\n",
    "]\n",
    "\n",
    "df = spark.createDataFrame(data)\n",
    "display(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1785900383046,
     "inputWidgets": {},
     "nuid": "98a8c8c2-34d2-4d5e-9261-b8d7aa07d0ec",
     "showTitle": false,
     "startTime": 1785900367256,
     "submitTime": 1785900367197,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "=== Delta Lake Version History & Audit Trail ===\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>version</th><th>timestamp</th><th>operation</th><th>operationParameters</th></tr></thead><tbody><tr><td>2</td><td>2026-08-05T03:26:19.000Z</td><td>OPTIMIZE</td><td>Map(predicate -> [], auto -> false, clusterBy -> [], zOrderBy -> [\"customer_id\"], batchId -> 0)</td></tr><tr><td>1</td><td>2026-08-05T03:26:10.000Z</td><td>WRITE</td><td>Map(mode -> Append, statsOnLoad -> false, partitionBy -> [], canMergeSchema -> true)</td></tr><tr><td>0</td><td>2026-08-05T03:24:56.000Z</td><td>CREATE OR REPLACE TABLE AS SELECT</td><td>Map(isV1SaveAsTableOverwrite -> true, partitionBy -> [], clusterBy -> [], description -> null, isManaged -> true, canOverwriteSchema -> true, properties -> {\"delta.parquet.format.version\":\"2.12.0\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.parquet.compression.codec\":\"zstd\",\"delta.enableDeletionVectors\":\"true\"}, statsOnLoad -> true)</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         2,
         "2026-08-05T03:26:19.000Z",
         "OPTIMIZE",
         {
          "auto": "false",
          "batchId": "0",
          "clusterBy": "[]",
          "predicate": "[]",
          "zOrderBy": "[\"customer_id\"]"
         }
        ],
        [
         1,
         "2026-08-05T03:26:10.000Z",
         "WRITE",
         {
          "canMergeSchema": "true",
          "mode": "Append",
          "partitionBy": "[]",
          "statsOnLoad": "false"
         }
        ],
        [
         0,
         "2026-08-05T03:24:56.000Z",
         "CREATE OR REPLACE TABLE AS SELECT",
         {
          "canOverwriteSchema": "true",
          "clusterBy": "[]",
          "description": null,
          "isManaged": "true",
          "isV1SaveAsTableOverwrite": "true",
          "partitionBy": "[]",
          "properties": "{\"delta.parquet.format.version\":\"2.12.0\",\"delta.parquet.format.version.afe.internal\":\"2.12.0\",\"delta.parquet.compression.codec\":\"zstd\",\"delta.enableDeletionVectors\":\"true\"}",
          "statsOnLoad": "true"
         }
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "version",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "operation",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "operationParameters",
         "type": "{\"keyType\":\"string\",\"type\":\"map\",\"valueContainsNull\":true,\"valueType\":\"string\"}"
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Is Stream Active? True\n"
     ]
    }
   ],
   "source": [
    "from pyspark.sql.functions import current_timestamp, lit\n",
    "from delta.tables import DeltaTable\n",
    "\n",
    "# -------------------------------------------------------------------\n",
    "# 1. SCHEMA EVOLUTION: Add new columns on-the-fly without breaking pipeline\n",
    "# -------------------------------------------------------------------\n",
    "df_v2 = df.withColumn(\"order_status\", lit(\"COMPLETED\")) \\\n",
    "          .withColumn(\"ingested_at\", current_timestamp())\n",
    "\n",
    "# Merge schema dynamically into existing Delta table\n",
    "df_v2.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"append\") \\\n",
    "    .option(\"mergeSchema\", \"true\") \\\n",
    "    .saveAsTable(\"sales_orders_gold\")\n",
    "\n",
    "# -------------------------------------------------------------------\n",
    "# 2. DELTA OPTIMIZATION: Z-Ordering for high-performance querying\n",
    "# -------------------------------------------------------------------\n",
    "spark.sql(\"OPTIMIZE sales_orders_gold ZORDER BY (customer_id)\")\n",
    "\n",
    "# -------------------------------------------------------------------\n",
    "# 3. TIME TRAVEL / AUDIT TRAIL: Inspect version history & change feed\n",
    "# -------------------------------------------------------------------\n",
    "delta_table = DeltaTable.forName(spark, \"sales_orders_gold\")\n",
    "history_df = delta_table.history()\n",
    "\n",
    "print(\"=== Delta Lake Version History & Audit Trail ===\")\n",
    "display(history_df.select(\"version\", \"timestamp\", \"operation\", \"operationParameters\"))\n",
    "\n",
    "# -------------------------------------------------------------------\n",
    "# 4. STREAMING READ: Read Delta table as an incremental stream (CDC)\n",
    "# -------------------------------------------------------------------\n",
    "stream_df = spark.readStream \\\n",
    "    .format(\"delta\") \\\n",
    "    .table(\"sales_orders_gold\")\n",
    "\n",
    "print(f\"Is Stream Active? {stream_df.isStreaming}\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "New Notebook 2026-08-04 22:16:49",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
