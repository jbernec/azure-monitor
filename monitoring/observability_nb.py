# Databricks notebook source
# MAGIC %md
# MAGIC #### Implementing Azure Monitor Observability for the VA Data Engineering platform.
# MAGIC
# MAGIC * 1. Define required OTLP and Logs Ingestion API packages
# MAGIC     * azure-core==1.30.0
# MAGIC     * azure-identity==1.15.0
# MAGIC     * azure-monitor-opentelemetry-exporter==1.0.0b22
# MAGIC     * azure-monitor-ingestion==1.0.3
# MAGIC     * azure-monitor-opentelemetry==1.0.0
# MAGIC     * opentelemetry-api==1.22.0
# MAGIC     * opentelemetry-sdk==1.22.0
# MAGIC * 2. Use the Logs Ingestion API to track and send custom audit metrics/values to Log Analytics custom tables and OpenTelementry Protocol (OTLP) framework to export exception logs to the AppExceptions native table in Azure Monitor Log Analytics and the Exceptions Log table in Azure Monitor AppInsights.
# MAGIC
# MAGIC * 3. To edit the log analytics DCR and custom logs/tables schema requires two steps:
# MAGIC     * Manually edit the schema of the custom log in the Log analytics tables view in the azure portal.
# MAGIC     * Run the custom powershell code provided in the following link to edit the DCR: https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-edit#putting-everything-together
# MAGIC
# MAGIC * 4. Info logs get exported to the traces logs. Exceptions get exported to the Exceptions log in App Insight and AppExceptions log in the Log Analytics workspace. Custom audits get sent to the specified custom log/table in the Log Analytics workspace using the logs ingestion API.

# COMMAND ----------

# https://pypi.org/project/azure-monitor-opentelemetry-exporter/
# https://github.com/open-telemetry/opentelemetry-python
# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/monitor/azure-monitor-opentelemetry-exporter/samples/logs/sample_exception.py

# https://learn.microsoft.com/en-us/azure/azure-monitor/app/opentelemetry-configuration?tabs=python#set-the-cloud-role-name-and-the-cloud-role-instance
# You can also set environment variables using the spark_env_vars field in the Create cluster request or Edit cluster request Clusters API endpoints. 
# https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters#--request-structure-of-the-cluster-definition
# https://learn.microsoft.com/en-us/azure/databricks/clusters/init-scripts


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Map Storage Account to Service Principal Object for Token Access.

# COMMAND ----------

# DBTITLE 1,Configure ADLS Gen 2 Access
# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.adls05.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls05.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adls05.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.adls05.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls05.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

# DBTITLE 1,Import Required Packages
# https://realpython.com/python-logging/
# https://opentelemetry.io/docs/instrumentation/python/
# https://betterstack.com/community/guides/logging/how-to-start-logging-with-python/

from datetime import datetime
import os
import json
import logging
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.session import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql import DataFrame
import azure.identity
from azure.identity import DefaultAzureCredential, EnvironmentCredential, ManagedIdentityCredential, SharedTokenCacheCredential
from azure.identity import ClientSecretCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.core.exceptions import HttpResponseError
from opentelemetry._logs import (
    get_logger_provider,
    set_logger_provider,
)
from opentelemetry.sdk._logs import (
    LoggerProvider,
    LoggingHandler,
)
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter

# COMMAND ----------

# DBTITLE 1,Set Client Secret Credential
tenant_id=dbutils.secrets.get(scope="myscope", key="tenantid")
client_id = dbutils.secrets.get(scope="myscope", key="clientid")
client_secret = dbutils.secrets.get(scope="myscope", key="clientsecret")
#credential = DefaultAzureCredential()
credential = azure.identity.ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

# COMMAND ----------

# DBTITLE 1,Configure OTLP Logger Components
set_logger_provider(LoggerProvider())
exporter = AzureMonitorLogExporter.from_connection_string(
    dbutils.secrets.get("myscope", key="appinsightsconnstr"), credential=credential
)
get_logger_provider().add_log_record_processor(BatchLogRecordProcessor(exporter))

# Attach LoggingHandler to namespaced logger
handler = LoggingHandler()
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##### Ingest Batch Data and Capture Streaming Metrics of rExport to Azure Monitor Log Analytics.

# COMMAND ----------

# ingest batch data data into df
def read_data_files(spark: SparkSession, file_path: str) -> DataFrame:
    try:
        pass
        df = (
            spark.read.format("json")
            .option("header", True)
            .option("inferSchema", True)
            .load(file_path)
            .withColumn(
                "tpep_dropoff_datetime",
                col=col("tpep_dropoff_datetime").cast(TimestampType()),
            )
            .withColumn(
                "tpep_pickup_datetime",
                col=col("tpep_pickup_datetime").cast(TimestampType()),
            )
        )

        properties = {
            "notebook_name": "observability_poc",
            "taxidata_ingestion_status": "success",
            "level": "info",
        }
        logger.info(
            "Batch read of nytaxi json files, new column addition and data type convertion were successful.",
            extra=properties,
            exc_info=True
        )
        return df
    except Exception as e:
        properties = {
            "notebook_name": "observability_poc",
            "read_data_files_error": "true",
        }
        logger.error(
            "Exception occured",
            stack_info=True,
            exc_info=True,
            extra=properties,
        )


try:
    pass
    data_path = "dbfs:/databricks-datasets/nyctaxi/sample/json/"
    df_json = read_data_files(spark=spark, file_path=data_path)
    df_json.display()
    record_count = df_json.count()
    print(record_count)
except Exception as e:
    properties = {
        "notebook_name": "observability_poc",
        "dataframe_display_error": "true",
    }
    logger.error(
        "Error occurred reading and displaying dataframe",
        exc_info=True,
        extra=properties,
    )
    print("check the azure monitor exception logs")

# COMMAND ----------

# datetime formats

print(datetime.now().strftime("%Y-%d-%m %H:%M:%S.%f"))

date_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
print(date_time)

# COMMAND ----------

# Clear out data from previous demo execution

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path= "abfss://adlscontainer@adls05.dfs.core.windows.net/jsondata/_checkpoint"
schema_location = "abfss://adlscontainer@adls05.dfs.core.windows.net/jsondata/_schematracking"
target_path = "abfss://adlscontainer@adls05.dfs.core.windows.net/jsondata/output"
table_name = f"{username}_nytaxi"
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(target_path, True)
dbutils.fs.rm(schema_location, True)

# COMMAND ----------

options = {
    "cloudFiles.format": "json",
    "cloudFiles.inferColumnTypes": True,
    "cloudFiles.inferSchema": True,
    "cloudFiles.schemaLocation": schema_location,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ingest Streaming Data and Capture Streaming Metrics of rExport to Azure Monitor Log Analytics.

# COMMAND ----------

def start_streaming_query(target_path:str):
    while True:
        try:
            q = (
                spark.readStream.format("cloudFiles")
                .options(**options)
                .load("dbfs:/databricks-datasets/nyctaxi/sample/json/")
                .writeStream.format("delta")
                .trigger(availableNow=True)
                .option("mergeSchema", "true")
                .option("checkpointLocation", checkpoint_path)
                .queryName("nytaxi_query")
                .option("path", target_path)
                .start()
            )
            q.awaitTermination()
            return q
            properties = {
            "notebook_name": "observability_poc",
            "notebook_cell_execution": "nytaxi data streaming query",
            }
            logger.info("streaming query execution was successful", exc_info=True, extra=properties)
            print("check the azure monitor Apptraces logs")
        except BaseException as e:
            properties = {
            "notebook_name": "observability_poc",
            "notebook_cell_execution": "nytaxi data streaming query",
            }
            logger.exception("streaming query execution encountered an exception", exc_info=True, extra=properties)
            print("check the azure monitor exception logs")
            # Adding a new column will trigger an UnknownFieldException. In this case we just restart the stream:
            if not ("UnknownFieldException" in str(e.stackTrace)):
                raise e



# COMMAND ----------

query_ = start_streaming_query(target_path=target_path)

# COMMAND ----------


query_metrics = [{
    "TotalInputRowsCount": query_.lastProgress["numInputRows"],
    "ProcessedInputRowsPerSecond": query_.lastProgress["processedRowsPerSecond"],
    "InputSink": query_.lastProgress["sink"]["description"],
    "InputSources": query_.lastProgress["sources"][0]["description"],
    "TimeGenerated": query_.lastProgress["timestamp"],
    "Application": "observability_streaming_query",
    "RequiredClusterfeature": "photon_predictive_io",
    "JobId": query_.lastProgress["id"],
    "JobName": query_.lastProgress["name"],
    "CurrentTableName": table_name
}]

query_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upload Streaming Metrics to Log Analytics Using the Logs Ingestion API.

# COMMAND ----------

# Configure Log Ingestion Client for Azure Commercial Cloud
# https://learn.microsoft.com/en-us/azure/azure-monitor/logs/notebooks-azure-monitor-logs

dce_endpoint = "https://logsingestionendpoint-jwl7.eastus-1.ingest.monitor.azure.com" # ingestion endpoint of the Data Collection Endpoint object
dcr_immutableid = "dcr-bc9e64eba16345518a890476255b6827" # immutableId property of the Data Collection Rule
stream_name = "Custom-ApacheSparkLogs_CL" # name of the stream in the DCR that represents the destination table

# credential = DefaultAzureCredential()
client = LogsIngestionClient(endpoint=dce_endpoint, credential=credential, logging_enable=True)

# configure the metrics publishing role on the DCR for this operation to be successful.
try:
    client.upload(rule_id=dcr_immutableid, stream_name=stream_name, logs=query_metrics)
except HttpResponseError as e:
    print(f"Upload failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Information for configuring log ingestion client for non-public Azure cloud
# MAGIC By default, LogsIngestionClient is configured to connect to the public Azure cloud. To connect to non-public Azure clouds, some additional configuration is required. The appropriate scope for authentication must be provided using the credential_scopes keyword argument. The following example shows how to configure the client to connect to Azure US Government:
# MAGIC
# MAGIC logs_client = LogsIngestionClient(endpoint, credential_scopes=["https://monitor.azure.us//.default"])
# MAGIC
# MAGIC https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/monitor/azure-monitor-ingestion/README.md#configure-clients-for-non-public-azure-clouds
