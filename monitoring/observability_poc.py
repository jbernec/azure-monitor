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

# export OTEL_SERVICE_NAME="nytaxi_streaming_pipeline"
# export OTEL_RESOURCE_ATTRIBUTES="service.namespace=cloudrole_opentelemetry,service.instance.id=databricks_opentelemtry_instance"


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Log Analytics vs. App Insights: https://turbo360.com/blog/azure-application-insights-vs-log-analytics

# COMMAND ----------

# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.stor0406ch.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.stor0406ch.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.stor0406ch.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.stor0406ch.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.stor0406ch.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

# https://realpython.com/python-logging/
# https://opentelemetry.io/docs/instrumentation/python/
# https://betterstack.com/community/guides/logging/how-to-start-logging-with-python/

from datetime import datetime
import os
import json
import logging
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
import azure.identity
from azure.identity import DefaultAzureCredential
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

set_logger_provider(LoggerProvider())
exporter = AzureMonitorLogExporter.from_connection_string(
    dbutils.secrets.get("myscope", key="appinsightsconnstr")
)
get_logger_provider().add_log_record_processor(BatchLogRecordProcessor(exporter))

# Attach LoggingHandler to namespaced logger
handler = LoggingHandler()
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# COMMAND ----------

print(os.getenv("OTEL_SERVICE_NAME"))
# print(os.environ.get("OTEL_RESOURCE_ATTRIBUTES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Ingestion Test

# COMMAND ----------

# file_path = "abfss://fsstore0406@stor0406ch.dfs.core.windows.net/data/json/input"
data_path = "dbfs:/databricks-datasets/nyctaxi/sample/json/"
df_nytaxi = spark.read.format("json").option("inferSchema", True).load(data_path)
df_nytaxi.display()
df_nytaxi.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, cast
from pyspark.sql.types import DateType, TimestampType

try:
    df = df_nytaxi.select("*")\
        .withColumn("tpep_dropoff_datetime", col=col("tpep_dropoff_datetime").cast(TimestampType()))\
        .withColumn("tpep_pickup_datetime", col=col("tpep_pickup_datetime").cast(TimestampType()))
    df.display()
    df.printSchema()
    logger.warning("New column and data type convertion was successful.")
except Exception as e:
    properties={"notebook_name": "observability_poc", "dataframe_transformation_error": "true"}
    #logger.error("Oohh boy, an exception occured", exc_info=True)
    logger.exception("Oohh boy, we have an exception", exc_info=True, extra=properties)
    print("check the azure monitor exception logs")

# COMMAND ----------


# ingest content data into df
def read_data_files(spark:SparkSession, file_path:str)->DataFrame:
    try:
        pass
        df = spark.read.format("json").option("header", True).option("inferSchema", True).load(file_path)
        properties={"notebook_name": "observability_poc","taxidata_ingestion_status": "success", "level": "info"}
        logger.info("Batch read of nytaxi JSON files was successful, check validation results.", extra=properties)
        return df
    except Exception as e:
        properties={"notebook_name": "observability_poc", "read_data_files_error": "true"}
        logger.error("Ingestion exception occured", stack_info=True, exc_info=True, extra=properties)

try:
    pass
    file_path = "abfss://fsstore0406@stor0406ch.dfs.core.windows.net/data/json/input"
    df_json = read_data_files(spark=spark, file_path=file_path)
    df_json.display()
    # logger.info("Visual exploration of nytaxi dataframe was successful")
except Exception as e:
    properties={"notebook_name": "observability_poc","dataframe_display_error": "true"}
    logger.error("Error occurred reading and displaying dataframe", exc_info=True, extra=properties)
    print("check the azure monitor exception logs")

# COMMAND ----------

df_json.display()
num_rows = df_json.count()
print(num_rows)

# COMMAND ----------

help(azure.identity.ClientSecretCredential)

# COMMAND ----------

# information needed to send data to the DCR endpoint
dce_endpoint = "https://logsingestionendpoint-jwl7.eastus-1.ingest.monitor.azure.com" # ingestion endpoint of the Data Collection Endpoint object
dcr_immutableid = "dcr-bc9e64eba16345518a890476255b6827" # immutableId property of the Data Collection Rule
stream_name = "Custom-ApacheSparkLogs_CL" # name of the stream in the DCR that represents the destination table

# credential = DefaultAzureCredential()
tenant_id=dbutils.secrets.get(scope="myscope", key="tenantid")
client_id = dbutils.secrets.get(scope="myscope", key="clientid")
client_secret = dbutils.secrets.get(scope="myscope", key="clientsecret")
credential = azure.identity.ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
client = LogsIngestionClient(endpoint=dce_endpoint, credential=credential, logging_enable=True)

# COMMAND ----------

# datetime formats

print(datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
# datetime.now().strftime("%c")
print(datetime.now())
date_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
num_rows = 1110

# COMMAND ----------

# audit values to be exported to log analytics using the logs ingestion api

log_body = [
  {
    "TimeGenerated": date_time,
    "Application": "otlp-logs-export",
    "LastRecordsCount": num_rows,
    "LastinputRowsPerSecond": 1400,
    "RequiredClusterfeature": "photon_predictive_io"
  }
]

# COMMAND ----------

log_body

# COMMAND ----------

# configure the metrics publishing role on the DCR for this operation to be successful.
try:
    client.upload(rule_id=dcr_immutableid, stream_name=stream_name, logs=log_body)
except HttpResponseError as e:
    print(f"Upload failed: {e}")

# COMMAND ----------

options = {"cloudFiles.useNotifications": True,
           "cloudFiles.format": "json",
           "header": "true",
           "cloudFiles.connectionString": dbutils.secrets.get("myscope", key="storconstr"),
           "cloudFiles.resourceGroup": "rgadb",
           "cloudFiles.subscriptionId": dbutils.secrets.get("myscope", key="subid"),
           "cloudFiles.tenantId": dbutils.secrets.get("myscope", key="tenantid"),
           "cloudFiles.clientId": dbutils.secrets.get("myscope", key="clientid"),
           "cloudFiles.clientSecret": dbutils.secrets.get("myscope", key="clientsecret"),
           "cloudFiles.inferColumnTypes": True,
           "cloudFiles.includeExistingFiles": True,
           "cloudFiles.inferSchema": True,
           "cloudFiles.schemaLocation": "abfss://fsstore0406@stor0406ch.dfs.core.windows.net/data/json/_schematracking",
           #"cloudFiles.schemaEvolutionMode": "addNewColumns",
           #"cloudFiles.schemaEvolutionMode": "failOnNewColumns",
           #"cloudFiles.schemaEvolutionMode": "rescue",
           "cloudFiles.region": "eastus"}

# COMMAND ----------

try:
    df = (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .load("abfss://fsstore0406@stor0406ch.dfs.core.windows.net/data/json/input/")
    )
    logger.info("readstream so far is successful")
except Exception as ex:
    properties = {
                "notebook_name": "observability_poc",
                "notebook_cell_execution": "readstream"
            }
    logger.exception("readstream exception occurred", exc_info=True, extra=properties)

# COMMAND ----------

# Assign the spn contributor rbac role, in order to list and create Event Grid Subscriptions. Ensure that the EventGrid service is registered in this subscription.

try:
    pass
    streaming_query = df.writeStream\
        .trigger(availableNow=True)\
            .format("delta")\
                .outputMode("append")\
                    .option("checkpointLocation", "abfss://fsstore0406@stor0406ch.dfs.core.windows.net/data/json/_checkpoint")\
                        .start("abfss://fsstore0406@stor0406ch.dfs.core.windows.net/data/json/output")
    # Use properties in logging statements
    properties={'custom_dimensions': streaming_query.lastProgress}
    #logger.info("Review today's streaming metrics at appinsights traces table", extra=properties)
    # logger.info(f"Review the streaming metrics at the appinsights traces table: {json.dumps(df_out.lastProgress)}")
    logger.info(f"Review the json streaming metrics at the appinsights traces table")

    # configure the metrics publishing role on the DCR for this operation to be successful.
    query_body = {
                "LastRecordsCount": streaming_query.lastProgress["numInputRows"],
                "InputSink": streaming_query.lastProgress["sink"]["description"],
                "InputSources": streaming_query.lastProgress["sources"][0]["description"],
                "LastinputRowsPerSecond": streaming_query.lastProgress["inputRowsPerSecond"],
                "TimeGenerated": streaming_query.lastProgress["timestamp"],
                "Application": "observability_streaming_query",
                "RequiredClusterfeature": "photon_predictive_io"
            }
    client.upload(rule_id=dcr_immutableid, stream_name=stream_name, logs=query_body)
except HttpResponseError as e:
    print(f"Upload failed: {e}")
except Exception as e:
    properties = {
                "notebook_name": "observability_poc",
                "notebook_cell_execution": "writestream"
            }
    logger.exception("writestream exception occured", exc_info=True, extra=properties)

# COMMAND ----------

try:
    pass
    while True:
        msg = streaming_query.lastProgress
        if msg != None:
            # print("YAY")

            print("\n")

            # Convert the StreamingQuery output to Python dictionary object
            body = json.dumps(streaming_query.lastProgress)
            print(body)
            # Use properties in logging statements
            # query_update = {key: streaming_query.lastProgress[key] for key in streaming_query.lastProgress if key != "timestamp"} #remove timestamp key
            # properties={query_update} # not working cos of unhashable dict elements
            properties = {
                "numInputRows": streaming_query.lastProgress["numInputRows"],
                "sink": streaming_query.lastProgress["sink"]["description"],
                "sources": streaming_query.lastProgress["sources"][0]["description"],
                "inputRowsPerSecond": streaming_query.lastProgress["inputRowsPerSecond"],
                "querytimestamp": streaming_query.lastProgress["timestamp"],
                "notebook_name": "observability_poc",
            }
            logger.info(
                "View today's streaming metrics at appinsights traces table",
                extra=properties,
            )
            # logger.info(f"View today's json streaming metrics at appinsights traces table: {body}")
            break
except Exception as e:
    properties = {
                "notebook_name": "observability_poc",
                "notebook_cell_execution": "send streaming query metrics"
            }
    logger.exception("sending streaming query logs failed", exc_info=True, extra=properties)

# COMMAND ----------

streaming_query.lastProgress
