from azure.identity import ClientSecretCredential
import azure.identity
from datetime import datetime
import json
import logging
from azure.monitor.ingestion import LogsIngestionClient, LogsUploadError
from azure.core.exceptions import HttpResponseError
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, EnvironmentCredential


# set authentication method
USE_AAD_FOR_LOG_INGESTION = True
LOG_ANALYTICS_KEY = ""

client_secret_creds = {
    "tenant_id": dbutils.secrets.get(scope="scope", key="tenantid"),
    "client_id": dbutils.secrets.get(scope="scope", key="clientid"),
    "client_secret": dbutils.secrets.get(scope="scope", key="clientsecret")
}

def authenticate_azure_log_ingestion(client_secret_cred: dict = None, use_client_id: bool=False, log_key: str =None, use_aad_for_log_ingestion: bool =False):
    if use_aad_for_log_ingestion:
        print("Using AAD for authentication.")
        credential = DefaultAzureCredential()
    elif use_client_id:
        print("Using client secret credentials for authentication.")
        credential = azure.identity.ClientSecretCredential(tenant_id=client_secret_cred["tenant_id"], client_id=client_secret_cred["client_id"], client_secret=client_secret_cred["client_secret"])
    else:
        print("Using API keys for authentication.")
        if log_key is None:
            raise ValueError("API key must be provided if not using AAD for authentication.")
        credential = AzureKeyCredential(log_key)
    return credential


def authenticate_public_cloud():
    # [START create_client_public_cloud]
    from azure.identity import DefaultAzureCredential
    from azure.monitor.ingestion import LogsIngestionClient

    credential = DefaultAzureCredential()
    endpoint = "https://example.ingest.monitor.azure.com"
    client = LogsIngestionClient(endpoint, credential)
    # [END create_client_public_cloud]


def authenticate_sovereign_cloud():
    # [START create_client_sovereign_cloud]
    from azure.identity import AzureAuthorityHosts, DefaultAzureCredential
    from azure.monitor.ingestion import LogsIngestionClient

    credential = DefaultAzureCredential(authority=AzureAuthorityHosts.AZURE_GOVERNMENT)
    endpoint = "https://example.ingest.monitor.azure.us"
    client = LogsIngestionClient(endpoint, credential, credential_scopes=["https://monitor.azure.us/.default"])
    # [END create_client_sovereign_cloud]
