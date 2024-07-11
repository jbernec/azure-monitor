# set authentication method
USE_AAD_FOR_LOG_INGESTION = True
LOG_ANALYTICS_KEY = ""

def authenticate_azure_log_ingestion(client_secret_cred: dict, log_key=None, use_aad_for_log_ingestion=False, use_client_id=False):
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

azure_log_analytics_credential = authenticate_azure_log_ingestion(log_key=LOG_ANALYTICS_KEY, use_aad_for_log_ingestion=USE_AAD_FOR_LOG_INGESTION)
