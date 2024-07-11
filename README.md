# azure-monitor-observability


# User-specified parameter
USE_AAD_FOR_SEARCH = True  

def authenticate_azure_search(api_key=None, use_aad_for_search=False):
    if use_aad_for_search:
        print("Using AAD for authentication.")
        credential = DefaultAzureCredential()
    else:
        print("Using API keys for authentication.")
        if api_key is None:
            raise ValueError("API key must be provided if not using AAD for authentication.")
        credential = AzureKeyCredential(api_key)
    return credential

azure_search_credential = authenticate_azure_search(api_key=SEARCH_SERVICE_API_KEY, use_aad_for_search=USE_AAD_FOR_SEARCH)
