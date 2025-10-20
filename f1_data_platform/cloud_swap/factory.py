"""Cloud provider factory."""

from typing import Dict, Any
from .interfaces import CloudProvider
from .providers.local import LocalCloudProvider


def get_cloud_provider(provider_type: str, config: Dict[str, Any]) -> CloudProvider:
    """Factory function to create cloud provider instances."""
    
    if provider_type.lower() == "local":
        return LocalCloudProvider(config)
    
    elif provider_type.lower() == "aws":
        from .providers.aws import AWSCloudProvider
        return AWSCloudProvider(config)
    
    elif provider_type.lower() == "azure":
        # Import here to avoid dependency issues if Azure SDK not installed
        try:
            from .providers.azure import AzureCloudProvider
            return AzureCloudProvider(config)
        except ImportError:
            raise ImportError("Azure dependencies not installed. Run: pip install azure-storage-blob azure-identity pyodbc")
    
    elif provider_type.lower() == "gcp":
        # Import here to avoid dependency issues if GCP SDK not installed
        try:
            from .providers.gcp import GCPCloudProvider
            return GCPCloudProvider(config)
        except ImportError:
            raise ImportError("GCP dependencies not installed. Run: pip install google-cloud-storage google-auth")
    
    else:
        raise ValueError(f"Unsupported cloud provider: {provider_type}")


class CloudProviderFactory:
    """Factory class for creating cloud provider instances."""
    
    @staticmethod
    def create(provider_type: str, config: Dict[str, Any]) -> CloudProvider:
        """Create a cloud provider instance."""
        return get_cloud_provider(provider_type, config)
    
    @staticmethod
    def get_supported_providers() -> list:
        """Get list of supported cloud providers."""
        return ["local", "aws", "azure", "gcp"]