#!/usr/bin/env python3
"""
Demo script showing Azure cloud provider usage in CI/CD scenarios.

This demonstrates how you can simply set environment="azure" in your config
and the cloud_swap module automatically handles all the Azure-specific code.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from f1_data_platform.config.settings import Settings
from f1_data_platform.cloud_swap import get_cloud_provider


def demo_azure_integration():
    """Demonstrate Azure cloud provider integration."""
    print("🔷 F1 Pipeline Azure Integration Demo")
    print("=" * 50)
    
    # Simulate CI/CD environment where config is passed
    print("\n1. 📝 Loading Azure configuration...")
    
    # In CI/CD, you would typically load from environment variables or config files
    azure_config = {
        "environment": "azure",
        "storage": {
            "provider": "azure",
            "account_name": "myf1storageaccount",
            "container_name": "f1-data",
            "use_credential": True  # Use managed identity in CI/CD
        },
        "database": {
            "provider": "azure_sql",
            "server": "f1-pipeline-server.database.windows.net",
            "database": "f1_pipeline",
            "username": "f1admin",
            "password": os.getenv("F1_DB_PASSWORD", "secure_password"),
            "port": 1433
        },
        "compute": {
            "provider": "azure_batch",
            "batch_account_name": "f1pipelinebatch",
            "resource_group": "f1-pipeline-rg",
            "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID", "your-subscription-id")
        },
        "openf1": {
            "base_url": "https://api.openf1.org/v1",
            "rate_limit_delay": 0.1,
            "max_retries": 5,
            "timeout": 60,
            "years_to_fetch": [2023]
        },
        "logging": {
            "level": "INFO"
        }
    }
    
    # Create settings from config
    settings = Settings(**azure_config)
    print(f"   ✅ Environment: {settings.environment}")
    print(f"   ✅ Storage Provider: {settings.storage.provider}")
    print(f"   ✅ Database Provider: {settings.database.provider}")
    print(f"   ✅ Compute Provider: {settings.compute.provider}")
    
    print("\n2. 🏗️  Initializing cloud provider...")
    
    # This is where the magic happens - cloud_swap automatically handles Azure
    try:
        # Get cloud provider config from settings
        cloud_config = settings.get_cloud_provider_config()
        
        # Factory automatically creates the right provider based on environment
        cloud_provider = get_cloud_provider(settings.environment, cloud_config)
        
        print(f"   ✅ Cloud provider created: {type(cloud_provider).__name__}")
        
        # Get individual providers
        storage = cloud_provider.get_storage_provider()
        database = cloud_provider.get_database_provider()
        compute = cloud_provider.get_compute_provider()
        
        print(f"   ✅ Storage: {type(storage).__name__}")
        print(f"   ✅ Database: {type(database).__name__}")
        print(f"   ✅ Compute: {type(compute).__name__}")
        
    except ImportError as e:
        print(f"   ⚠️  Azure dependencies not installed: {e}")
        print("   💡 In production CI/CD, these would be pre-installed")
        return
    except Exception as e:
        print(f"   ⚠️  Provider initialization error: {e}")
        print("   💡 This is expected in demo mode without real Azure credentials")
    
    print("\n3. 🔍 Health check simulation...")
    
    try:
        # Simulate health check (would fail without real credentials)
        health_status = cloud_provider.health_check()
        print("   Health Status:")
        for service, status in health_status.items():
            status_emoji = "✅" if status else "❌"
            print(f"     {status_emoji} {service}: {'healthy' if status else 'unhealthy'}")
    except Exception as e:
        print(f"   ⚠️  Health check error (expected without credentials): {e}")
    
    print("\n4. 🚀 CI/CD Usage Examples:")
    print("""
    # In your CI/CD pipeline (e.g., GitHub Actions, Azure DevOps):
    
    # Step 1: Set environment variables
    export F1_DB_PASSWORD="${{ secrets.AZURE_SQL_PASSWORD }}"
    export AZURE_SUBSCRIPTION_ID="${{ secrets.AZURE_SUBSCRIPTION_ID }}"
    
    # Step 2: Install dependencies
    pip install -r requirements.txt
    
    # Step 3: Run with Azure config
    python run_f1_data_platform.py --config config/azure.yaml
    
    # The cloud_swap module automatically:
    # ✅ Detects environment="azure" 
    # ✅ Loads AzureCloudProvider
    # ✅ Configures Azure Blob Storage
    # ✅ Configures Azure SQL Database  
    # ✅ Configures Azure Batch (optional)
    # ✅ Handles authentication (managed identity, service principal, etc.)
    """)
    
    print("\n5. 🔧 Configuration Flexibility:")
    print("""
    The same pipeline can work with any cloud provider by just changing config:
    
    Local Development:     environment: local
    AWS Production:        environment: aws  
    Azure Production:      environment: azure
    GCP Staging:          environment: gcp
    
    No code changes needed - just configuration! 🎉
    """)


if __name__ == "__main__":
    demo_azure_integration()