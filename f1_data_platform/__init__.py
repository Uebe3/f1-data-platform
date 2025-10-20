"""F1 Data Platform

A comprehensive, cloud-agnostic data analytics platform for Formula 1 racing data.
"""

__version__ = "1.0.0"
__author__ = "F1 Data Platform Team"
__email__ = "team@f1dataplatform.com"

from f1_data_platform.config.settings import Settings
from f1_data_platform.cloud_swap.factory import CloudProviderFactory

__all__ = ["Settings", "CloudProviderFactory"]