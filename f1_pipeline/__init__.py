"""F1 Data Analytics Pipeline

A comprehensive, cloud-agnostic data analytics platform for Formula 1 racing data.
"""

__version__ = "1.0.0"
__author__ = "F1 Pipeline Team"
__email__ = "team@f1pipeline.com"

from f1_pipeline.config.settings import Settings
from f1_pipeline.cloud_swap.factory import CloudProviderFactory

__all__ = ["Settings", "CloudProviderFactory"]