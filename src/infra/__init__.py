"""
Infrastructure layer for PollPulse TN ETL pipeline.

Provides Supabase client and data management utilities.
"""

from .client import get_supabase_client
from .data_manager import DataSystem

__all__ = ['get_supabase_client', 'DataSystem']

