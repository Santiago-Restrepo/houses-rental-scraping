"""
Loading layer for the ETL pipeline.
Contains all loaders for different data destinations.
"""

from .base_loader import BaseLoader
from .csv_loader import CSVLoader
from .sheets_loader import SheetsLoader

__all__ = ['BaseLoader', 'CSVLoader', 'SheetsLoader']

