"""
Base transformer class for data cleaning and processing operations.
Provides common functionality for all transformers.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def transform(self, data: List[Dict]) -> List[Dict]:
        """Transform the input data."""
        pass

    def clean_string(self, value: Any) -> str:
        """Clean and standardize string values."""
        if not value or value == 'N/A':
            return ''
        
        if isinstance(value, str):
            return value.strip()
        
        return str(value).strip()

    def clean_numeric(self, value: Any) -> int:
        """Clean and convert values to integers."""
        if not value or value == 'N/A':
            return 0
        
        if isinstance(value, int):
            return value
        
        if isinstance(value, str):
            # Remove non-numeric characters
            numeric_str = ''.join(filter(str.isdigit, value))
            return int(numeric_str) if numeric_str else 0
        
        return 0

    def clean_float(self, value: Any) -> float:
        """Clean and convert values to floats."""
        if not value or value == 'N/A':
            return 0.0
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove non-numeric characters except decimal point
            numeric_str = ''.join(c for c in value if c.isdigit() or c == '.')
            return float(numeric_str) if numeric_str else 0.0
        
        return 0.0

