"""
Base loader class for data persistence operations.
Provides common functionality for all loaders.
"""

import os
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from config.settings import DATA_DIR


class BaseLoader(ABC):
    """Base class for all data loaders."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def load(self, data: List[Dict], filepath: str) -> bool:
        """Load data to the destination."""
        pass

    def _validate_data(self, data: List[Dict]) -> bool:
        """Validate that data is in the expected format."""
        if not isinstance(data, list):
            self.logger.error("Data must be a list")
            return False
        
        if not data:
            self.logger.warning("No data to load")
            return False
        
        if not isinstance(data[0], dict):
            self.logger.error("Data items must be dictionaries")
            return False
        
        return True

