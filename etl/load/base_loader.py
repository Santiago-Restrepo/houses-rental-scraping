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
    
    def __init__(self, data_dir: str = DATA_DIR):
        self.data_dir = data_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self._ensure_data_directory()

    @abstractmethod
    def load(self, data: List[Dict], filename: str) -> bool:
        """Load data to the destination."""
        pass

    def _ensure_data_directory(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            self.logger.info(f"Created data directory: {self.data_dir}")

    def _get_file_path(self, filename: str) -> str:
        """Get the full file path."""
        return os.path.join(self.data_dir, filename)

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

