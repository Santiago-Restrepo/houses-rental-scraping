"""
Base extractor class for web scraping operations.
Provides common functionality for all extractors.
"""

import requests
import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from config.settings import USER_AGENT, REQUEST_TIMEOUT, REQUEST_RETRIES


class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, max_workers: int = 5, retries: int = REQUEST_RETRIES, timeout: int = REQUEST_TIMEOUT):
        self.max_workers = max_workers
        self.retries = retries
        self.timeout = timeout
        self.headers = {'User-Agent': USER_AGENT}
        self.error_urls = set()
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self, *args, **kwargs) -> List[Dict]:
        """Extract data from the source."""
        pass

    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch a web page with retries and error handling."""
        for attempt in range(self.retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for URL: {url}, Error: {e}")
                if attempt < self.retries - 1:
                    time.sleep(1)
        
        self.error_urls.add(url)
        self.logger.error(f"Failed to fetch URL: {url} after {self.retries} attempts.")
        return None

    def get_error_urls(self) -> set:
        """Get the set of URLs that failed to fetch."""
        return self.error_urls.copy()

