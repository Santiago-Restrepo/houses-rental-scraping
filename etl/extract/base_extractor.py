"""
Base extractor class for web scraping operations.
Provides common functionality for all extractors.
"""

import requests
import logging
import time
import random
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from config.settings import USER_AGENTS, REQUEST_TIMEOUT, REQUEST_RETRIES, RATE_LIMIT_CONFIG


class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, max_workers: int = 3, retries: int = REQUEST_RETRIES, timeout: int = REQUEST_TIMEOUT):
        self.max_workers = max_workers
        self.retries = retries
        self.timeout = timeout
        self.error_urls = set()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_count = 0
        self.last_request_time = 0
        self.backoff_delay = RATE_LIMIT_CONFIG['base_delay']

    @abstractmethod
    def extract(self, *args, **kwargs) -> List[Dict]:
        """Extract data from the source."""
        pass

    def _get_random_headers(self) -> Dict[str, str]:
        """Get headers with random user agent."""
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def _apply_rate_limiting(self):
        """Apply rate limiting with jitter."""
        current_time = time.time()
        
        # Check if we need to wait based on requests per minute
        if self.request_count > 0:
            time_since_last = current_time - self.last_request_time
            min_interval = 60.0 / RATE_LIMIT_CONFIG['requests_per_minute']
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                # Add jitter to avoid patterns
                jitter = random.uniform(*RATE_LIMIT_CONFIG['jitter_range'])
                time.sleep(sleep_time + jitter)
        
        self.last_request_time = time.time()
        self.request_count += 1

    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch a web page with retries, rate limiting, and error handling."""
        self._apply_rate_limiting()
        
        for attempt in range(self.retries):
            try:
                headers = self._get_random_headers()
                response = requests.get(url, headers=headers, timeout=self.timeout)
                
                # Handle rate limiting responses
                if response.status_code == 429:
                    self.logger.warning(f"Rate limited (429) for URL: {url}. Applying backoff...")
                    time.sleep(self.backoff_delay)
                    self.backoff_delay = min(
                        self.backoff_delay * RATE_LIMIT_CONFIG['backoff_multiplier'],
                        RATE_LIMIT_CONFIG['max_backoff']
                    )
                    continue
                elif response.status_code == 503:
                    self.logger.warning(f"Service unavailable (503) for URL: {url}. Retrying...")
                    time.sleep(self.backoff_delay)
                    continue
                
                response.raise_for_status()
                
                # Reset backoff delay on successful request
                self.backoff_delay = RATE_LIMIT_CONFIG['base_delay']
                return response.text
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for URL: {url}, Error: {e}")
                if attempt < self.retries - 1:
                    # Exponential backoff with jitter
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
        
        self.error_urls.add(url)
        self.logger.error(f"Failed to fetch URL: {url} after {self.retries} attempts.")
        return None

    def get_error_urls(self) -> set:
        """Get the set of URLs that failed to fetch."""
        return self.error_urls.copy()

