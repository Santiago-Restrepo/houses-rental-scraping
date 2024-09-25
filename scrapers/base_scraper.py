import requests
from abc import ABC, abstractmethod
import logging
import time
from typing import List, Dict

# Constants
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}


class WebScraper(ABC):
    def __init__(self, base_url: str, save_data_path: str, save_data_file_name: str, max_workers: int = 5, retries: int = 3, timeout: int = 10):
        self.base_url = base_url
        self.max_workers = max_workers
        self.retries = retries
        self.timeout = timeout
        self.headers = HEADERS
        self.save_data_path = save_data_path
        self.save_data_file_name = save_data_file_name
        self.error_urls = set()

    @abstractmethod
    def extract_data(self, url: str) -> List[Dict]:
        pass

    def fetch_page(self, url: str) -> str:
        """Fetches a web page with retries."""
        for attempt in range(self.retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for URL: {url}, Error: {e}")
                time.sleep(1)
        self.error_urls.add(url)
        logging.error(f"Failed to fetch URL: {url} after {self.retries} attempts.")
        return ""