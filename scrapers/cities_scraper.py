from .base_scraper import WebScraper
from typing import List, Dict
from constants.configuration import RENTAL_BASE_URL
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urlparse
from utils.data_handler import save_data_to_csv

class CitiesScraper(WebScraper):
    def extract_data(self, path = '') -> List[Dict]:
        """Scrapes the city links from the page."""
        data_fetched = self.scrape_single_page(f'{self.base_url}/{path}')
        city_links = [link for link in data_fetched['links'] if link[0].startswith(RENTAL_BASE_URL)]
        extracted_data = [self.extract_city_info(link[0]) for link in city_links]
        save_data_to_csv(extracted_data, filename=f'{self.save_data_path}/{self.save_data_file_name}')
        return extracted_data

    def scrape_single_page(self, url: str) -> Dict:
        """Scrapes a single page for links and title."""
        html_content = self.fetch_page(url)
        return self.parse_page(html_content) if html_content else {}

    def parse_page(self, html_content: str) -> Dict:
        """Parses the city listings page."""
        soup = BeautifulSoup(html_content, 'lxml')
        return {
            'title': soup.title.string if soup.title else 'No title',
            'links': [(a.get('href'), a.text) for a in soup.find_all('a', href=True)]
        }

    def extract_city_info(self, url: str) -> dict:
        """Extracts city info from the URL."""
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        return {
            'name': query_params.get('nCiudad', [''])[0],
            'code': query_params.get('pCiudad', [''])[0]
        }
