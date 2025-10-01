"""
Cities data extractor for scraping city information from Espacio Urbano.
"""

import re
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urlparse
from typing import List, Dict
from .base_extractor import BaseExtractor
from config.settings import CITIES_LIST_URL, RENTAL_BASE_URL


class CitiesExtractor(BaseExtractor):
    """Extractor for city data from the main listings page."""
    
    def extract(self) -> List[Dict]:
        """Extract city information from the main cities listing page."""
        self.logger.info("Extracting cities data...")
        
        html_content = self.fetch_page(CITIES_LIST_URL)
        if not html_content:
            self.logger.error("Failed to fetch cities page")
            return []
        
        cities_data = self._parse_cities_page(html_content)
        self.logger.info(f"Extracted {len(cities_data)} cities")
        
        return cities_data
    
    def _parse_cities_page(self, html_content: str) -> List[Dict]:
        """Parse the cities listing page to extract city information."""
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Find all links that match the rental URL pattern
        city_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and href.startswith(RENTAL_BASE_URL):
                city_links.append((href, link.text.strip()))
        
        # Extract city information from URLs
        cities_data = []
        for url, city_name in city_links:
            city_info = self._extract_city_info(url, city_name)
            if city_info:
                cities_data.append(city_info)
        
        return cities_data
    
    def _extract_city_info(self, url: str, city_name: str) -> Dict:
        """Extract city information from URL parameters."""
        try:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            
            return {
                'name': query_params.get('nCiudad', [''])[0] or city_name,
                'code': query_params.get('pCiudad', [''])[0],
                'url': url,
                'extracted_name': city_name
            }
        except Exception as e:
            self.logger.error(f"Error extracting city info from URL {url}: {e}")
            return None

