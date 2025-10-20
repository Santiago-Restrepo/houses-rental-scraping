"""
Announcements data extractor for scraping rental property listings.
"""

import re
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base_extractor import BaseExtractor
from config.settings import (
    BASE_URL, RENTAL_BASE_URL, MAX_LISTINGS_PER_PAGE, 
    PAGE_SLEEP_TIME, CITY_SLEEP_TIME, STREAMING_CONFIG
)


class AnnouncementsExtractor(BaseExtractor):
    """Extractor for rental property announcements."""
    
    def extract(self, cities: List[Dict]) -> List[Dict]:
        """Extract announcements from all cities concurrently (legacy method)."""
        self.logger.info(f"Extracting announcements from {len(cities)} cities...")
        
        all_announcements = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._extract_city_announcements, city) for city in cities]
            
            for future in as_completed(futures):
                try:
                    announcements = future.result()
                    all_announcements.extend(announcements)
                except Exception as e:
                    self.logger.error(f"Error extracting announcements: {e}")
        
        self.logger.info(f"Extracted {len(all_announcements)} total announcements")
        return all_announcements

    def extract_streaming(self, cities: List[Dict], callback: Optional[Callable] = None) -> int:
        """
        Extract announcements from cities in streaming fashion with callback.
        
        Args:
            cities: List of city dictionaries
            callback: Optional callback function to process each city's announcements
            
        Returns:
            Total number of announcements extracted
        """
        self.logger.info(f"Starting streaming extraction from {len(cities)} cities...")
        
        total_announcements = 0
        batch_size = STREAMING_CONFIG['batch_size']
        
        # Process cities in batches
        for i in range(0, len(cities), batch_size):
            batch = cities[i:i + batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1}: cities {i+1}-{min(i+batch_size, len(cities))}")
            
            # Process batch with limited concurrency
            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batch))) as executor:
                futures = [executor.submit(self._extract_city_announcements, city) for city in batch]
                
                for future in as_completed(futures):
                    try:
                        announcements = future.result()
                        total_announcements += len(announcements)
                        
                        # Call callback if provided
                        if callback and announcements:
                            callback(announcements)
                            
                    except Exception as e:
                        self.logger.error(f"Error extracting announcements from batch: {e}")
            
            # Add delay between batches to be respectful
            if i + batch_size < len(cities):
                time.sleep(CITY_SLEEP_TIME)
        
        self.logger.info(f"Streaming extraction completed. Total announcements: {total_announcements}")
        return total_announcements
    
    def _extract_city_announcements(self, city: Dict) -> List[Dict]:
        """Extract all announcements for a specific city."""
        city_announcements = []
        offset = 0
        
        self.logger.info(f"Extracting announcements for city: {city['name']} (id: {city['id']})")
        
        while True:
            url = self._build_city_url(city['id'], offset)
            self.logger.debug(f"Fetching page: {url}")
            
            html_content = self.fetch_page(url)
            if not html_content:
                self.logger.error(f"Failed to fetch page for city {city['name']} at offset {offset}")
                break
            
            page_announcements = self._parse_announcements_page(html_content, city)
            city_announcements.extend(page_announcements)
            
            if self._is_last_page(html_content):
                self.logger.info(f"Reached last page for city {city['name']}")
                break
            
            offset += MAX_LISTINGS_PER_PAGE
            time.sleep(PAGE_SLEEP_TIME)
        
        self.logger.info(f"Extracted {len(city_announcements)} announcements for {city['name']}")
        return city_announcements
    
    def _build_city_url(self, city_id: str, offset: int) -> str:
        """Build URL for city listings with pagination."""
        return f"{BASE_URL}{RENTAL_BASE_URL}?pCiudad={city_id}&pTipoInmueble=&nCiudad=&offset={offset}"
    
    def _is_last_page(self, html_content: str) -> bool:
        """Check if the current page is the last page."""
        soup = BeautifulSoup(html_content, 'lxml')
        pagination_text = soup.find('a', class_='page-link', string=re.compile(r'Página \d+ / \d+'))
        
        if pagination_text:
            match = re.search(r'Página (\d+) / (\d+)', pagination_text.text)
            if match:
                current_page = int(match.group(1))
                total_pages = int(match.group(2))
                return current_page >= total_pages
        
        return False
    
    def _parse_announcements_page(self, html_content: str, city: Dict) -> List[Dict]:
        """Parse announcements from a single page."""
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Find announcement containers (adjust selector based on actual HTML structure)
        announcement_divs = soup.find_all('div', class_='row')
        
        announcements = []
        for div in announcement_divs:
            announcement = self._parse_single_announcement(div, city)
            if announcement and announcement['id'] != 'N/A':
                announcements.append(announcement)
        
        return announcements
    
    def _parse_single_announcement(self, announcement_div, city: Dict) -> Dict:
        """Parse a single announcement from HTML."""
        try:
            listing_id = self._extract_listing_id(announcement_div)
            img_url = self._extract_image_url(announcement_div)
            neighborhood = self._extract_neighborhood(announcement_div)
            price = self._extract_price(announcement_div)
            rooms, bathrooms, parkings, area = self._extract_property_details(announcement_div)
            description = self._extract_description(announcement_div)
            is_featured, is_recently_updated = self._extract_tags(announcement_div)
            
            return {
                'id': listing_id,
                'url': f"{BASE_URL}/Ficha.asp?xId={listing_id}",
                'neighborhood': neighborhood,
                'img_url': img_url,
                'price': price,
                'rooms': rooms,
                'bathrooms': bathrooms,
                'parkings': parkings,
                'area': area,
                'description': description,
                'city': city['name'],
                'city_id': city['id'],
                'is_featured': is_featured,
                'is_recently_updated': is_recently_updated
            }
        except Exception as e:
            self.logger.error(f"Error parsing announcement: {e}")
            return None
    
    def _extract_listing_id(self, announcement_div) -> str:
        """Extract the listing ID from the announcement."""
        id_match = re.search(r'Ficha\.asp\?xId=(\d+)', str(announcement_div))
        return id_match.group(1) if id_match else 'N/A'
    
    def _extract_image_url(self, announcement_div) -> str:
        """Extract the image URL from the announcement."""
        img_tag = announcement_div.find('img')
        return img_tag['src'] if img_tag else 'N/A'
    
    def _extract_neighborhood(self, announcement_div) -> str:
        """Extract the neighborhood from the announcement."""
        subtitle_tag = announcement_div.find('p')
        subtitle = subtitle_tag.text.strip() if subtitle_tag else 'N/A'
        neighborhood_match = re.search(r'Anuncio \d+ - (.+)', subtitle)
        return neighborhood_match.group(1) if neighborhood_match else 'N/A'
    
    def _extract_price(self, announcement_div) -> int:
        """Extract the price as a numeric value."""
        price_tag = announcement_div.find('h3', string=re.compile(r'\$'))
        price_text = price_tag.text.strip() if price_tag else 'N/A'
        price_numeric = re.sub(r'[^\d]', '', price_text)
        return int(price_numeric) if price_numeric else 0
    
    def _extract_property_details(self, announcement_div) -> tuple:
        """Extract property details (rooms, bathrooms, parkings, area)."""
        h3_tags = announcement_div.find_all('h3')
        if len(h3_tags) > 1:
            spans = h3_tags[1].find_all('span')
            rooms = spans[0].text.strip() if len(spans) > 0 else 'N/A'
            bathrooms = spans[1].text.strip() if len(spans) > 1 else 'N/A'
            parkings = spans[2].text.strip() if len(spans) > 2 else 'N/A'
            area = spans[3].text.strip() if len(spans) > 3 else 'N/A'
            return (rooms, bathrooms, parkings, area)
        return ('N/A', 'N/A', 'N/A', 'N/A')
    
    def _extract_description(self, announcement_div) -> str:
        """Extract the description from the announcement."""
        p_tags = announcement_div.find_all('p')
        return max((p.text.strip() for p in p_tags if p.text.strip()), key=len, default='N/A')

    def _extract_tags(self, announcement_div) -> tuple:
        """Extract the tags from the announcement."""
        tags = announcement_div.find_all('span', class_='badge')
        text_tags = [tag.text.strip() for tag in tags]
        is_featured = 'Destacado' in text_tags
        is_recently_updated = 'Recien actualizado' in text_tags
        return is_featured, is_recently_updated

