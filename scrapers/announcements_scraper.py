from .base_scraper import WebScraper
from bs4 import BeautifulSoup
import re
from typing import List, Dict
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from constants.configuration import MAX_LISTINGS_PER_PAGE, BASE_URL, RENTAL_BASE_URL
from utils.data_handler import save_data_to_csv
class AnnouncementsScraper(WebScraper):
    def extract_data(self, cities: List[Dict]) -> List[Dict]:
        """Scrapes listings from all cities concurrently."""
        all_announcements = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_city, city) for city in cities]
            for future in as_completed(futures):
                try:
                    announcements = future.result()
                    all_announcements.extend(announcements)
                except Exception as e:
                    logging.error(f"Error scraping city: {e}")
        save_data_to_csv(all_announcements, filename=f'{self.save_data_path}/{self.save_data_file_name}')
        return all_announcements

    def scrape_city(self, city: Dict, start_offset: int = 0) -> List[Dict]:
        """Scrapes all listings for a given city."""
        all_listings = []
        offset = start_offset

        while True:
            url = self.build_city_url(city['code'], offset)
            logging.info(f"Scraping: {url}")
            html_content = self.fetch_page(url)
            listings = self.parse_city_page(html_content, city)

            all_listings.extend(listings)
            if self.is_last_page(html_content):
                logging.info(f"Reached the last page for city {city['name']} (code: {city['code']}).")
                break

            offset += MAX_LISTINGS_PER_PAGE
        save_data_to_csv(all_listings, filename=f'{self.save_data_path}/cities/{city["name"]}.csv')
        return all_listings

    def build_city_url(self, city_code: str, offset: int) -> str:
        """Builds a URL for the city's listings with pagination."""
        return f"{self.base_url}{RENTAL_BASE_URL}?pCiudad={city_code}&pTipoInmueble=&offset={offset}"

    def is_last_page(self, html_content: str) -> bool:
        """Determines if the current page is the last page."""
        soup = BeautifulSoup(html_content, 'lxml')
        pagination_text = soup.find('a', class_='page-link', string=re.compile(r'Página \d+ / \d+'))

        if pagination_text:
            match = re.search(r'Página (\d+) / (\d+)', pagination_text.text)
            if match:
                current_page = int(match.group(1))
                total_pages = int(match.group(2))
                return current_page >= total_pages

        return False

    def parse_city_page(self, html_content: str, city: Dict) -> List[Dict]:
        """Parses a city page to extract individual listings."""
        soup = BeautifulSoup(html_content, 'lxml')
        listing_divs = soup.find_all('div', class_='row')
        return [self.parse_listing(listing, city) for listing in listing_divs if listing]

    def parse_listing(self, listing, city) -> Dict:
        """Parses an individual listing."""
        try:
            listing_id = self.extract_listing_id(listing)
            img_url = self.extract_image_url(listing)
            neighborhood = self.extract_neighborhood(listing)
            price = self.extract_price(listing)
            rooms, bathrooms, parkings, area = self.extract_property_details(listing)
            description = self.extract_description(listing)

            return {
                'id': listing_id,
                'url': f"{self.base_url}/Ficha.asp?xId={listing_id}",
                'neighborhood': neighborhood,
                'img_url': img_url,
                'price': price,
                'rooms': rooms,
                'bathrooms': bathrooms,
                'parkings': parkings,
                'area': area,
                'description': description,
                'city': city['name']
            }
        except Exception as e:
            logging.error(f"Error parsing listing: {e}")
            return {}

    def extract_listing_id(self, listing) -> str:
        """Extracts the listing ID."""
        id_match = re.search(r'Ficha\.asp\?xId=(\d+)', str(listing))
        return id_match.group(1) if id_match else 'N/A'

    def extract_image_url(self, listing) -> str:
        """Extracts the image URL."""
        img_tag = listing.find('img')
        return img_tag['src'] if img_tag else 'N/A'

    def extract_neighborhood(self, listing) -> str:
        """Extracts the neighborhood."""
        subtitle_tag = listing.find('p')
        subtitle = subtitle_tag.text.strip() if subtitle_tag else 'N/A'
        neighborhood_match = re.search(r'Anuncio \d+ - (.+)', subtitle)
        return neighborhood_match.group(1) if neighborhood_match else 'N/A'

    def extract_price(self, listing) -> int:
        """Extracts the price as a numeric value."""
        price_tag = listing.find('h3', string=re.compile(r'\$'))
        price_text = price_tag.text.strip() if price_tag else 'N/A'
        price_numeric = re.sub(r'[^\d]', '', price_text)
        return int(price_numeric) if price_numeric else 'N/A'

    def extract_property_details(self, listing) -> List[str]:
        """Extracts property details such as rooms, bathrooms, parkings, and area."""
        h3_tags = listing.find_all('h3')
        if len(h3_tags) > 1:
            spans = h3_tags[1].find_all('span')
            rooms = spans[0].text.strip() if len(spans) > 0 else 'N/A'
            bathrooms = spans[1].text.strip() if len(spans) > 1 else 'N/A'
            parkings = spans[2].text.strip() if len(spans) > 2 else 'N/A'
            area = spans[3].text.strip() if len(spans) > 3 else 'N/A'
            return [rooms, bathrooms, parkings, area]
        return ['N/A', 'N/A', 'N/A', 'N/A']

    def extract_description(self, listing) -> str:
        """Extracts the description (title)."""
        
        p_tags = listing.find_all('p')
        return max((p.text.strip() for p in p_tags if p.text.strip()), key=len, default='N/A')
