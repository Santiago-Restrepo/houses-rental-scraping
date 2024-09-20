import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict
from urllib.parse import parse_qs, urlparse
import re
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
MAX_LISTINGS_PER_PAGE = 50
BASE_URL='https://www.espaciourbano.com'
RENTAL_BASE_URL = '/Resumen_Ciudad_arriendos.asp'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}

# Zones mapping
zones = {
    'apartments': 1,
    'houses': 2,
    'country_houses': 3,
    'studio_apartments': 4,
    'warehouses': 6,
    'locals': 7,
    'offices': 8,
    'lots_and_parcels': 10,
}

class WebScraper:
    def __init__(self, base_url: str, max_workers: int = 5, retries: int = 3, timeout: int = 10):
        self.base_url = base_url
        self.max_workers = max_workers
        self.retries = retries
        self.timeout = timeout
        self.headers = HEADERS

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
        logging.error(f"Failed to fetch URL: {url} after {self.retries} attempts.")
        return ""

class CitiesScraper(WebScraper):
    def extract_data(self, url: str) -> List[Dict]:
        """Scrapes the city links from the page."""
        data_fetched = self.scrape_single_page(url)
        city_links = [link for link in data_fetched['links'] if link[0].startswith(RENTAL_BASE_URL)]
        return [self.extract_city_info(link[0]) for link in city_links]

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

class AnnouncementsScraper(WebScraper):
    def scrape_cities(self, cities: List[Dict]) -> List[Dict]:
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


def main(): 
    """Main function to run the scrapers.""" 
    cities_scraper = CitiesScraper(base_url=BASE_URL) 
    announcements_scraper = AnnouncementsScraper(base_url=BASE_URL)
    # Fetch city data
    logging.info("Fetching city data...")
    cities_url = f'{BASE_URL}/listado_arriendos.asp'
    cities = cities_scraper.extract_data(cities_url)

    # Fetch and process announcements for each city
    logging.info("Fetching listings for all cities...")
    all_announcements = announcements_scraper.scrape_cities(cities)

    # Convert to DataFrame and export to CSV
    df = pd.DataFrame(all_announcements)
    df.to_csv('data/announcements.csv', index=False)
    logging.info("Scraping completed and saved to announcements.csv")

if __name__ == '__main__':
    main()