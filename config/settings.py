"""
Configuration settings for the houses rental scraping project.
Contains all URLs, scraping parameters, and application settings.
"""

# Base URLs
BASE_URL = "https://www.espaciourbano.com"
CITIES_LIST_URL = f"{BASE_URL}/listado_arriendos.asp"
RENTAL_BASE_URL = "/Resumen_Ciudad_arriendos.asp"

# Scraping Configuration
MAX_LISTINGS_PER_PAGE = 50
PAGE_SLEEP_TIME = 2  # seconds between page requests
MAX_WORKERS = 5
REQUEST_TIMEOUT = 10
REQUEST_RETRIES = 3

# Data Storage Configuration
DATA_DIR = "data"
CITIES_FILENAME = "cities.csv"
ANNOUNCEMENTS_FILENAME = "announcements.csv"

# User Agent for web requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Property Types Configuration
PROPERTY_TYPES = {
    'apartments': 1,
    'houses': 2,
    'country_houses': 3,
    'studio_apartments': 4,
    'warehouses': 6,
    'locals': 7,
    'offices': 8,
    'lots_and_parcels': 10,
}

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

