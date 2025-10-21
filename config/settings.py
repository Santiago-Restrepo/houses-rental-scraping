"""
Configuration settings for the houses rental scraping project.
Contains all URLs, scraping parameters, and application settings.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base URLs
BASE_URL = "https://www.espaciourbano.com"
CITIES_LIST_URL = f"{BASE_URL}/listado_arriendos.asp"
RENTAL_BASE_URL = "/Resumen_Ciudad_arriendos.asp"

# Scraping Configuration
MAX_LISTINGS_PER_PAGE = int(os.getenv("MAX_LISTINGS_PER_PAGE", 50))
PAGE_SLEEP_TIME = int(os.getenv("PAGE_SLEEP_TIME", 2))  # seconds between page requests
CITY_SLEEP_TIME = int(os.getenv("CITY_SLEEP_TIME", 5))  # seconds between cities
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 3))  # Reduced to be more respectful
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 15))  # Increased timeout
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", 3))

# Rate Limiting Configuration
RATE_LIMIT_CONFIG = {
    'requests_per_minute': 30,
    'concurrent_requests': 3,
    'backoff_multiplier': 2,
    'max_backoff': 60,
    'base_delay': 1,
    'jitter_range': (0.5, 2.0)  # Random jitter to avoid patterns
}

# User Agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0"
]

# Streaming Configuration
STREAMING_CONFIG = {
    'batch_size': 5,  # Process 5 cities at a time
    'save_frequency': 10,  # Save every 10 cities processed
    'checkpoint_file': 'etl_checkpoint.json'
}

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

# Google Sheets Configuration
CREDS_PATH = os.getenv("CREDS_PATH")
SHEETS_KEY = os.getenv("SHEETS_KEY")

# PostgreSQL Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

# Loader Configuration
DEFAULT_LOADER = "csv"  # Options: "csv", "postgres", "sheets"

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

