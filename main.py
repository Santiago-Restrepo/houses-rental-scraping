from utils.logging_config import configure_logging
from scrapers.cities_scraper import CitiesScraper
from scrapers.announcements_scraper import AnnouncementsScraper
from constants.configuration import BASE_URL
import logging

def main(): 
    configure_logging()
    """Main function to run the scrapers.""" 
    data_path= 'data'
    cities_file_name = 'cities.csv'
    announcements_file_name = 'announcements.csv'
    cities_scraper = CitiesScraper(base_url=BASE_URL, save_data_path=data_path, save_data_file_name=cities_file_name)
    announcements_scraper = AnnouncementsScraper(base_url=BASE_URL, save_data_path=data_path, save_data_file_name=announcements_file_name)
    
    # Fetch cities data
    extracted_cities = cities_scraper.extract_data(path='listado_arriendos.asp')

    # Fetch announcements data
    announcements_scraper.extract_data(cities=extracted_cities)

    logging.info('Done!')    

if __name__ == '__main__':
    main()