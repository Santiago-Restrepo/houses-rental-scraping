"""
ETL Orchestrator for coordinating the entire data pipeline.
"""

import logging
from typing import List, Dict
from etl.extract import CitiesExtractor, AnnouncementsExtractor
from etl.transform import CitiesTransformer, AnnouncementsTransformer
from etl.load import CSVLoader
from config.settings import CITIES_FILENAME, ANNOUNCEMENTS_FILENAME


class ETLOrchestrator:
    """Main orchestrator for the ETL pipeline."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize ETL components
        self.cities_extractor = CitiesExtractor()
        self.announcements_extractor = AnnouncementsExtractor()
        self.cities_transformer = CitiesTransformer()
        self.announcements_transformer = AnnouncementsTransformer()
        self.csv_loader = CSVLoader()
    
    def run_full_pipeline(self) -> bool:
        """Run the complete ETL pipeline."""
        self.logger.info("Starting ETL pipeline...")
        
        try:
            # Extract cities data
            self.logger.info("Step 1: Extracting cities data...")
            raw_cities = self.cities_extractor.extract()
            if not raw_cities:
                self.logger.error("Failed to extract cities data")
                return False
            
            # Transform cities data
            self.logger.info("Step 2: Transforming cities data...")
            transformed_cities = self.cities_transformer.transform(raw_cities)
            if not transformed_cities:
                self.logger.error("Failed to transform cities data")
                return False
            
            # Load cities data
            self.logger.info("Step 3: Loading cities data...")
            if not self.csv_loader.load_cities(transformed_cities, CITIES_FILENAME):
                self.logger.error("Failed to load cities data")
                return False
            
            # Extract announcements data
            self.logger.info("Step 4: Extracting announcements data...")
            raw_announcements = self.announcements_extractor.extract(transformed_cities)
            if not raw_announcements:
                self.logger.error("Failed to extract announcements data")
                return False
            
            # Transform announcements data
            self.logger.info("Step 5: Transforming announcements data...")
            transformed_announcements = self.announcements_transformer.transform(raw_announcements)
            if not transformed_announcements:
                self.logger.error("Failed to transform announcements data")
                return False
            
            # Load announcements data
            self.logger.info("Step 6: Loading announcements data...")
            if not self.csv_loader.load_announcements(transformed_announcements, ANNOUNCEMENTS_FILENAME):
                self.logger.error("Failed to load announcements data")
                return False
            
            # Also save by city for easier analysis
            self.logger.info("Step 7: Loading announcements by city...")
            if not self.csv_loader.load_announcements_by_city(transformed_announcements, None):
                self.logger.warning("Failed to save announcements by city (non-critical)")
            
            self.logger.info("ETL pipeline completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            return False
    
    def run_cities_only(self) -> bool:
        """Run only the cities ETL pipeline."""
        self.logger.info("Starting cities-only ETL pipeline...")
        
        try:
            # Extract cities data
            raw_cities = self.cities_extractor.extract()
            if not raw_cities:
                return False
            
            # Transform cities data
            transformed_cities = self.cities_transformer.transform(raw_cities)
            if not transformed_cities:
                return False
            
            # Load cities data
            if not self.csv_loader.load_cities(transformed_cities, CITIES_FILENAME):
                return False
            
            self.logger.info("Cities ETL pipeline completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Cities ETL pipeline failed: {e}")
            return False
    
    def get_error_summary(self) -> Dict:
        """Get a summary of errors encountered during extraction."""
        return {
            'cities_extractor_errors': len(self.cities_extractor.get_error_urls()),
            'announcements_extractor_errors': len(self.announcements_extractor.get_error_urls()),
            'failed_urls': {
                'cities': list(self.cities_extractor.get_error_urls()),
                'announcements': list(self.announcements_extractor.get_error_urls())
            }
        }

