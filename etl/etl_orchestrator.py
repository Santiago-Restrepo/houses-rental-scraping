"""
ETL Orchestrator for coordinating the entire data pipeline.
"""

import logging
import json
import os
from typing import List, Dict, Optional
from etl.extract import CitiesExtractor, AnnouncementsExtractor
from etl.transform import CitiesTransformer, AnnouncementsTransformer
from etl.load import CSVLoader
from config.settings import (
    CITIES_FILENAME, ANNOUNCEMENTS_FILENAME, DATA_DIR, 
    STREAMING_CONFIG
)


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
        
        # Streaming state
        self.checkpoint_file = os.path.join(DATA_DIR, STREAMING_CONFIG['checkpoint_file'])
        self.processed_cities = set()
        self.total_announcements = 0
        self.is_first_batch = True
    
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
            
            # Note: Per-city files are no longer created to simplify data structure
            # Use the consolidated announcements.csv file for analysis
            
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
    
    def run_streaming_pipeline(self) -> bool:
        """Run the ETL pipeline with streaming data processing."""
        self.logger.info("Starting streaming ETL pipeline...")
        
        try:
            # Load checkpoint if exists
            self._load_checkpoint()
            
            # Step 1: Extract and save cities (if not already done)
            cities = self._get_or_extract_cities()
            if not cities:
                self.logger.error("Failed to get cities data")
                return False
            
            # Filter out already processed cities
            remaining_cities = [city for city in cities if city['id'] not in self.processed_cities]
            
            if not remaining_cities:
                self.logger.info("All cities have already been processed!")
                return True
            
            self.logger.info(f"Processing {len(remaining_cities)} remaining cities "
                           f"(out of {len(cities)} total)")
            
            # Step 2: Stream announcements processing
            success = self._process_announcements_streaming(remaining_cities)
            
            if success:
                # Clean up checkpoint file
                self._cleanup_checkpoint()
                self.logger.info("Streaming ETL pipeline completed successfully!")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Streaming ETL pipeline failed: {e}")
            self._save_checkpoint()  # Save progress before failing
            return False
    
    def _get_or_extract_cities(self) -> Optional[List[Dict]]:
        """Get cities data, extracting if necessary."""
        cities_file = os.path.join(DATA_DIR, CITIES_FILENAME)
        
        if os.path.exists(cities_file):
            self.logger.info("Cities file exists, loading from file...")
            try:
                import pandas as pd
                df = pd.read_csv(cities_file)
                return df.to_dict('records')
            except Exception as e:
                self.logger.warning(f"Failed to load cities from file: {e}")
        
        # Extract cities if file doesn't exist or loading failed
        self.logger.info("Extracting cities data...")
        raw_cities = self.cities_extractor.extract()
        if not raw_cities:
            return None
        
        transformed_cities = self.cities_transformer.transform(raw_cities)
        if not transformed_cities:
            return None
        
        # Save cities data
        if not self.csv_loader.load_cities(transformed_cities, CITIES_FILENAME):
            self.logger.error("Failed to save cities data")
            return None
        
        return transformed_cities
    
    def _process_announcements_streaming(self, cities: List[Dict]) -> bool:
        """Process announcements in streaming fashion."""
        self.logger.info(f"Starting streaming announcements processing for {len(cities)} cities...")
        
        def process_announcements_batch(announcements: List[Dict]):
            """Callback function to process each batch of announcements."""
            if not announcements:
                return
            
            # Transform the announcements
            transformed_announcements = self.announcements_transformer.transform(announcements)
            
            if transformed_announcements:
                # Save in streaming fashion
                success = self.csv_loader.load_announcements_streaming(
                    transformed_announcements, 
                    ANNOUNCEMENTS_FILENAME,
                    is_first_batch=self.is_first_batch
                )
                
                if success:
                    self.total_announcements += len(transformed_announcements)
                    self.is_first_batch = False
                    
                    # Update processed cities
                    city_ids = set(ann['city_id'] for ann in transformed_announcements)
                    self.processed_cities.update(city_ids)
                    
                    # Save checkpoint periodically
                    if len(self.processed_cities) % STREAMING_CONFIG['save_frequency'] == 0:
                        self._save_checkpoint()
                        self.logger.info(f"Checkpoint saved. Processed {len(self.processed_cities)} cities, "
                                       f"{self.total_announcements} total announcements")
                else:
                    self.logger.error("Failed to save announcements batch")
        
        # Use streaming extraction with callback
        total_extracted = self.announcements_extractor.extract_streaming(
            cities, 
            callback=process_announcements_batch
        )
        
        self.logger.info(f"Streaming processing completed. Total announcements extracted: {total_extracted}")
        return True
    
    def _load_checkpoint(self):
        """Load checkpoint data if it exists."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                
                self.processed_cities = set(checkpoint_data.get('processed_cities', []))
                self.total_announcements = checkpoint_data.get('total_announcements', 0)
                self.is_first_batch = checkpoint_data.get('is_first_batch', True)
                
                self.logger.info(f"Loaded checkpoint: {len(self.processed_cities)} cities processed, "
                               f"{self.total_announcements} total announcements")
                
            except Exception as e:
                self.logger.warning(f"Failed to load checkpoint: {e}")
                self._reset_state()
        else:
            self._reset_state()
    
    def _save_checkpoint(self):
        """Save current progress to checkpoint file."""
        try:
            checkpoint_data = {
                'processed_cities': list(self.processed_cities),
                'total_announcements': self.total_announcements,
                'is_first_batch': self.is_first_batch
            }
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def _cleanup_checkpoint(self):
        """Remove checkpoint file after successful completion."""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                self.logger.info("Checkpoint file cleaned up")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup checkpoint: {e}")
    
    def _reset_state(self):
        """Reset streaming state to initial values."""
        self.processed_cities = set()
        self.total_announcements = 0
        self.is_first_batch = True
    
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

