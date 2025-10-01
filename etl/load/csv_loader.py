"""
CSV loader for saving data to CSV files.
"""

import os
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from .base_loader import BaseLoader


class CSVLoader(BaseLoader):
    """Loader for saving data to CSV files."""
    
    def load(self, data: List[Dict], filename: str, mode: str = 'w') -> bool:
        """Load data to CSV file."""
        if not self._validate_data(data):
            return False
        
        try:
            file_path = self._get_file_path(filename)
            
            # Create DataFrame from data
            df = pd.DataFrame(data)
            
            # Save to CSV
            if mode == 'a' and os.path.exists(file_path):
                # Append mode - don't write header
                df.to_csv(file_path, mode='a', header=False, index=False)
                self.logger.info(f"Appended {len(data)} records to {file_path}")
            else:
                # Write mode - write header
                df.to_csv(file_path, index=False)
                self.logger.info(f"Saved {len(data)} records to {file_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data to CSV {filename}: {e}")
            return False

    def load_cities(self, cities_data: List[Dict], filename: str = None) -> bool:
        """Load cities data to CSV."""
        if filename is None:
            filename = "cities.csv"
        
        return self.load(cities_data, filename)

    def load_announcements(self, announcements_data: List[Dict], filename: str = None) -> bool:
        """Load announcements data to CSV."""
        if filename is None:
            filename = "announcements.csv"
        
        return self.load(announcements_data, filename)

    def load_announcements_streaming(self, announcements_data: List[Dict], filename: str, 
                                   is_first_batch: bool = False) -> bool:
        """
        Load announcements data in streaming fashion (append mode).
        
        Args:
            announcements_data: List of announcement dictionaries
            filename: Target filename
            is_first_batch: Whether this is the first batch (writes header)
            
        Returns:
            True if successful, False otherwise
        """
        if not announcements_data:
            return True  # Empty batch is not an error
        
        try:
            file_path = self._get_file_path(filename)
            
            # Add extraction timestamp to each announcement
            extraction_time = datetime.now().isoformat()
            for announcement in announcements_data:
                announcement['extraction_timestamp'] = extraction_time
            
            # Create DataFrame from data
            df = pd.DataFrame(announcements_data)
            
            # Determine write mode
            write_header = is_first_batch or not os.path.exists(file_path)
            
            # Save to CSV
            df.to_csv(file_path, mode='a', header=write_header, index=False)
            
            self.logger.info(f"Streaming: {'Created' if write_header else 'Appended'} "
                           f"{len(announcements_data)} announcements to {filename}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in streaming save to CSV {filename}: {e}")
            return False

    def load_announcements_by_city(self, announcements_data: List[Dict], city_name: str) -> bool:
        """Load announcements data grouped by city (DEPRECATED - use consolidated approach)."""
        self.logger.warning("load_announcements_by_city is deprecated. Use consolidated approach instead.")
        
        if not announcements_data:
            return False
        
        # Group by city
        cities = {}
        for announcement in announcements_data:
            city = announcement.get('city', 'unknown')
            if city not in cities:
                cities[city] = []
            cities[city].append(announcement)
        
        # Save each city's data separately
        success = True
        for city, city_data in cities.items():
            filename = f"announcements_{city.lower().replace(' ', '_')}.csv"
            if not self.load(city_data, filename):
                success = False
        
        return success
