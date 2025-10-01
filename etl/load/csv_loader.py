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
    
    def load(self, data: List[Dict], filepath: str, mode: str = 'w') -> bool:
        """Load data to CSV file."""
        if not self._validate_data(data):
            return False
        try:
            # Create DataFrame from data
            df = pd.DataFrame(data)
            
            # Save to CSV
            if mode == 'a' and os.path.exists(filepath):
                # Append mode - don't write header
                df.to_csv(filepath, mode='a', header=False, index=False)
                self.logger.info(f"Appended {len(data)} records to {filepath}")
            else:
                # Write mode - write header
                df.to_csv(filepath, index=False)
                self.logger.info(f"Saved {len(data)} records to {filepath}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data to CSV {filepath}: {e}")
            return False

    def load_cities(self, cities_data: List[Dict], filepath: str = None) -> bool:
        """Load cities data to CSV."""        
        
        return self.load(cities_data, filepath)

    def load_announcements(self, announcements_data: List[Dict], filepath: str = None) -> bool:
        """Load announcements data to CSV."""
        
        return self.load(announcements_data, filepath)

    def load_announcements_streaming(self, announcements_data: List[Dict], filepath: str, 
                                   is_first_batch: bool = False) -> bool:
        """
        Load announcements data in streaming fashion (append mode).
        
        Args:
            announcements_data: List of announcement dictionaries
            filepath: Target filepath
            is_first_batch: Whether this is the first batch (writes header)
            
        Returns:
            True if successful, False otherwise
        """
        if not announcements_data:
            return True  # Empty batch is not an error
        
        try:            
            # Add extraction timestamp to each announcement
            extraction_time = datetime.now().isoformat()
            for announcement in announcements_data:
                announcement['extraction_timestamp'] = extraction_time
            
            # Create DataFrame from data
            df = pd.DataFrame(announcements_data)
            
            # Determine write mode
            write_header = is_first_batch or not os.path.exists(filepath)
            
            # Save to CSV
            df.to_csv(filepath, mode='a', header=write_header, index=False)
            
            self.logger.info(f"Streaming: {'Created' if write_header else 'Appended'} "
                           f"{len(announcements_data)} announcements to {filepath}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in streaming save to CSV {filepath}: {e}")
            return False