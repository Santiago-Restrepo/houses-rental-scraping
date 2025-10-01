"""
Cities data transformer for cleaning and processing city information.
"""

import re
from typing import List, Dict
from .base_transformer import BaseTransformer


class CitiesTransformer(BaseTransformer):
    """Transformer for city data cleaning and standardization."""
    
    def transform(self, cities_data: List[Dict]) -> List[Dict]:
        """Transform and clean cities data."""
        self.logger.info(f"Transforming {len(cities_data)} cities...")
        
        transformed_cities = []
        for city in cities_data:
            transformed_city = self._transform_single_city(city)
            if transformed_city:
                transformed_cities.append(transformed_city)
        
        self.logger.info(f"Successfully transformed {len(transformed_cities)} cities")
        return transformed_cities
    
    def _transform_single_city(self, city: Dict) -> Dict:
        """Transform a single city record."""
        try:
            return {
                'id': self.clean_string(city.get('code', '')),
                'name': self._clean_city_name(city.get('name', '')),
                'url': self.clean_string(city.get('url', '')),
                'is_active': True,
                'created_at': self._get_current_timestamp()
            }
        except Exception as e:
            self.logger.error(f"Error transforming city {city}: {e}")
            return None
    
    def _clean_city_name(self, city_name: str) -> str:
        """Clean and standardize city names."""
        if not city_name:
            return ''
        
        # Remove extra whitespace and standardize
        cleaned_name = re.sub(r'\s+', ' ', city_name.strip())
        
        # Title case for consistency
        return cleaned_name.title()
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()

