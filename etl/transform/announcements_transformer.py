"""
Announcements data transformer for cleaning and processing rental property data.
"""

import re
from typing import List, Dict
from .base_transformer import BaseTransformer


class AnnouncementsTransformer(BaseTransformer):
    """Transformer for announcement data cleaning and standardization."""
    
    def transform(self, announcements_data: List[Dict]) -> List[Dict]:
        """Transform and clean announcements data."""
        self.logger.info(f"Transforming {len(announcements_data)} announcements...")
        
        transformed_announcements = []
        for announcement in announcements_data:
            transformed_announcement = self._transform_single_announcement(announcement)
            if transformed_announcement:
                transformed_announcements.append(transformed_announcement)
        
        self.logger.info(f"Successfully transformed {len(transformed_announcements)} announcements")
        return transformed_announcements
    
    def _transform_single_announcement(self, announcement: Dict) -> Dict:
        """Transform a single announcement record."""
        try:
            return {
                'id': self.clean_string(announcement.get('id', '')),
                'url': self.clean_string(announcement.get('url', '')),
                'city_name': self._clean_city_name(announcement.get('city', '')),
                'city_id': self.clean_string(announcement.get('city_id', '')),
                'neighborhood': self._clean_neighborhood(announcement.get('neighborhood', '')),
                'price': self._clean_price(announcement.get('price', 0)),
                'rooms': self._clean_rooms(announcement.get('rooms', '')),
                'bathrooms': self._clean_bathrooms(announcement.get('bathrooms', '')),
                'parkings': self._clean_parkings(announcement.get('parkings', '')),
                'area': self._clean_area(announcement.get('area', '')),
                'description': self._clean_description(announcement.get('description', '')),
                'image_url': self.clean_string(announcement.get('img_url', '')),
                'property_type': self._infer_property_type(announcement),
                'is_active': True,
                'created_at': self._get_current_timestamp()
            }
        except Exception as e:
            self.logger.error(f"Error transforming announcement {announcement.get('id', 'unknown')}: {e}")
            return None
    
    def _clean_city_name(self, city_name: str) -> str:
        """Clean and standardize city names."""
        if not city_name:
            return ''
        
        # Remove extra whitespace and standardize
        cleaned_name = re.sub(r'\s+', ' ', city_name.strip())
        
        # Title case for consistency
        return cleaned_name.title()
    
    def _clean_neighborhood(self, neighborhood: str) -> str:
        """Clean and standardize neighborhood names."""
        if not neighborhood:
            return ''
        
        # Remove extra whitespace and standardize
        cleaned_name = re.sub(r'\s+', ' ', neighborhood.strip())
        
        # Title case for consistency
        return cleaned_name.title()
    
    def _clean_price(self, price: any) -> int:
        """Clean and standardize price values."""
        return self.clean_numeric(price)
    
    def _clean_rooms(self, rooms: any) -> int:
        """Clean and standardize room counts."""
        return self.clean_numeric(rooms)
    
    def _clean_bathrooms(self, bathrooms: any) -> int:
        """Clean and standardize bathroom counts."""
        return self.clean_numeric(bathrooms)
    
    def _clean_parkings(self, parkings: any) -> int:
        """Clean and standardize parking counts."""
        return self.clean_numeric(parkings)
    
    def _clean_area(self, area: any) -> float:
        """Clean and standardize area values."""
        if not area or area == 'N/A':
            return 0.0
        
        if isinstance(area, str):
            # Extract numeric value from area string (e.g., "90 M2" -> 90.0)
            area_match = re.search(r'(\d+(?:\.\d+)?)', str(area))
            if area_match:
                return float(area_match.group(1))
        
        return self.clean_float(area)
    
    def _clean_description(self, description: str) -> str:
        """Clean and standardize descriptions."""
        if not description:
            return ''
        
        # Remove extra whitespace and newlines
        cleaned = re.sub(r'\s+', ' ', description.strip())
        
        # Limit description length to prevent extremely long descriptions
        if len(cleaned) > 1000:
            cleaned = cleaned[:1000] + "..."
        
        return cleaned
    
    def _infer_property_type(self, announcement: Dict) -> str:
        """Infer property type based on available data."""
        description = announcement.get('description', '').lower()
        rooms = self.clean_numeric(announcement.get('rooms', 0))
        area = self._clean_area(announcement.get('area', 0))
        
        # Simple heuristics to infer property type
        if 'apartamento' in description:
            if rooms == 1 or area < 50:
                return 'studio_apartment'
            return 'apartment'
        elif 'casa' in description:
            return 'house'
        elif 'bodega' in description or 'local' in description:
            return 'warehouse'
        elif 'oficina' in description:
            return 'office'
        else:
            # Default based on room count
            if rooms <= 1:
                return 'studio_apartment'
            elif rooms <= 3:
                return 'apartment'
            else:
                return 'house'
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()

