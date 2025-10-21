"""
Unit tests for ETL transformers.
"""

import unittest
from etl.transform import CitiesTransformer, AnnouncementsTransformer


class TestCitiesTransformer(unittest.TestCase):
    """Test cases for CitiesTransformer."""

    def setUp(self):
        """Set up test fixtures."""
        self.transformer = CitiesTransformer()

    def test_transform_success(self):
        """Test successful cities transformation."""
        raw_cities = [
            {'name': 'Bogotá', 'url': '/city/bogota'},
            {'name': 'Medellín', 'url': '/city/medellin'}
        ]

        transformed_cities = self.transformer.transform(raw_cities)

        self.assertIsInstance(transformed_cities, list)
        self.assertEqual(len(transformed_cities), 2)

        city = transformed_cities[0]
        self.assertIn('id', city)
        self.assertIn('name', city)
        self.assertIn('url', city)
        self.assertIn('created_at', city)

    def test_transform_empty_data(self):
        """Test transformation with empty data."""
        transformed_cities = self.transformer.transform([])

        self.assertEqual(transformed_cities, [])

    def test_transform_invalid_data(self):
        """Test transformation with invalid data."""
        raw_cities = [{'invalid': 'data'}]

        transformed_cities = self.transformer.transform(raw_cities)

        # Should handle gracefully or filter out invalid entries
        self.assertIsInstance(transformed_cities, list)


class TestAnnouncementsTransformer(unittest.TestCase):
    """Test cases for AnnouncementsTransformer."""

    def setUp(self):
        """Set up test fixtures."""
        self.transformer = AnnouncementsTransformer()

    def test_transform_success(self):
        """Test successful announcements transformation."""
        raw_announcements = [
            {
                'id': '123',
                'url': 'https://example.com/listing/123',
                'price': 1200000,
                'city': 'Bogotá',
                'city_id': '1',
                'neighborhood': 'Centro',
                'area': '80m²',
                'rooms': '2',
                'bathrooms': '1',
                'parkings': '1',
                'description': 'Nice apartment in the center',
                'img_url': 'https://example.com/image.jpg',
                'is_featured': False,
                'is_recently_updated': True
            }
        ]

        transformed_announcements = self.transformer.transform(raw_announcements)

        self.assertIsInstance(transformed_announcements, list)
        self.assertEqual(len(transformed_announcements), 1)

        announcement = transformed_announcements[0]
        self.assertIn('id', announcement)
        self.assertIn('url', announcement)
        self.assertIn('price', announcement)
        self.assertIn('city_name', announcement)
        self.assertIn('neighborhood', announcement)
        self.assertIn('area', announcement)
        self.assertIn('rooms', announcement)
        self.assertIn('bathrooms', announcement)
        self.assertIn('description', announcement)
        self.assertIn('property_type', announcement)
        self.assertIn('created_at', announcement)

    def test_transform_empty_data(self):
        """Test transformation with empty data."""
        transformed_announcements = self.transformer.transform([])

        self.assertEqual(transformed_announcements, [])

    def test_transform_price_cleaning(self):
        """Test price cleaning functionality."""
        raw_announcements = [
            {'title': 'Test', 'price': '$1,200,000', 'city': 'Bogotá'},
            {'title': 'Test', 'price': '1.200.000', 'city': 'Bogotá'},
            {'title': 'Test', 'price': 'Negotiable', 'city': 'Bogotá'}
        ]

        transformed = self.transformer.transform(raw_announcements)

        # Should handle different price formats
        self.assertIsInstance(transformed, list)
        for ann in transformed:
            if 'price' in ann:
                self.assertIsInstance(ann['price'], (int, type(None)))


if __name__ == '__main__':
    unittest.main()