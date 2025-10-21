"""
Unit tests for ETL loaders.
"""

import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock
from etl.load import CSVLoader, PostgresLoader


class TestCSVLoader(unittest.TestCase):
    """Test cases for CSVLoader."""

    def setUp(self):
        """Set up test fixtures."""
        self.loader = CSVLoader()

    def test_load_cities_success(self):
        """Test successful cities loading to CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            cities_data = [
                {'id': '1', 'name': 'Bogotá', 'url': '/city/bogota', 'created_at': '2023-01-01'},
                {'id': '2', 'name': 'Medellín', 'url': '/city/medellin', 'created_at': '2023-01-01'}
            ]

            success = self.loader.load_cities(cities_data, temp_path)

            self.assertTrue(success)
            self.assertTrue(os.path.exists(temp_path))

            # Check file contents
            with open(temp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.assertIn('Bogotá', content)
                self.assertIn('Medellín', content)

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_load_announcements_success(self):
        """Test successful announcements loading to CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            announcements_data = [
                {
                    'id': '123',
                    'url': 'https://example.com/listing/123',
                    'price': 1200000,
                    'city_name': 'Bogotá',
                    'neighborhood': 'Centro',
                    'area': 80.0,
                    'rooms': 2,
                    'bathrooms': 1,
                    'created_at': '2023-01-01'
                }
            ]

            success = self.loader.load_announcements(announcements_data, temp_path)

            self.assertTrue(success)
            self.assertTrue(os.path.exists(temp_path))

            # Check file contents
            with open(temp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.assertIn('123', content)
                self.assertIn('Bogotá', content)

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_load_invalid_data(self):
        """Test loading with invalid data."""
        success = self.loader.load_cities(None, '/invalid/path')
        self.assertFalse(success)


class TestPostgresLoader(unittest.TestCase):
    """Test cases for PostgresLoader."""

    def setUp(self):
        """Set up test fixtures."""
        self.loader = PostgresLoader('postgresql://test:test@localhost:5432/test')

    @patch('psycopg2.connect')
    def test_load_success(self, mock_connect):
        """Test successful loading to PostgreSQL."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Mock the execute_values function
        with patch('psycopg2.extras.execute_values'):
            data = [{
                'provider_listing_id': '123',
                'title': 'Test Apartment',
                'property_type': 'Apartment',
                'city': 'Bogotá',
                'neighborhood': 'Centro',
                'price': 1000000,
                'currency': 'COP',
                'rooms': 2,
                'bathrooms': 1,
                'parkings': 1,
                'area_m2': 80,
                'link': 'https://example.com/123',
                'image_url': 'https://example.com/image.jpg',
                'features': {},
                'metadata': {},
                'first_seen': '2023-01-01',
                'last_seen': '2023-01-01',
                'active': True
            }]
            success = self.loader.load(data, 'listings')

            self.assertTrue(success)

    @patch('psycopg2.connect')
    def test_load_failure(self, mock_connect):
        """Test loading failure."""
        mock_connect.side_effect = Exception('Connection failed')

        data = [{'id': 1, 'name': 'Test'}]
        success = self.loader.load(data, 'test_table')

        self.assertFalse(success)




if __name__ == '__main__':
    unittest.main()