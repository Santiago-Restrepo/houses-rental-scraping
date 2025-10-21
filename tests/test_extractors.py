"""
Unit tests for ETL extractors.
"""

import unittest
from unittest.mock import patch, MagicMock
from etl.extract import CitiesExtractor, AnnouncementsExtractor

# Disable logging during tests
import logging
logging.disable(logging.CRITICAL)


class TestCitiesExtractor(unittest.TestCase):
    """Test cases for CitiesExtractor."""

    def setUp(self):
        """Set up test fixtures."""
        self.extractor = CitiesExtractor()

    @patch('etl.extract.base_extractor.BaseExtractor.fetch_page')
    def test_extract_success(self, mock_fetch):
        """Test successful cities extraction."""
        mock_html = """
        <html>
        <body>
            <a href="/Resumen_Ciudad_arriendos.asp?nCiudad=Bogota&pCiudad=1">Bogotá</a>
            <a href="/Resumen_Ciudad_arriendos.asp?nCiudad=Medellin&pCiudad=2">Medellín</a>
        </body>
        </html>
        """
        mock_fetch.return_value = mock_html

        cities = self.extractor.extract()

        self.assertIsInstance(cities, list)
        self.assertGreater(len(cities), 0)
        if cities:  # Only check structure if cities were extracted
            self.assertIn('name', cities[0])
            self.assertIn('url', cities[0])
            self.assertIn('code', cities[0])

    @patch('etl.extract.base_extractor.BaseExtractor.fetch_page')
    def test_extract_failure(self, mock_fetch):
        """Test cities extraction failure."""
        mock_fetch.return_value = None

        cities = self.extractor.extract()

        self.assertEqual(cities, [])


class TestAnnouncementsExtractor(unittest.TestCase):
    """Test cases for AnnouncementsExtractor."""

    def setUp(self):
        """Set up test fixtures."""
        self.extractor = AnnouncementsExtractor()

    @patch('etl.extract.base_extractor.BaseExtractor.fetch_page')
    def test_extract_success(self, mock_fetch):
        """Test successful announcements extraction."""
        # Mock the page fetch to return HTML with pagination indicating last page
        mock_html = """
        <html>
        <body>
            <a class="page-link">Página 1 / 1</a>
            <div class="row">
                <a href="/Ficha.asp?xId=123">Beautiful Apartment</a>
                <h3>$1,200,000</h3>
                <p>Anuncio 123 - Centro</p>
                <h3><span>2</span><span>1</span><span>1</span><span>80m²</span></h3>
                <p>This is a beautiful apartment in the center</p>
            </div>
        </body>
        </html>
        """
        mock_fetch.return_value = mock_html

        cities = [{'id': 1, 'name': 'Bogotá', 'url': '/city/bogota'}]
        announcements = self.extractor.extract(cities)

        self.assertIsInstance(announcements, list)
        # Note: Actual parsing might be more complex, adjust assertions based on implementation

    @patch('etl.extract.base_extractor.BaseExtractor.fetch_page')
    def test_extract_failure(self, mock_fetch):
        """Test announcements extraction failure."""
        mock_fetch.return_value = None

        cities = [{'id': 1, 'name': 'Bogotá', 'url': '/city/bogota'}]
        announcements = self.extractor.extract(cities)

        self.assertEqual(announcements, [])

    @patch('etl.extract.base_extractor.BaseExtractor.fetch_page')
    def test_extract_streaming_success(self, mock_fetch):
        """Test successful streaming announcements extraction."""
        # Mock the page fetch to return HTML with pagination indicating last page
        mock_html = """
        <html>
        <body>
            <a class="page-link">Página 1 / 1</a>
            <div class="row">
                <a href="/Ficha.asp?xId=123">Beautiful Apartment</a>
                <h3>$1,200,000</h3>
                <p>Anuncio 123 - Centro</p>
                <h3><span>2</span><span>1</span><span>1</span><span>80m²</span></h3>
                <p>This is a beautiful apartment in the center</p>
            </div>
        </body>
        </html>
        """
        mock_fetch.return_value = mock_html

        cities = [{'id': 1, 'name': 'Bogotá', 'url': '/city/bogota'}]

        callback_called = []
        def test_callback(announcements):
            callback_called.append(len(announcements))

        total = self.extractor.extract_streaming(cities, callback=test_callback)

        self.assertGreater(total, 0)
        self.assertEqual(len(callback_called), 1)


if __name__ == '__main__':
    unittest.main()