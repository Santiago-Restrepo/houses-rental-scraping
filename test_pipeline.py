"""
Simple test script to verify the ETL pipeline components work correctly.
"""

import sys
import logging
from etl.extract import CitiesExtractor, AnnouncementsExtractor
from etl.transform import CitiesTransformer, AnnouncementsTransformer
from etl.load import CSVLoader
from utils.logger import setup_logging


def test_cities_extraction():
    """Test cities extraction."""
    print("Testing cities extraction...")
    
    extractor = CitiesExtractor()
    cities = extractor.extract()
    
    print(f"Extracted {len(cities)} cities")
    if cities:
        print(f"Sample city: {cities[0]}")
    
    return cities


def test_cities_transformation(cities):
    """Test cities transformation."""
    print("\nTesting cities transformation...")
    
    transformer = CitiesTransformer()
    transformed_cities = transformer.transform(cities)
    
    print(f"Transformed {len(transformed_cities)} cities")
    if transformed_cities:
        print(f"Sample transformed city: {transformed_cities[0]}")
    
    return transformed_cities


def test_cities_loading(cities):
    """Test cities loading."""
    print("\nTesting cities loading...")
    
    loader = CSVLoader()
    success = loader.load_cities(cities, "test_cities.csv")
    
    print(f"Loading successful: {success}")
    return success


def test_announcements_extraction(cities):
    """Test announcements extraction (limited)."""
    print("\nTesting announcements extraction (first city only)...")
    
    # Test with only the first city to avoid long execution
    test_cities = cities[:1] if cities else []
    
    extractor = AnnouncementsExtractor()
    announcements = extractor.extract(test_cities)
    
    print(f"Extracted {len(announcements)} announcements")
    if announcements:
        print(f"Sample announcement: {announcements[0]}")
    
    return announcements


def test_announcements_transformation(announcements):
    """Test announcements transformation."""
    print("\nTesting announcements transformation...")
    
    transformer = AnnouncementsTransformer()
    transformed_announcements = transformer.transform(announcements)
    
    print(f"Transformed {len(transformed_announcements)} announcements")
    if transformed_announcements:
        print(f"Sample transformed announcement: {transformed_announcements[0]}")
    
    return transformed_announcements


def test_announcements_loading(announcements):
    """Test announcements loading."""
    print("\nTesting announcements loading...")
    
    loader = CSVLoader()
    success = loader.load_announcements(announcements, "test_announcements.csv")
    
    print(f"Loading successful: {success}")
    return success


def main():
    """Run all tests."""
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    print("Running ETL Pipeline Component Tests")
    print("=" * 40)
    
    try:
        # Test cities pipeline
        cities = test_cities_extraction()
        if not cities:
            print("Cities extraction failed - stopping tests")
            return False
        
        transformed_cities = test_cities_transformation(cities)
        if not transformed_cities:
            print("Cities transformation failed - stopping tests")
            return False
        
        cities_loading_success = test_cities_loading(transformed_cities)
        if not cities_loading_success:
            print("Cities loading failed - stopping tests")
            return False
        
        # Test announcements pipeline (limited)
        announcements = test_announcements_extraction(transformed_cities)
        if announcements:
            transformed_announcements = test_announcements_transformation(announcements)
            if transformed_announcements:
                announcements_loading_success = test_announcements_loading(transformed_announcements)
                if not announcements_loading_success:
                    print("Announcements loading failed")
                    return False
        
        print("\n" + "=" * 40)
        print("All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

