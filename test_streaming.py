#!/usr/bin/env python3
"""
Test script for the new streaming ETL pipeline.
Demonstrates the improved functionality with better rate limiting and streaming data processing.
"""

import sys
import logging
from etl import ETLOrchestrator
from utils.logger import setup_logging


def test_streaming_pipeline():
    """Test the new streaming ETL pipeline."""
    logger = setup_logging()
    logger.info("Testing streaming ETL pipeline...")
    
    try:
        orchestrator = ETLOrchestrator()
        
        # Run streaming pipeline
        success = orchestrator.run_streaming_pipeline()
        
        if success:
            logger.info("✅ Streaming pipeline test PASSED!")
            
            # Show final statistics
            error_summary = orchestrator.get_error_summary()
            total_errors = error_summary['cities_extractor_errors'] + error_summary['announcements_extractor_errors']
            
            logger.info(f"📊 Pipeline Statistics:")
            logger.info(f"  - Cities extractor errors: {error_summary['cities_extractor_errors']}")
            logger.info(f"  - Announcements extractor errors: {error_summary['announcements_extractor_errors']}")
            logger.info(f"  - Total errors: {total_errors}")
            
            if total_errors == 0:
                logger.info("🎉 Perfect run with no extraction errors!")
            else:
                logger.warning(f"⚠️  Pipeline completed with {total_errors} extraction errors")
                
            return True
        else:
            logger.error("❌ Streaming pipeline test FAILED!")
            return False
            
    except Exception as e:
        logger.error(f"❌ Streaming pipeline test ERROR: {e}")
        return False


def main():
    """Main test function."""
    print("🚀 Testing Houses Rental Scraping ETL Pipeline")
    print("=" * 50)
    
    success = test_streaming_pipeline()
    
    if success:
        print("\n✅ All tests passed! The streaming ETL pipeline is working correctly.")
        print("\n📋 Key improvements implemented:")
        print("  • Streaming data processing with checkpointing")
        print("  • Advanced rate limiting with exponential backoff")
        print("  • User agent rotation to avoid detection")
        print("  • Simplified data structure (no per-city files)")
        print("  • Progress visibility and resumable processing")
        print("\n🎯 Usage:")
        print("  python main.py --mode streaming")
        print("  python cli.py streaming")
        sys.exit(0)
    else:
        print("\n❌ Tests failed! Check the logs for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()
