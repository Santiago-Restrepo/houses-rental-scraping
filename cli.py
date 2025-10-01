"""
Command-line interface for the ETL pipeline.
Allows running different parts of the pipeline independently.
"""

import argparse
import sys
import logging
from etl import ETLOrchestrator
from utils.logger import setup_logging


def run_full_pipeline():
    """Run the complete ETL pipeline."""
    logger = setup_logging()
    logger.info("Running full ETL pipeline...")
    
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_full_pipeline()
    
    if not success:
        logger.error("Full pipeline failed!")
        sys.exit(1)
    
    logger.info("Full pipeline completed successfully!")


def run_cities_only():
    """Run only the cities extraction pipeline."""
    logger = setup_logging()
    logger.info("Running cities-only pipeline...")
    
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_cities_only()
    
    if not success:
        logger.error("Cities pipeline failed!")
        sys.exit(1)
    
    logger.info("Cities pipeline completed successfully!")


def run_streaming_pipeline():
    """Run the streaming ETL pipeline."""
    logger = setup_logging()
    logger.info("Running streaming ETL pipeline...")
    
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_streaming_pipeline()
    
    if not success:
        logger.error("Streaming pipeline failed!")
        sys.exit(1)
    
    logger.info("Streaming pipeline completed successfully!")


def show_error_summary():
    """Show error summary from the last run."""
    logger = setup_logging()
    
    orchestrator = ETLOrchestrator()
    error_summary = orchestrator.get_error_summary()
    
    print("Error Summary:")
    print(f"  Cities extractor errors: {error_summary['cities_extractor_errors']}")
    print(f"  Announcements extractor errors: {error_summary['announcements_extractor_errors']}")
    
    if error_summary['failed_urls']['cities']:
        print("\nFailed cities URLs:")
        for url in error_summary['failed_urls']['cities']:
            print(f"  - {url}")
    
    if error_summary['failed_urls']['announcements']:
        print("\nFailed announcements URLs:")
        for url in error_summary['failed_urls']['announcements']:
            print(f"  - {url}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Houses Rental Scraping ETL Pipeline')
    parser.add_argument('command', choices=['full', 'streaming', 'cities', 'errors'], 
                       help='Command to run: full (complete pipeline), streaming (streaming pipeline), cities (cities only), errors (show error summary)')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'full':
            run_full_pipeline()
        elif args.command == 'streaming':
            run_streaming_pipeline()
        elif args.command == 'cities':
            run_cities_only()
        elif args.command == 'errors':
            show_error_summary()
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

