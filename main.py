"""
Main entry point for the houses rental scraping ETL pipeline.
"""

import sys
import logging
from etl import ETLOrchestrator
from utils.logger import setup_logging


def main():
    """Main function to run the ETL pipeline."""
    import sys
    import argparse
    
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Houses rental scraping ETL pipeline')
    parser.add_argument('--mode', choices=['full', 'streaming', 'cities-only'],
                       default='streaming', help='ETL pipeline mode')
    parser.add_argument('--loader', choices=['csv', 'postgres', 'sheets'],
                       default=None, help='Loader type to use (overrides DEFAULT_LOADER setting)')
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    logger.info(f"Starting houses rental scraping ETL pipeline in {args.mode} mode...")
    
    try:
        # Initialize ETL orchestrator with specified loader
        loader_type = args.loader if args.loader else None
        orchestrator = ETLOrchestrator(loader_type=loader_type)
        
        # Run the appropriate pipeline
        if args.mode == 'streaming':
            success = orchestrator.run_streaming_pipeline()
        elif args.mode == 'cities-only':
            success = orchestrator.run_cities_only()
        else:  # full
            success = orchestrator.run_full_pipeline()
        
        if success:
            logger.info("ETL pipeline completed successfully!")
            
            # Log error summary if there were any errors
            error_summary = orchestrator.get_error_summary()
            total_errors = error_summary['cities_extractor_errors'] + error_summary['announcements_extractor_errors']
            
            if total_errors > 0:
                logger.warning(f"Pipeline completed with {total_errors} extraction errors")
                logger.info("Error summary:")
                logger.info(f"  - Cities extractor errors: {error_summary['cities_extractor_errors']}")
                logger.info(f"  - Announcements extractor errors: {error_summary['announcements_extractor_errors']}")
            else:
                logger.info("Pipeline completed without extraction errors")
                
        else:
            logger.error("ETL pipeline failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()