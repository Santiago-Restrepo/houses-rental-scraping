"""
Main entry point for the houses rental scraping ETL pipeline.
"""

import sys
import logging
from etl import ETLOrchestrator
from utils.logger import setup_logging


def main():
    """Main function to run the ETL pipeline."""
    # Setup logging
    logger = setup_logging()
    logger.info("Starting houses rental scraping ETL pipeline...")
    
    try:
        # Initialize ETL orchestrator
        orchestrator = ETLOrchestrator()
        
        # Run the full ETL pipeline
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