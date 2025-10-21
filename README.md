# Houses Rental Scraping ETL Pipeline

A well-architected ETL (Extract, Transform, Load) pipeline for scraping rental property data from Espacio Urbano website.

## 🏗️ Architecture

The project follows a clean ETL architecture with three distinct layers:

### 📥 Extract Layer (`etl/extract/`)
- **`BaseExtractor`**: Abstract base class for all extractors
- **`CitiesExtractor`**: Extracts city information from the main listings page
- **`AnnouncementsExtractor`**: Extracts rental property announcements from city pages

### 🔄 Transform Layer (`etl/transform/`)
- **`BaseTransformer`**: Abstract base class for all transformers
- **`CitiesTransformer`**: Cleans and standardizes city data
- **`AnnouncementsTransformer`**: Cleans and standardizes property announcement data

### 💾 Load Layer (`etl/load/`)
- **`BaseLoader`**: Abstract base class for all loaders
- **`CSVLoader`**: Saves data to CSV files with various options
- **`PostgresLoader`**: Saves data to PostgreSQL database with upsert logic
- **`SheetsLoader`**: Saves data to Google Sheets

### 🎯 Orchestration (`etl/etl_orchestrator.py`)
- **`ETLOrchestrator`**: Coordinates the entire pipeline execution with strategy pattern for loaders

## 🚀 Quick Start

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd houses-rental-scraping
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your specific configuration values
```

### Running the Pipeline

#### Option 1: Run with different modes (Recommended)
```bash
# Streaming mode (default) - processes data incrementally with checkpointing
python main.py --mode streaming

# Full mode - traditional batch processing
python main.py --mode full

# Cities only - extract and save cities data only
python main.py --mode cities-only

# Specify loader type (overrides DEFAULT_LOADER setting)
python main.py --mode streaming --loader postgres
python main.py --mode full --loader csv
python main.py --mode streaming --loader sheets
```

#### Option 2: Use the CLI interface
```bash
# Run streaming pipeline (recommended)
python cli.py streaming

# Run complete pipeline
python cli.py full

# Run only cities extraction
python cli.py cities

# Show error summary from last run
python cli.py errors

# Specify loader type (overrides DEFAULT_LOADER setting)
python cli.py streaming --loader postgres
python cli.py full --loader csv
python cli.py cities --loader sheets
```

## 📁 Project Structure

```
houses-rental-scraping/
├── config/
│   └── settings.py          # Configuration settings with hardcoded URLs
├── etl/
│   ├── __init__.py
│   ├── etl_orchestrator.py  # Main ETL orchestrator
│   ├── extract/             # Extraction layer
│   │   ├── __init__.py
│   │   ├── base_extractor.py
│   │   ├── cities_extractor.py
│   │   └── announcements_extractor.py
│   ├── transform/           # Transformation layer
│   │   ├── __init__.py
│   │   ├── base_transformer.py
│   │   ├── cities_transformer.py
│   │   └── announcements_transformer.py
│   └── load/               # Loading layer
│       ├── __init__.py
│       ├── base_loader.py
│       └── csv_loader.py
├── utils/
│   └── logger.py           # Logging configuration
├── main.py                 # Main entry point
├── cli.py                  # Command-line interface
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## ⚙️ Configuration

All configuration is centralized in `config/settings.py` and can be overridden using environment variables:

- **URLs**: Base URLs for scraping (hardcoded for reliability)
- **Scraping Parameters**: Page limits, sleep times, timeouts (configurable via various env vars)
- **Data Storage**: Output directories and filenames
- **Property Types**: Mapping of property type codes
- **Loader Configuration**: Default loader type (hardcoded as "csv")
- **Database Configuration**: PostgreSQL connection string (configurable via `DATABASE_URL`)
- **Google Sheets**: Credentials path and sheet key (configurable via `CREDS_PATH` and `SHEETS_KEY`)
- **Logging**: Log level and format (hardcoded defaults)

### Environment Variables

Copy `.env.example` to `.env` and modify the values as needed:

```bash
cp .env.example .env
```

Key environment variables:
- `DATABASE_URL`: PostgreSQL connection string (contains sensitive credentials)
- `SHEETS_KEY`: Google Sheets document ID
- `CREDS_PATH`: Path to Google service account credentials
- `MAX_LISTINGS_PER_PAGE`: Maximum listings to scrape per page (default: 50)
- `PAGE_SLEEP_TIME`: Delay between page requests (default: 2)
- `CITY_SLEEP_TIME`: Delay between cities (default: 5)
- `MAX_WORKERS`: Number of concurrent threads (default: 3)
- `REQUEST_TIMEOUT`: HTTP request timeout (default: 15)
- `REQUEST_RETRIES`: Number of retry attempts for failed requests (default: 3)

## 📊 Output Data

The pipeline can save data to multiple destinations based on the selected loader:

### CSV Files (when using `csv` loader)

### `data/cities.csv`
Contains city information:
- `id`: Unique city identifier
- `name`: Cleaned city name
- `url`: Source URL
- `is_active`: Whether the city is active
- `created_at`: Timestamp

### `data/announcements.csv`
Contains rental property announcements:
- `id`: Unique announcement identifier
- `url`: Link to full announcement
- `city`: Associated city name
- `city_id`: Associated city ID
- `neighborhood`: Property neighborhood
- `price`: Monthly rent price
- `rooms`: Number of rooms
- `bathrooms`: Number of bathrooms
- `parkings`: Number of parking spaces
- `area`: Property area
- `description`: Property description
- `img_url`: Property image URL
- `extraction_timestamp`: When the data was extracted

### PostgreSQL Database (when using `postgres` loader)
The pipeline creates and maintains two tables:

#### `listings` table
- `id`: UUID primary key
- `provider_listing_id`: Unique identifier from the source
- `title`: Property title
- `property_type`: Apartment, house, etc.
- `city`: City name
- `neighborhood`: Neighborhood name
- `price`: Current price
- `currency`: Currency code (COP)
- `rooms`, `bathrooms`, `area_m2`: Property details
- `features`: JSON object with additional features
- `metadata`: JSON object with extraction metadata
- `first_seen`, `last_seen`: Timestamps
- `active`: Boolean status

#### `listing_snapshots` table
- `id`: Serial primary key
- `listing_id`: Foreign key to listings
- `scraped_at`: Timestamp of scrape
- `price`: Price at scrape time
- `status`: Active/inactive status
- `raw_json`: Complete raw data

### Google Sheets (when using `sheets` loader)
Data is saved to a Google Sheet specified by `SHEETS_KEY` in configuration.

## 🆕 New Features (v3.0)

### Strategy Pattern for Loaders
- **Multiple Destinations**: Choose between CSV, PostgreSQL, or Google Sheets
- **Runtime Configuration**: Switch loaders without code changes
- **Independent Operation**: Each loader works completely independently
- **Extensible Design**: Easy to add new loaders by implementing `BaseLoader`

### PostgreSQL Integration
- **Upsert Logic**: Automatically handles new and existing listings
- **Data Normalization**: Transforms scraped data to relational format
- **Snapshot Tracking**: Maintains historical price and status changes
- **Transactional Safety**: Uses database transactions for data integrity

### Streaming ETL Pipeline
- **Incremental Processing**: Data is saved as it's extracted, not at the end
- **Checkpointing**: Resume processing from where it left off if interrupted
- **Progress Visibility**: See results in real-time as cities are processed
- **Memory Efficient**: Process data in batches instead of loading everything into memory

### Advanced Rate Limiting
- **Exponential Backoff**: Automatically handles rate limiting with intelligent delays
- **User Agent Rotation**: Rotates between different user agents to avoid detection
- **Request Throttling**: Respects server limits with configurable request rates
- **Jitter**: Adds randomness to avoid pattern detection

### Improved Data Structure
- **Consolidated Storage**: Single `announcements.csv` file instead of multiple per-city files
- **Metadata Tracking**: Includes extraction timestamps for data lineage
- **Simplified Analysis**: Easier to work with single consolidated dataset

## 🔍 Data Sources

The pipeline scrapes data from:
1. **Cities List**: `https://www.espaciourbano.com/listado_arriendos.asp`
2. **City Announcements**: `https://www.espaciourbano.com/Resumen_Ciudad_arriendos.asp`

## 📝 Logging

Logs are automatically saved to `data/logs/` with timestamps. The logging system provides:
- File and console output
- Detailed error tracking
- Performance monitoring
- URL failure tracking

## 🛠️ Development

### Adding New Extractors
1. Inherit from `BaseExtractor`
2. Implement the `extract()` method
3. Add to the orchestrator

### Adding New Transformers
1. Inherit from `BaseTransformer`
2. Implement the `transform()` method
3. Add data cleaning logic

### Adding New Loaders
1. Inherit from `BaseLoader`
2. Implement the `load()` method
3. Add loader type to `_create_loader()` factory method in `ETLOrchestrator`
4. Update configuration settings if needed

## 🚨 Error Handling

The pipeline includes comprehensive error handling:
- Retry mechanisms for failed requests
- Graceful handling of parsing errors
- Detailed error logging and reporting
- Non-blocking failures (continues processing other data)

## 📈 Performance

- **Concurrent Processing**: Uses ThreadPoolExecutor for parallel city processing
- **Rate Limiting**: Configurable sleep times between requests
- **Memory Efficient**: Processes data in batches
- **Error Recovery**: Continues processing even if some URLs fail

## 🔒 Legal Considerations

This tool is designed for private use and research purposes. Users are responsible for:
- Complying with the website's terms of service
- Respecting robots.txt guidelines
- Implementing appropriate rate limiting
- Using the data responsibly

## 📞 Support

For issues or questions:
1. Check the logs in `data/logs/`
2. Review the error summary using `python cli.py errors`
3. Verify configuration in `config/settings.py` and your `.env` file
4. Ensure all required environment variables are set