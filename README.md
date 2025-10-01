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

### 🎯 Orchestration (`etl/etl_orchestrator.py`)
- **`ETLOrchestrator`**: Coordinates the entire pipeline execution

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

### Running the Pipeline

#### Option 1: Run the complete pipeline
```bash
python main.py
```

#### Option 2: Use the CLI interface
```bash
# Run complete pipeline
python cli.py full

# Run only cities extraction
python cli.py cities

# Show error summary from last run
python cli.py errors
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

All configuration is centralized in `config/settings.py`:

- **URLs**: Base URLs for scraping (hardcoded for reliability)
- **Scraping Parameters**: Page limits, sleep times, timeouts
- **Data Storage**: Output directories and filenames
- **Property Types**: Mapping of property type codes

## 📊 Output Data

The pipeline generates the following CSV files:

### `data/cities.csv`
Contains city information:
- `id`: Unique city identifier
- `city_name`: Cleaned city name
- `url`: Source URL
- `is_active`: Whether the city is active
- `created_at`: Timestamp

### `data/announcements.csv`
Contains rental property announcements:
- `announcement_id`: Unique announcement identifier
- `url`: Link to full announcement
- `city_name`: Associated city
- `neighborhood`: Property neighborhood
- `price`: Monthly rent price
- `rooms`: Number of rooms
- `bathrooms`: Number of bathrooms
- `parkings`: Number of parking spaces
- `area`: Property area in square meters
- `description`: Property description
- `property_type`: Inferred property type
- `created_at`: Timestamp

### `data/announcements_{city_name}.csv`
Individual files for each city's announcements.

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
3. Add to the orchestrator

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
3. Verify configuration in `config/settings.py`