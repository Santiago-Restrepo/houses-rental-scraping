# Houses Rental Scraper

## Overview

This Python project is designed to scrape rental listings for houses from a popular real estate website in Antioquia, Colombia. Since the website lacks advanced search and filtering capabilities, this scraper helps solve that problem by extracting the needed data and organizing it in a CSV file for easier analysis and access.

## Features

- Scrapes house rental listings, including relevant details like location, price, and property features.
- Supports pagination to retrieve listings across multiple pages.
- Exports collected data to a CSV file for convenient offline access and analysis.

## Setup and Usage

1. **Install Dependencies**  
   First, install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment Variables**  
   Set up the necessary environment variables in a `.env` file to control scraping behavior:

   - `MAX_LISTINGS_PER_PAGE`: Maximum number of listings to scrape per page.
   - `PAGE_SLEEP_TIME`: Delay between requests (in seconds) to avoid overwhelming the server.
   - `BASE_URL`: The base URL of the website.
   - `RENTAL_BASE_URL`: The specific URL for rental listings.

3. **Run the Scraper**  
   Execute the scraper with:

   ```bash
   python main.py
   ```

   The scraper will gather listings for all available cities and save the results in a CSV file.

## Notes

This tool is intended to help organize and manage real estate rental data locally. Please ensure to comply with the website’s terms of service when using this scraper.
