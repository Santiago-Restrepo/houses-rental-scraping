"""
Postgres loader for saving data to PostgreSQL database.
Handles both listings and listing_snapshots tables with upsert logic.
"""

import logging
import psycopg2
import psycopg2.extras
from typing import List, Dict, Optional
from datetime import datetime
from .base_loader import BaseLoader


class PostgresLoader(BaseLoader):
    """Loader for saving data to PostgreSQL database."""

    def __init__(self, connection_string: str):
        super().__init__()
        self.connection_string = connection_string
        self.logger = logging.getLogger(self.__class__.__name__)

    def load(self, data: List[Dict], table_name: str) -> bool:
        """Load data to PostgreSQL table."""
        if not self._validate_data(data):
            return False

        try:
            with psycopg2.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    if table_name == 'listings':
                        return self._load_listings(cursor, data)
                    elif table_name == 'listing_snapshots':
                        return self._load_snapshots(cursor, data)
                    else:
                        self.logger.error(f"Unknown table: {table_name}")
                        return False

        except Exception as e:
            self.logger.error(f"Error loading data to PostgreSQL: {e}")
            return False

    def load_listings(self, listings_data: List[Dict]) -> bool:
        """Load listings data to PostgreSQL."""
        return self.load(listings_data, 'listings')

    def load_snapshots(self, snapshots_data: List[Dict]) -> bool:
        """Load listing snapshots data to PostgreSQL."""
        return self.load(snapshots_data, 'listing_snapshots')

    def _load_listings(self, cursor, data: List[Dict]) -> bool:
        """Load listings with upsert logic."""
        try:
            # Prepare data for bulk insert
            listings_values = []
            for listing in data:
                listings_values.append((
                    listing.get('provider_listing_id'),
                    listing.get('title'),
                    listing.get('property_type'),
                    listing.get('city'),
                    listing.get('neighborhood'),
                    listing.get('location'),
                    listing.get('price'),
                    listing.get('currency', 'COP'),
                    listing.get('rooms'),
                    listing.get('bathrooms'),
                    listing.get('area_m2'),
                    listing.get('link'),
                    listing.get('image_url'),
                    psycopg2.extras.Json(listing.get('features', {})),
                    psycopg2.extras.Json(listing.get('metadata', {})),
                    listing.get('first_seen') or datetime.now(),
                    listing.get('last_seen') or datetime.now(),
                    listing.get('active', True)
                ))

            # Bulk upsert query
            upsert_query = """
                INSERT INTO listings (
                    provider_listing_id, title, property_type, city, neighborhood,
                    location, price, currency, rooms, bathrooms, area_m2,
                    link, image_url, features, metadata, first_seen, last_seen, active
                ) VALUES %s
                ON CONFLICT (provider_listing_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    property_type = EXCLUDED.property_type,
                    city = EXCLUDED.city,
                    neighborhood = EXCLUDED.neighborhood,
                    location = EXCLUDED.location,
                    price = EXCLUDED.price,
                    currency = EXCLUDED.currency,
                    rooms = EXCLUDED.rooms,
                    bathrooms = EXCLUDED.bathrooms,
                    area_m2 = EXCLUDED.area_m2,
                    link = EXCLUDED.link,
                    image_url = EXCLUDED.image_url,
                    features = EXCLUDED.features,
                    metadata = EXCLUDED.metadata,
                    last_seen = EXCLUDED.last_seen,
                    active = EXCLUDED.active
            """

            psycopg2.extras.execute_values(cursor, upsert_query, listings_values)
            self.logger.info(f"Upserted {len(data)} listings")
            return True

        except Exception as e:
            self.logger.error(f"Error loading listings: {e}")
            return False

    def _load_snapshots(self, cursor, data: List[Dict]) -> bool:
        """Load listing snapshots."""
        try:
            # Prepare data for bulk insert
            snapshots_values = []
            for snapshot in data:
                snapshots_values.append((
                    snapshot.get('listing_id'),
                    snapshot.get('provider_listing_id'),
                    snapshot.get('scraped_at') or datetime.now(),
                    snapshot.get('price'),
                    snapshot.get('currency', 'COP'),
                    snapshot.get('status', 'active'),
                    psycopg2.extras.Json(snapshot.get('raw_json', {}))
                ))

            # Bulk insert query (snapshots are immutable, so no upsert)
            insert_query = """
                INSERT INTO listing_snapshots (
                    listing_id, provider_listing_id, scraped_at,
                    price, currency, status, raw_json
                ) VALUES %s
            """

            psycopg2.extras.execute_values(cursor, insert_query, snapshots_values)
            self.logger.info(f"Inserted {len(data)} listing snapshots")
            return True

        except Exception as e:
            self.logger.error(f"Error loading snapshots: {e}")
            return False

    def load_announcements_streaming(self, announcements_data: List[Dict]) -> bool:
        """
        Load announcements data in streaming fashion.
        Transforms announcements to listings and snapshots format.
        """
        if not announcements_data:
            return True

        try:
            with psycopg2.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    # Transform announcements to listings format
                    listings_data = []
                    snapshots_data = []

                    for announcement in announcements_data:
                        # Create listing record
                        listing = self._transform_announcement_to_listing(announcement)
                        listings_data.append(listing)

                        # Create snapshot record
                        snapshot = self._transform_announcement_to_snapshot(announcement, listing['provider_listing_id'])
                        snapshots_data.append(snapshot)

                    # Load listings first
                    success_listings = self._load_listings(cursor, listings_data)
                    if not success_listings:
                        return False

                    # Retrieve listing_ids for the inserted listings
                    provider_ids = [listing['provider_listing_id'] for listing in listings_data]
                    cursor.execute(
                        "SELECT id, provider_listing_id FROM listings WHERE provider_listing_id = ANY(%s)",
                        (provider_ids,)
                    )
                    listing_id_map = {row[1]: row[0] for row in cursor.fetchall()}

                    # Update snapshots with correct listing_id
                    for snapshot in snapshots_data:
                        snapshot['listing_id'] = listing_id_map.get(snapshot['provider_listing_id'])

                    # Load snapshots
                    success_snapshots = self._load_snapshots(cursor, snapshots_data)

                    return success_listings and success_snapshots

        except Exception as e:
            self.logger.error(f"Error in streaming load: {e}")
            return False

    def _transform_announcement_to_listing(self, announcement: Dict) -> Dict:
        """Transform announcement data to listing format."""
        return {
            'provider_listing_id': str(announcement.get('id', '')),
            'title': announcement.get('description', ''),
            'property_type': self._infer_property_type(announcement),
            'city': announcement.get('city', ''),
            'neighborhood': announcement.get('neighborhood', ''),
            'location': f"{announcement.get('city', '')}, {announcement.get('neighborhood', '')}",
            'price': announcement.get('price', 0),
            'currency': 'COP',
            'rooms': announcement.get('rooms', 0) if isinstance(announcement.get('rooms'), int) else 0,
            'bathrooms': announcement.get('bathrooms', 0) if isinstance(announcement.get('bathrooms'), int) else 0,
            'area_m2': announcement.get('area', 0) if isinstance(announcement.get('area'), (int, float)) else 0,
            'link': announcement.get('url', ''),
            'image_url': announcement.get('img_url', ''),
            'features': self._extract_features(announcement),
            'metadata': {
                'is_featured': announcement.get('is_featured', False),
                'is_recently_updated': announcement.get('is_recently_updated', False),
                'extraction_timestamp': announcement.get('extraction_timestamp'),
                'city_id': announcement.get('city_id')
            },
            'first_seen': datetime.now(),
            'last_seen': datetime.now(),
            'active': True
        }

    def _transform_announcement_to_snapshot(self, announcement: Dict, provider_listing_id: str) -> Dict:
        """Transform announcement data to snapshot format."""
        return {
            'listing_id': None,  # Will be set after listing is inserted/updated
            'provider_listing_id': provider_listing_id,
            'scraped_at': datetime.now(),
            'price': announcement.get('price', 0),
            'currency': 'COP',
            'status': 'active',
            'raw_json': announcement
        }

    def _infer_property_type(self, announcement: Dict) -> str:
        """Infer property type from announcement data."""
        # This is a simple inference - could be enhanced based on description or other fields
        description = announcement.get('description', '').lower()
        if 'apartamento' in description or 'apto' in description:
            return 'Apartment'
        elif 'casa' in description:
            return 'House'
        elif 'local' in description:
            return 'Commercial'
        elif 'oficina' in description:
            return 'Office'
        else:
            return 'Other'

    def _extract_features(self, announcement: Dict) -> Dict:
        """Extract features from announcement data."""
        features = {}

        # Extract parking information
        parkings = announcement.get('parkings')
        if parkings and parkings != 'N/A':
            try:
                features['parking_spaces'] = int(parkings)
            except (ValueError, TypeError):
                pass

        # Add other potential features
        if announcement.get('is_featured'):
            features['featured'] = True

        return features