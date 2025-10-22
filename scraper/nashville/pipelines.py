import os
import psycopg2
import json
from scraper.nashville.transform import transform_event


class PostgresPipeline:
    def open_spider(self, spider):
        self.connection = psycopg2.connect(os.environ['DATABASE_URL'])
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            item_dict = dict(item)
            spider.logger.info(
                f"Processing: {item_dict.get('name', 'Unknown')}")
            self.cursor.execute(
                "INSERT INTO raw_data (source_spider, raw_json) VALUES (%s, %s)",
                (spider.name, json.dumps(dict(item)))
            )
            transformed_item = transform_event(dict(item))
            if not transformed_item:
                spider.logger.warning(f"Transform returned None - skipping")
                self.connection.rollback()
                return item
            cleaned_item = self._clean_item_for_db(transformed_item)
            values = (
                cleaned_item.get('name'),
                cleaned_item.get('url'),
                cleaned_item.get('event_date'),
                cleaned_item.get('venue_name'),
                cleaned_item.get('venue_address'),
                cleaned_item.get('description'),
                cleaned_item.get('source'),
                cleaned_item.get('category'),
                cleaned_item.get('genre'),
                cleaned_item.get('season'),
                cleaned_item.get('latitude'),   # Now properly cleaned
                cleaned_item.get('longitude')
            )

            # Insert into events table
            self.cursor.execute(
                """
            INSERT INTO events (name, url, event_date, venue_name, venue_address, description, source, category, genre, season, latitude, longitude) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            """,
                values
            )

            self.connection.commit()

            if self.cursor.rowcount > 0:
                spider.logger.info(f"✓ Saved: {cleaned_item.get('name')}")

        except Exception as e:
            self.connection.rollback()
            spider.logger.error(f"Pipeline error: {e}")
            import traceback
            spider.logger.debug(traceback.format_exc())

        return item

    def _clean_item_for_db(self, item):
        cleaned = item.copy()

    # Clean latitude - must be float or None
        lat = cleaned.get('latitude')
        if lat:
            try:
                # Handle various formats
                lat_str = str(lat).strip()
                if lat_str and lat_str.lower() not in ['none', 'null', 'n/a', '']:
                    cleaned['latitude'] = float(lat_str)
                else:
                    cleaned['latitude'] = None
            except (ValueError, TypeError):
                pass
        else:
            cleaned['latitude'] = None

    # Clean longitude - must be float or None
        lng = cleaned.get('longitude')
        if lng:
            try:
                lng_str = str(lng).strip()
                if lng_str and lng_str.lower() not in ['none', 'null', 'n/a', '']:
                    cleaned['longitude'] = float(lng_str)
                else:
                    cleaned['longitude'] = None
            except (ValueError, TypeError):
                self.logger.debug(
                    f"Invalid longitude: {lng} - setting to None")
                cleaned['longitude'] = None
        else:
            cleaned['longitude'] = None

    # Validate latitude/longitude ranges
        if cleaned.get('latitude'):
            if not (-90 <= cleaned['latitude'] <= 90):
                self.logger.debug(
                    f"Latitude out of range: {cleaned['latitude']}")
                cleaned['latitude'] = None

        if cleaned.get('longitude'):
            if not (-180 <= cleaned['longitude'] <= 180):
                self.logger.debug(
                    f"Longitude out of range: {cleaned['longitude']}")
                cleaned['longitude'] = None

    # Clean other text fields - convert empty strings to None
        text_fields = ['name', 'url', 'venue_name', 'venue_address', 'venue_city',
                       'description', 'category', 'genre', 'season', 'neighborhood']

        for field in text_fields:
            value = cleaned.get(field)
            if value:
                # Strip whitespace and check if empty
                value_str = str(value).strip()
                if not value_str or value_str.lower() in ['none', 'null', 'n/a']:
                    cleaned[field] = None
                else:
                    cleaned[field] = value_str
            else:
                cleaned[field] = None

        return cleaned
