import scrapy
import os
import json
from urllib.parse import urlencode
import time
from dotenv import load_dotenv
from nashville.items import BusinessItem
load_dotenv()


class GooglePlacesSpider(scrapy.Spider):
    name = 'google_places'
    allowed_domains = []
    NASHVILLE_LAT = 36.1627
    NASHVILLE_LNG = -86.7816
    RADIUS = 15000  # 15 kilometer radius
    CATEGORIES_TO_SEARCH = [
        'restaurant', 'hotel', 'tourist_attraction', 'park', 'museum', 'bar']

    def start_requests(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        if not self.api_key:
            raise logger.error(
                "GOOGLE_API_KEY not found in environment variables")
            return
        for category in self.CATEGORIES_TO_SEARCH:
            params = {
                'location': self.NASHVILLE_LAT_LNG,
                'radius': self.RADIUS,
                'keyword': category,
                'key': api_key,
            }
            url = f"{self.base_url}?{urlencode(params)}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'api_key': api_key, 'category': category}
            )

    def parse(self, response):
        data = json.loads(response.text)
        api_key = response.meta['api_key']
        category = response.meta['category']
        if data.get('status') not in ['OK', 'ZERO_RESULTS']:
            self.logger.error(
                f"API error for '{category}': {data.get('status')} - {data.get('error_message')}")
            return
        if 'result' in data:
            for place in data['result']:
                item = BusinessItem()
                item['source'] = 'google_places'
                item['name'] = place.get('displayName', 'name')
                item['venue_address'] = place.get('formattedAddress')
                item['rating'] = place.get('rating')
                item['category'] = category
                location = place.get('geometry', {}).get('location', {})
                item['latitude'] = location.get('lat')
                item['longitude'] = location.get('lng')
                if place.get('name') and item['latitude'] and item['longitude']:
                    item[
                        'url'] = f"https://www.google.com/maps/search/?api=1&query={item['latitude']},{item['longitude']}&query_place_id={place.get('placeId')}"
                item['description'] = f"Rating:{place.get('rating', 'N/A')} ({place.get('user_ratings_total', 0)} reviews)"
                item['venue_city'] = 'Nashville'
                yield item
        if 'next_page_token' in data:
            time.sleep(2)  # wait for token to become valid
            next_page_token = data['next_page_token']
            params = {
                'pagetoken': next_page_token,
                'key': api_key,
            }
            url = f"{self.base_url}?{urlencode(params)}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'api_key': api_key, 'category': category}
            )
