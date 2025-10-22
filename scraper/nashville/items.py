import scrapy


class EventItem(scrapy.Item):
    name = scrapy.Field()
    event_id = scrapy.Field()
    url = scrapy.Field()
    venue_name = scrapy.Field()
    venue_city = scrapy.Field()
    venue_address = scrapy.Field()
    event_date = scrapy.Field()
    description = scrapy.Field()
    source = scrapy.Field()


class BusinessItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    description = scrapy.Field()
    source = scrapy.Field()
    neighborhood = scrapy.Field()
    event_id = scrapy.Field()
    venue_name = scrapy.Field()
    venue_city = scrapy.Field()
    venue_address = scrapy.Field()
    event_date = scrapy.Field()
    category = scrapy.Field()
    genre = scrapy.Field()
    season = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()


class FlexibleItem(scrapy.Item):
    def __setitem__(self, key, value):
        if key not in self.fields:
            self.fields[key] = scrapy.Field()
        super().__setitem__(key, value)
