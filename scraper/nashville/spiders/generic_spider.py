import scrapy
import json
from scrapy_playwright.page import PageMethod
from scraper.nashville.items import BusinessItem
class GenericSpider(scrapy.Spider):
    name = 'generic'
    async def start(self):
        config_path = '/app/sites.json'        
        try:
            with open(config_path, 'r') as f:
                sites_config = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"CRITICAL: sites.json file not found at {config_path}")
            return
        except Exception as e:
            self.logger.error(f"CRITICAL: Failed to read or parse sites.json: {e}")
            return
        for source, config in sites_config.items():
            meta = {'config': config, 'source': source}
            wait_selector = config.get('item_container_selector') or config.get('item_anchor_selector')
            if config.get('uses_playwright', False):
                meta['playwright'] = True
                methods = []
                if wait_selector:
                    if wait_selector.startswith('xpath:'):
                        wait_selector = wait_selector.replace('xpath:', '')
                    methods.append(PageMethod('wait_for_selector', wait_selector))
                wait_time = config.get('wait_after_load')
                if wait_time:
                    methods.append(PageMethod('wait_for_timeout', wait_time))
                if methods:
                    meta['playwright_page_methods'] = methods
            yield scrapy.Request(url=config['start_url'], callback=self.parse, meta=meta)            
    def parse(self, response):
        config = response.meta['config']
        source = response.meta['source']
        item_elements = []
        container_selector = config.get('item_container_selector')
        anchor_selector = config.get('item_anchor_selector')
        if container_selector:
            item_elements = self._get_elements(response, container_selector)
        elif anchor_selector:
            parent_tag = config.get('parent_container_tag', 'div')
            for anchor in self._get_elements(response, anchor_selector):
                name_text = ' '.join(anchor.css('::text').getall()).strip()
                filter_out_text = config.get('name_filter_out', '')
                if filter_out_text and filter_out_text in name_text:
                    continue
                parent = anchor.xpath(f'ancestor::{parent_tag}[1]')
                if parent:
                    item_elements.append(parent)
        for item_element in item_elements:
            item = BusinessItem()
            item['source'] = source
            item['category'] = config.get('category')
            for field, value in config.get('defaults', {}).items():
                item[field] = value
            for field, field_selector in config.get('fields', {}).items():
                data = self._extract_data(item_element, field_selector)
                if data:
                    item[field] = data.strip() if data else None
            if config.get('detail_page_fields'):
                if item.get('url'):
                    absolute_url = response.urljoin(item['url'])
                    item['url'] = absolute_url
                    yield response.follow(
                        absolute_url,
                        callback=self.parse_details,
                        meta={'item': dict(item), 'config': config}
                    )
            else:
                if item.get('url'):
                    item['url'] = response.urljoin(item['url'])
                yield item                
    def parse_details(self, response):
        item = BusinessItem(response.meta['item'])
        config = response.meta['config']
        for field, css_selector in config.get('detail_page_fields', {}).items():
            data = self._extract_data(response, css_selector)
            item[field] = data.strip() if data else None
        yield item        
    def _get_elements(self, element, selector_str):
        if selector_str.startswith('xpath:'):
            return element.xpath(selector_str.replace('xpath:', ''))
        return element.css(selector_str)        
    def _extract_data(self, element, selector_str):
        use_xpath = selector_str.startswith('xpath:')
        clean_selector = selector_str.replace('xpath:', '').replace('css:', '')
        method = element.xpath if use_xpath else element.css
        if '::text' in clean_selector or 'following-sibling::text()' in clean_selector:
            raw_data = method(clean_selector).getall()
            return ' '.join(part.strip() for part in raw_data if part.strip())
        else:
            return method(clean_selector).get()
