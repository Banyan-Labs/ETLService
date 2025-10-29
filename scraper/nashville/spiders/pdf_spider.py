import os
import re
import hashlib
from typing import Dict, Any, List
import pymupdf
import scrapy
from scraper.nashville.items import BusinessItem
class PDFSpider(scrapy.Spider):
    name = 'pdf'
    # Regex patterns for data extraction
    DATE_PATTERNS = [
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4}',
        r'\d{1,2}/\d{1,2}/\d{2,4}',
        r'\d{4}-\d{2}-\d{2}'
    ]
    ADDRESS_KEYWORDS = ['street', 'st', 'avenue', 'ave', 'road',
                        'rd', 'boulevard', 'blvd', 'drive', 'dr', 'nashville']
    URL_PATTERN = r'https?://[^\s]+'
    def __init__(self, pdf_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not pdf_path:
            raise ValueError("pdf_path argument is required")
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        self.pdf_path = pdf_path
    def start_requests(self):
        yield scrapy.Request(
            url=f'file://{os.path.abspath(self.pdf_path)}',
            callback=self.parse,
            dont_filter=True
        )
    def parse(self, response):
        text = self._extract_pdf_text()
        if not text:
            self.logger.error("No text extracted from PDF")
            return
        items = self._parse_text_to_items(text)
        self.logger.info(f"Extracted {len(items)} items from PDF")
        for item_data in items:
            if self._is_valid_item(item_data):
                yield self._create_item(item_data)
    def _extract_pdf_text(self) -> str:
        try:
            doc = pymupdf.open(self.pdf_path)
            text = '\n'.join(page.get_text() for page in doc)
            doc.close()
            return text
        except Exception as e:
            self.logger.error(f"PDF extraction failed: {e}")
            return ""
    def _parse_text_to_items(self, text: str) -> List[Dict[str, Any]]:
        lines = [line.strip()
                 for line in text.split('\n') if len(line.strip()) > 3]
        items = []
        current = {}
        for line in lines:
            if self._is_structured_label(line):
                label, value = self._parse_label_value(line)
                if label in ['venue', 'location', 'place']:
                    if current.get('name'):
                        items.append(current)
                    current = {'name': value, 'venue_name': value}
                elif label == 'name':
                    if current.get('name'):
                        items.append(current)
                    current = {'name': value, 'venue_name': value}
                elif label in ['address', 'venue address']:
                    current['venue_address'] = value
                elif label in ['date', 'event date', 'when']:
                    current['event_date'] = value
                elif label in ['website', 'url', 'web', 'link']:
                    current['url'] = value
                else:
                    current.setdefault('description', []).append(line)
            else:
                self._classify_and_add_line(line, current)
        if current.get('name'):
            items.append(current)
        return self._clean_items(items)
    def _is_structured_label(self, line: str) -> bool:
        return ':' in line and not line.startswith('http')
    def _parse_label_value(self, line: str) -> tuple:
        parts = line.split(':', 1)
        label = parts[0].strip().lower()
        value = parts[1].strip() if len(parts) > 1 else ''
        return label, value
    def _classify_and_add_line(self, line: str, current: Dict):
        if self._matches_pattern(line, self.URL_PATTERN):
            current['url'] = line
        elif self._is_date(line):
            current['event_date'] = line
        elif self._is_address(line):
            current['venue_address'] = line
        elif self._looks_like_name(line):
            if current.get('name'):
                current.setdefault('description', []).append(line)
            else:
                current['name'] = line
                current['venue_name'] = line
        else:
            current.setdefault('description', []).append(line)
    def _is_date(self, text: str) -> bool:
        return any(re.search(pattern, text.lower()) for pattern in self.DATE_PATTERNS)
    def _is_address(self, text: str) -> bool:
        return any(kw in text.lower() for kw in self.ADDRESS_KEYWORDS)
    def _matches_pattern(self, text: str, pattern: str) -> bool:
        return bool(re.search(pattern, text))
    def _looks_like_name(self, text: str) -> bool:
        if not (5 <= len(text) <= 100):
            return False
        return text[0].isupper()
    def _clean_items(self, items: List[Dict]) -> List[Dict]:
        cleaned = []
        for item in items:
            if isinstance(item.get('description'), list):
                item['description'] = ' '.join(item['description'])[:500]
            cleaned.append(item)
        return cleaned
    def _is_valid_item(self, item: Dict[str, Any]) -> bool:
        name = item.get('name', '')
        return name and len(name) >= 3
    def _create_item(self, data: Dict[str, Any]) -> BusinessItem:
        item = BusinessItem()
        item['source'] = 'pdf_upload'
        item['name'] = data.get('name', '').strip()
        item['venue_name'] = data.get(
            'venue_name', data.get('name', '')).strip()
        item['venue_address'] = data.get('venue_address', '').strip()
        item['venue_city'] = 'Nashville'
        item['description'] = data.get('description')
        item['event_date'] = data.get('event_date')
        item['category'] = 'pdf_extracted'
        item['url'] = self._get_or_generate_url(
            item['name'], item['venue_address'], data.get('url'))
        return item
    def _get_or_generate_url(self, name: str, address: str, existing_url: str) -> str:
        url = (existing_url or '').strip()
        if url and len(url) > 5:
            return url
        content = f"{name}|{address or 'no-address'}"
        hash_value = hashlib.md5(content.encode()).hexdigest()[:12]
        return f"pdf://nashville-event/{hash_value}"