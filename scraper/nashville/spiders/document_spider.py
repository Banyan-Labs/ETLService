import scrapy
import json
import csv


class DocumentSpider(scrapy.Spider):
    name = 'document'
    custom_settings = {
        'ITEM_PIPELINES': {
            'scraper.nashville.pipelines.PostgresPipeline': 300,
        },
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self, filepath=None, file_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.file_type = file_type
        if not self.filepath:
            raise ValueError("filepath is required for document spider")
        if not self.file_type:
            raise ValueError("file_type is required for document spider")

    def start_requests(self):
        yield scrapy.Request(
            url=f'file://{self.filepath}',
            callback=self.parse,
            dont_filter=True
        )

    def parse(self, response):
        self.logger.info(
            f"📄 Processing {self.file_type} file: {self.filepath}")
        if self.file_type == 'csv':
            yield from self.parse_csv()
        elif self.file_type == 'json':
            yield from self.parse_json()
        elif self.file_type in ['xlsx', 'xls']:
            yield from self.parse_excel()

    def parse_csv(self):
        self.logger.info("Parsing CSV file...")
        with open(self.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                self.logger.info(
                    f"Detected columns: {', '.join(reader.fieldnames)}")
            for row_num, row in enumerate(reader, start=1):
                item = dict(row)
                item['source'] = 'user_upload_csv'
                if not item.get('name'):
                    self.logger.warning(
                        f"Row {row_num} skipped - missing name")
                    continue
                if not item.get('url') or not item['url'].strip():
                    import hashlib
                    name_hash = hashlib.md5(
                        item['name'].encode()).hexdigest()[:8]
                    item['url'] = f"uploaded://csv/{name_hash}"
                if not item.get('venue_city'):
                    item['venue_city'] = 'Nashville'
                self.logger.info(f"✓ Row {row_num}: {item['name']}")
                self.logger.debug(f"   Item keys: {list(item.keys())}")
                yield item
        self.logger.info("CSV parsing complete")

    def parse_json(self):
        self.logger.info("Parsing JSON file...")
        with open(self.filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            for idx, record in enumerate(data, start=1):
                item = dict(record)
                item['source'] = 'user_upload_json'
                if not item.get('name'):
                    self.logger.warning(f"Record {idx} skipped - missing name")
                    continue
                if not item.get('url') or not item['url'].strip():
                    import hashlib
                    name_hash = hashlib.md5(
                        item['name'].encode()).hexdigest()[:8]
                    item['url'] = f"uploaded://json/{name_hash}"
                if not item.get('venue_city'):
                    item['venue_city'] = 'Nashville'
                yield item
        self.logger.info("JSON parsing complete")

    def parse_excel(self):
        self.logger.info("Parsing Excel file...")
        try:
            import pandas as pd
        except ImportError:
            self.logger.error("pandas required for Excel")
            return
        df = pd.read_excel(self.filepath)
        self.logger.info(f"Detected columns: {', '.join(df.columns)}")
        for idx, row in df.iterrows():
            item = {k: (str(v) if pd.notna(v) else '')
                    for k, v in row.to_dict().items()}
            item['source'] = 'user_upload_excel'
            if not item.get('name'):
                self.logger.warning(f"Row {idx+1} skipped - missing name")
                continue
            if not item.get('url') or not item['url'].strip():
                import hashlib
                name_hash = hashlib.md5(item['name'].encode()).hexdigest()[:8]
                item['url'] = f"uploaded://excel/{name_hash}"
            if not item.get('venue_city'):
                item['venue_city'] = 'Nashville'
            yield item
        self.logger.info("Excel parsing complete")
