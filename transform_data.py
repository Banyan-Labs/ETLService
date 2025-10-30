import os
import json
import psycopg2
import re
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
try:
    genai.configure(api_key=os.environ['GOOGLE_API_KEY'])
    model = genai.GenerativeModel(
        'gemini-2.5-flash-preview-09-2025',
        generation_config={"response_mime_type": "application/json"},
        safety_settings={
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        }
    )
    print("AI Model configured successfully.")
except KeyError:
    print("CRITICAL ERROR: GOOGLE_API_KEY environment variable not set. AI extraction will fail.")
    model = None
except Exception as e:
    print(f"CRITICAL ERROR: Failed to configure AI model. Error: {e}")
    model = None
event_schema = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "name": {"type": "STRING", "description": "The official name of the event or attraction."},
            "event_date": {"type": "STRING", "description": "The specific date(s) and time(s) of the event. Combine date and time if available. Format like 'Month Day, Year HH:MM AM/PM' or 'Month Day-Day, Year' or 'Season Year'."},
            "venue_name": {"type": "STRING", "description": "The name of the place where the event is held."},
            "venue_address": {"type": "STRING", "description": "The street address of the venue."},
            "description": {"type": "STRING", "description": "A brief description of the event or attraction."},
            "url": {"type": "STRING", "description": "The official website URL for the event or venue, if provided."},
            "category": {"type": "STRING", "description": "The type of event (e.g., Music, Festival, Sports, Theater, Community, Arts)."},
            "genre": {"type": "STRING", "description": "The musical genre, if applicable (e.g., Country, Rock, Jazz, Classical)."},
            "season": {"type": "STRING", "description": "The season if specific dates aren't given (e.g., 'Spring 2025', 'Summer')."}
        },
        "required": ["name"]
    }
}


def get_db_connection():
    try:
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        return conn
    except KeyError:
        print("CRITICAL ERROR: DATABASE_URL environment variable not set.")
        return None
    except Exception as e:
        print(f"CRITICAL ERROR: Could not connect to database. Error: {e}")
        return None


def transform_arcgis_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    category = raw_data.get('category', 'Civic Facility')
    try:
        latitude = float(raw_data.get('latitude')) if raw_data.get(
            'latitude') else None
        longitude = float(raw_data.get('longitude')
                          ) if raw_data.get('longitude') else None
    except (ValueError, TypeError):
        latitude = None
        longitude = None
        print(
            f"WARNING: Skipped bad coordinates for ArcGIS item: {raw_data.get('name')}")
    clean_item = {
        'source': 'Nashville ArcGIS',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': category.replace('_', ' ').title(),
        'latitude': latitude,
        'longitude': longitude,
        'event_date': None,
        'season': None,
        'genre': None,
    }
    if not clean_item.get('name') or not clean_item.get('venue_name'):
        return None
    return clean_item


def transform_ticketmaster_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    event_date = raw_data.get('event_date')
    clean_item = {
        'source': 'Ticketmaster',
        'name': raw_data.get('name'),
        'url': raw_data.get('url'),
        'venue_name': raw_data.get('venue_name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city'),
        'description': raw_data.get('description'),
        'event_date': event_date,
        'category': raw_data.get('category', 'Event').title(),
        'genre': raw_data.get('genre'),
        'season': raw_data.get('season'),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
    }
    if not clean_item.get('name') or not clean_item.get('venue_name'):
        return None
    return clean_item


def transform_yelp_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    clean_item = {
        'source': 'Yelp',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'Business').title(),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'event_date': None,
        'season': None,
        'genre': None,
    }
    if not clean_item.get('name'):
        return None
    return clean_item


def transform_google_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    clean_item = {
        'source': 'Google Places',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'Attraction').title(),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'event_date': None,
        'season': None,
        'genre': None, }
    if not clean_item.get('name'):
        return None
    return clean_item


def transform_generic_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    source_map = {
        'nashville.com-events': 'Nashville Events',
        'nashville.com-hotels': 'Nashville Hotels',
        'underdog': 'Underdog Venue',
    }
    display_source = source_map.get(raw_item.get(
        'source_spider', 'Unknown'), raw_item.get('source_spider', 'Unknown'))

    clean_item = {
        'source': display_source,
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('venue_name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'General').title(),
        'event_date': raw_data.get('event_date'),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'season': raw_data.get('season'),
        'genre': raw_data.get('genre'),
    }
    if not clean_item.get('name'):
        return None
    return clean_item


def transform_seatgeek_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    clean_item = {
        'source': 'SeatGeek',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('venue_name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'Event').title(),
        'event_date': raw_data.get('event_date'),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'season': raw_data.get('season'),
        'genre': raw_data.get('genre'),
    }
    if not clean_item.get('name') or not clean_item.get('venue_name'):
        return None
    return clean_item


def transform_document_data(raw_item: dict) -> list[dict]:
    """
    Transform data from document spider (CSV, Excel, Word).
    Handles both structured data and AI-extracted content.

    Args:
        raw_item: Raw data dictionary from database

    Returns:
        List of cleaned event dictionaries
    """
    try:
        raw_data = json.loads(raw_item['raw_json'])
    except json.JSONDecodeError:
        print(
            f"ERROR: Could not parse raw_json for {raw_item.get('source_spider')}. Skipping.")
        return []

    source_spider = raw_item.get('source_spider', '')

    # Determine file type from source_spider name
    file_type = 'unknown'
    if 'csv' in source_spider:
        file_type = 'csv'
    elif 'xlsx' in source_spider or 'xls' in source_spider:
        file_type = 'excel'
    elif 'docx' in source_spider:
        file_type = 'word'

    # Check if this is text content that needs AI extraction
    if 'text' in raw_data and 'original_filepath' in raw_data:
        # Use AI extraction for unstructured document text
        return _extract_with_ai(raw_data, file_type)

    # Handle structured data directly from document spider
    clean_item = {
        'source': f'Document Upload ({file_type.upper()})',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('venue_name') or raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'Document Extracted').replace('_', ' ').title(),
        'event_date': raw_data.get('event_date'),
        'latitude': _safe_float(raw_data.get('latitude')),
        'longitude': _safe_float(raw_data.get('longitude')),
        'season': raw_data.get('season'),
        'genre': raw_data.get('genre'),
    }

    # Validate minimum requirements
    if not clean_item.get('name'):
        print(f"WARNING: Document item skipped, no name.")
        return []

    return [clean_item]


def _safe_float(value: any) -> float:
    """Safely convert value to float."""
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None


def _extract_with_ai(raw_data: dict, file_type: str) -> list[dict]:
    """
    Extract events from unstructured document text using AI.

    Args:
        raw_data: Dictionary containing 'text' and 'original_filepath'
        file_type: Type of file (csv, excel, word)

    Returns:
        List of extracted event dictionaries
    """
    if not model:
        print("CRITICAL: AI model not available. Skipping document transform.")
        return []

    raw_text = raw_data.get('text', '')
    filepath = raw_data.get('original_filepath', 'Untitled Document')

    if not raw_text or len(raw_text.strip()) < 20:
        print(
            f"WARNING: Skipping AI call for {filepath} due to minimal text content.")
        return []

    print(
        f"--- Calling AI to extract events from {file_type.upper()}: {os.path.basename(filepath)} ---")

    try:
        prompt = f"""
        Analyze the following text extracted from a {file_type.upper()} document named '{os.path.basename(filepath)}'.
        Your task is to identify and extract distinct events, attractions, or points of interest mentioned.
        
        Guidelines:
        - Extract only actual events or businesses with specific details
        - Ignore metadata, headers, or formatting artifacts
        - For CSV/Excel: each row should represent one event
        - For Word: parse structured information from paragraphs or tables
        - Combine related information (date + time, venue + address)
        
        Return the information as a JSON list of objects, strictly adhering to the provided schema.
        The 'name' field is mandatory for each object.
        If specific information is not found, use null or omit the field.
        
        TEXT_TO_PARSE:
        --- START TEXT ---
        {raw_text[:15000]}
        --- END TEXT ---
        """

        response = model.generate_content(
            prompt,
            generation_config={"response_schema": event_schema}
        )

        try:
            extracted_events_json = json.loads(response.text)
        except json.JSONDecodeError as json_err:
            print(
                f"CRITICAL ERROR: AI returned invalid JSON for {filepath}. Error: {json_err}")
            print(f"AI Response Text: {response.text[:500]}...")
            return []
        print(
            f"--- AI successfully extracted {len(extracted_events_json)} events from {filepath} ---")
        clean_events = []
        for i, event_data in enumerate(extracted_events_json):
            if not event_data.get('name'):
                print(
                    f"WARNING: AI returned an item with no name from {filepath}. Skipping.")
                continue
            unique_url = event_data.get('url')
            if not unique_url or unique_url.strip() == "":
                url_safe_name = re.sub(
                    r'\W+', '-', event_data.get('name', f'event-{i}')).lower()
                unique_url = f"file://{os.path.basename(filepath)}#{i}-{url_safe_name}"
            clean_item = {
                'source': f'Document Upload ({file_type.upper()})',
                'name': event_data.get('name'),
                'venue_name': event_data.get('venue_name'),
                'venue_address': event_data.get('venue_address'),
                'venue_city': 'Nashville',
                'description': event_data.get('description'),
                'url': unique_url,
                'category': event_data.get('category', 'Document Extracted').title(),
                'event_date': event_data.get('event_date'),
                'latitude': None,
                'longitude': None,
                'season': event_data.get('season'),
                'genre': event_data.get('genre'),
            }
            clean_events.append(clean_item)
        return clean_events
    except Exception as e:
        print(
            f"CRITICAL ERROR: Failed during AI extraction for {filepath}. Error: {e}")
        return [
            {
                'source': f'Document Upload ({file_type.upper()}) - AI Error',
                'name': f"Failed to parse: {os.path.basename(filepath)}",
                'venue_name': 'See Description',
                'venue_address': 'See Description',
                'venue_city': 'Nashville',
                'description': f"AI processing failed with error: {e}. Raw text: {raw_text[:500]}...",
                'url': f"file://{os.path.basename(filepath)}#failed-ai",
                'category': 'Error',
                'event_date': None,
                'latitude': None,
                'longitude': None,
                'season': None,
                'genre': None,
            }
        ]


def transform_pdf_data(raw_item: dict) -> list[dict]:
    if not model:
        print("CRITICAL: AI model not available. Skipping PDF transform.")
        return []
    try:
        raw_data = json.loads(raw_item['raw_json'])
    except json.JSONDecodeError:
        print(
            f"ERROR: Could not parse raw_json for {raw_item.get('source_spider')}. Skipping.")
        return []
    if 'text' in raw_data and 'original_filepath' in raw_data:
        raw_text = raw_data.get('text', '')
        filepath = raw_data.get('original_filepath', 'Untitled PDF')
        print(
            f"--- Calling AI to extract events from PDF: {os.path.basename(filepath)} ---")
        if not raw_text or len(raw_text.strip()) < 20:
            print(
                f"WARNING: Skipping AI call for {filepath} due to minimal text content.")
            return []
        try:
            prompt = f"""
            Analyze the following text extracted from a PDF document named '{os.path.basename(filepath)}'.
            Your task is to identify and extract distinct events, attractions, or points of interest mentioned.
            Ignore advertisements unless they are describing a specific, dated event.
            Ignore general directories or lists of businesses unless they contain specific event details (name, date/season, venue).
            Return the information as a JSON list of objects, strictly adhering to the provided schema.
            The 'name' field is mandatory for each object.
            If a specific piece of information (like venue_address, url, category, genre, season) is not found for an event, use null or omit the field if appropriate according to the schema.
            Combine date and time details into the 'event_date' field. If only a season or year range is given, use that for 'event_date' or 'season'.
            Be concise in the description field.

            TEXT_TO_PARSE:
            --- START TEXT ---
            {raw_text[:15000]}
            --- END TEXT ---
            """
            response = model.generate_content(
                prompt,
                generation_config={"response_schema": event_schema}
            )
            try:
                extracted_events_json = json.loads(response.text)
            except json.JSONDecodeError as json_err:
                print(
                    f"CRITICAL ERROR: AI returned invalid JSON for {filepath}. Error: {json_err}")
                print(f"AI Response Text: {response.text[:500]}...")
                return []
            print(
                f"--- AI successfully extracted {len(extracted_events_json)} potential events from {filepath} ---")
            clean_events = []
            for i, event_data in enumerate(extracted_events_json):
                if not event_data.get('name'):
                    print(
                        f"WARNING: AI returned an item with no name from {filepath}. Skipping.")
                    continue
                unique_url = event_data.get('url')
                if not unique_url or unique_url.strip() == "":
                    url_safe_name = re.sub(
                        r'\W+', '-', event_data.get('name', f'event-{i}')).lower()
                    unique_url = f"file://{os.path.basename(filepath)}#{i}-{url_safe_name}"
                clean_item = {
                    'source': 'PDF Upload',
                    'name': event_data.get('name'),
                    'venue_name': event_data.get('venue_name'),
                    'venue_address': event_data.get('venue_address'),
                    'venue_city': 'Nashville',
                    'description': event_data.get('description'),
                    'url': unique_url,
                    'category': event_data.get('category', 'Pdf Extracted').title(),
                    'event_date': event_data.get('event_date'),
                    'latitude': None,
                    'longitude': None,
                    'season': event_data.get('season'),
                    'genre': event_data.get('genre'),
                }
                clean_events.append(clean_item)
            return clean_events
        except Exception as e:
            print(
                f"CRITICAL ERROR: Failed during AI extraction for {filepath}. Error: {e}")
            return [
                {
                    'source': 'PDF Upload (AI Error)',
                    'name': f"Failed to parse: {os.path.basename(filepath)}",
                    'venue_name': 'See Description',
                    'venue_address': 'See Description',
                    'venue_city': 'Nashville',
                    'description': f"AI processing failed with error: {e}. Raw text: {raw_text[:500]}...",
                    'url': f"file://{os.path.basename(filepath)}#failed-ai",
                    'category': 'Error',
                    'event_date': None, 'latitude': None, 'longitude': None, 'season': None, 'genre': None,
                }
            ]
    else:
        print(
            f"Processing structured data from {raw_item.get('source_spider')}")
        clean_item = {
            'source': 'PDF Upload (Structured)',
            'name': raw_data.get('name'),
            'venue_name': raw_data.get('venue_name'),
            'venue_address': raw_data.get('venue_address'),
            'venue_city': raw_data.get('venue_city', 'Nashville'),
            'description': raw_data.get('description'),
            'url': raw_data.get('url'),
            'category': raw_data.get('category', 'Pdf Extracted').replace('_', ' ').title(),
            'event_date': raw_data.get('event_date'),
            'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
            'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
            'season': raw_data.get('season'),
            'genre': raw_data.get('genre'),
        }
        if not clean_item.get('name') or not clean_item.get('url'):
            print(f"WARNING: Structured PDF item skipped, no name or URL.")
            return []
        return [clean_item]


def run_transformations():
    print("transform started")
    conn = get_db_connection()
    if not conn:
        print("CRITICAL: No database connection. Transform task exiting.")
        return
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, raw_json, source_spider FROM raw_data")
        raw_results = cursor.fetchall()
    except Exception as e:
        print(f"CRITICAL: Failed to fetch from raw_data. Error: {e}")
        conn.close()
        return
    transformed_events = []
    processed_raw_ids = []
    for row in raw_results:
        raw_id, raw_json_str, source_spider = row
        raw_item = {'raw_json': raw_json_str, 'source_spider': source_spider}
        transformed = None
        try:
            if source_spider == 'nashville_arcgis':
                transformed = transform_arcgis_data(raw_item)
            elif source_spider == 'ticketmaster':
                transformed = transform_ticketmaster_data(raw_item)
            elif source_spider == 'yelp':
                transformed = transform_yelp_data(raw_item)
            elif source_spider == 'google_places':
                transformed = transform_google_data(raw_item)
            elif source_spider == 'generic':
                transformed = transform_generic_data(raw_item)
            elif source_spider == 'pdf' or source_spider.startswith('manual_upload_'):
                transformed = transform_pdf_data(raw_item)
            elif source_spider == 'document' or any(ext in source_spider for ext in ['csv', 'xlsx', 'xls', 'docx']):
                transformed = transform_document_data(raw_item)
            elif source_spider == 'seatgeek':
                transformed = transform_seatgeek_data(raw_item)
            else:
                print(
                    f"WARNING: No transformer for spider '{source_spider}', skipping item id {raw_id}")
        except Exception as e:
            print(
                f"CRITICAL ERROR: Failed to process item id {raw_id} from {source_spider}. Error: {str(e)}")
            continue
        if transformed:
            processed_raw_ids.append(raw_id)
            if isinstance(transformed, list):
                for item in transformed:
                    if item:
                        transformed_events.append(item)
            else:
                transformed_events.append(transformed)

    print(
        f"Transforming {len(raw_results)} raw items... {len(transformed_events)} clean events created.")

    if not transformed_events:
        print("No events to insert. Transform task finished.")
        cursor.close()
        conn.close()
        return
    ts_vector_sql = "to_tsvector('english', COALESCE(%s, '') || ' ' || COALESCE(%s, '') || ' ' || COALESCE(%s, '') || ' ' || COALESCE(%s, ''))"
    insert_query = f"""
        INSERT INTO events (name, url, event_date, venue_name, venue_address, description, source, category, genre, season, latitude, longitude, search_vector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {ts_vector_sql})
        ON CONFLICT (url) DO NOTHING
        """
    records_to_insert = []
    for event in transformed_events:
        text_for_search = (event.get('name'), event.get(
            'venue_name'), event.get('venue_address'), event.get('description'))
        event_values = (
            event.get('name'),
            event.get('url'),
            event.get('event_date'),
            event.get('venue_name'),
            event.get('venue_address'),
            event.get('description'),
            event.get('source'),
            event.get('category'),
            event.get('genre'),
            event.get('season'),
            event.get('latitude'),
            event.get('longitude')
        )
        records_to_insert.append(event_values + text_for_search)
    items_loaded = 0
    try:
        for record in records_to_insert:
            try:
                cursor.execute(insert_query, record)
                items_loaded += cursor.rowcount
            except Exception as e:
                print(f"ERROR inserting record: {record[0]}. Error: {e}")
                conn.rollback()
        conn.commit()
        print(
            f"Successfully inserted/updated {items_loaded} items into events table.")
    except Exception as e:
        print(f"CRITICAL: Database commit failed. Error: {e}")
        conn.rollback()
    if processed_raw_ids:
        try:
            delete_query = "DELETE FROM raw_data WHERE id IN %s"
            cursor.execute(delete_query, (tuple(processed_raw_ids),))
            conn.commit()
            print(
                f"Successfully deleted {len(processed_raw_ids)} processed items from raw_data table.")
        except Exception as e:
            print(f"ERROR: Failed to delete processed raw_data. Error: {e}")
            conn.rollback()
    cursor.close()
    conn.close()
    print(f"transform all done. {items_loaded} items loaded to events table.")


if __name__ == '__main__':
    print("Running transformations locally...")
    run_transformations()
