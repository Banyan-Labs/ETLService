"""
Transform layer - standardizes and enriches event data.
Handles both API-scraped events and user-uploaded documents.
"""
from typing import Dict, List
from .standardizer import standardize_date, standardize_venue_name, standardize_price
from .categorizer import categorize_event


def transform_event(raw_event: Dict) -> Dict:
    """
    Transform a single raw event to standardized format.

    CRITICAL: For user uploads, PRESERVE all their data.
    Only enhance, never overwrite.
    """

    if not raw_event:
        return None

    # Create a new dict with ALL original fields
    transformed = {}
    for key, value in raw_event.items():
        transformed[key] = value

    # Get source to determine processing strategy
    source = transformed.get('source', '')
    is_user_upload = source and 'user_upload' in str(source).lower()

    # CRITICAL: For user uploads, SKIP most processing
    if is_user_upload:
        # Just ensure required fields exist with some value
        if not transformed.get('name'):
            transformed['name'] = 'Untitled Event'
        if not transformed.get('url'):
            transformed['url'] = f"uploaded://unknown/{hash(str(transformed))}"
        if not transformed.get('venue_city'):
            transformed['venue_city'] = 'Nashville'

        # Return immediately - preserve ALL user data as-is
        return transformed

    # FOR API SOURCES ONLY: Apply transformations
    # Standardize date
    if transformed.get('event_date'):
        try:
            standardized = standardize_date(transformed['event_date'], source)
            if standardized:
                transformed['event_date'] = standardized
        except Exception as e:
            print(f"Date standardization error: {e}")

    # Standardize venue name
    if transformed.get('venue_name'):
        try:
            standardized = standardize_venue_name(transformed['venue_name'])
            if standardized:
                transformed['venue_name'] = standardized
        except Exception as e:
            print(f"Venue standardization error: {e}")

    # Auto-categorize (API sources only)
    try:
        category, genre = categorize_event(
            transformed.get('name', ''),
            transformed.get('description', ''),
            transformed.get('venue_name', '')
        )
        if category:
            transformed['category'] = category
        if genre:
            transformed['genre'] = genre
    except Exception as e:
        print(f"Categorization error: {e}")

    return transformed


def transform_events(raw_events: List[Dict]) -> List[Dict]:
    """Transform a list of raw events."""
    transformed = []
    for event in raw_events:
        result = transform_event(event)
        if result:
            transformed.append(result)
    return transformed
