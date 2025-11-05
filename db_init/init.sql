\c nashville_tourism;
CREATE TABLE IF NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    source_spider TEXT,
    raw_json TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    name TEXT,
    url TEXT UNIQUE,
    event_date TEXT,
    venue_name TEXT,
    venue_address TEXT,
    description TEXT,
    source TEXT,
    category TEXT,
    genre TEXT,
    season TEXT,
    latitude REAL,
    longitude REAL,
    search_vector TSVECTOR
);
CREATE INDEX IF NOT EXISTS idx_events_order_filter
ON events (source, event_date ASC, name ASC);
CREATE INDEX IF NOT EXISTS idx_events_fulltext
ON events USING GIN (search_vector);
CREATE INDEX IF NOT EXISTS idx_events_category
ON events (category);
CREATE INDEX IF NOT EXISTS idx_events_source_category
ON events (source, category);