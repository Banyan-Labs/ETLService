import os
import psycopg2
import sys
from typing import List, Dict, Any, Tuple
class PostgresExtractor:
    per_page = 25
    def __init__(self):
        self.conn = None
        self.cursor = None
    def _get_connection(self):
        try:
            conn = psycopg2.connect(os.environ['DATABASE_URL'])
            return conn
        except psycopg2.OperationalError as e:
            print(f"CRITICAL: Database connection failed: {e}", file=sys.stderr)
            return None
    def fetch_paginated_data(self, page: int, selected_source: str, selected_category: str, search_term: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], int, int]:
        events: List[Dict[str, Any]] = []
        sources: List[str] = []
        categories: List[str] = []
        total_pages: int = 0
        total_events: int = 0
        offset = (page - 1) * self.per_page
        try:
            self.conn = self._get_connection()
            if self.conn is None:
                raise Exception("Database connection could not be established.")                
            self.cursor = self.conn.cursor()            
            self.cursor.execute("SELECT to_regclass('public.events');")
            table_exists = self.cursor.fetchone()[0]            
            if not table_exists:
                print("Warning: 'events' table does not exist. Returning empty data.", file=sys.stderr)
                return events, sources, categories, 0, 0
            self.cursor.execute("SELECT DISTINCT source FROM events WHERE source IS NOT NULL ORDER BY source")
            sources = [row[0] for row in self.cursor.fetchall()]
            self.cursor.execute("SELECT DISTINCT category FROM events WHERE category IS NOT NULL ORDER BY category")
            categories = [row[0] for row in self.cursor.fetchall()]
            conditions = []
            params = []
            if selected_source:
                conditions.append("source = %s")
                params.append(selected_source)
            if selected_category:
                conditions.append("category = %s")
                params.append(selected_category)
            if search_term:
                conditions.append("search_vector @@ plainto_tsquery('english', %s)")
                params.append(search_term)
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""            
            count_query = f"SELECT COUNT(*) FROM events {where_clause}"
            self.cursor.execute(count_query, tuple(params))
            total_events = self.cursor.fetchone()[0]
            total_pages = (total_events + self.per_page - 1) // self.per_page
            order_clause = "ORDER BY ts_rank(search_vector, plainto_tsquery('english', %s)) DESC" if search_term else "ORDER BY event_date ASC, name ASC"            
            final_query = f"SELECT * FROM events {where_clause} {order_clause} LIMIT %s OFFSET %s"            
            final_params = list(params)
            if search_term:
                final_params.append(search_term)
            final_params.extend([self.per_page, offset])
            self.cursor.execute(final_query, tuple(final_params))
            colnames = [desc[0] for desc in self.cursor.description]
            events = [dict(zip(colnames, row)) for row in self.cursor.fetchall()]            
            return events, sources, categories, total_pages, total_events
        except Exception as e:
            print(f"Error extracting data from PostgreSQL: {e}", file=sys.stderr)
            return [], [], [], 0, 0            
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
