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
        return psycopg2.connect(os.environ['DATABASE_URL'])
    def fetch_paginated_data(self, page: int, selected_source: str, selected_category: str, search_term: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], int, int]:
        events: List[Dict[str, Any]] = []
        sources: List[str] = []
        categories: List[str] = []
        total_pages: int = 0
        total_events: int = 0
        offset = (page - 1) * self.per_page        
        try:
            self.conn = self._get_connection()
            self.cursor = self.conn.cursor()
            self.cursor.execute("select to_regclass('public.events');")
            table_exists = self.cursor.fetchone()[0]
            self.conn.rollback()            
            if not table_exists:
                return events, sources, categories, 0, 0
            self.cursor.execute("select distinct source from events where source is not null order by source")
            sources = [row[0] for row in self.cursor.fetchall()]
            self.cursor.execute("select distinct category from events where category is not null order by category")
            categories = [row[0] for row in self.cursor.fetchall()]
            conditions = []
            params = []
            if selected_source:
                conditions.append("source=%s")
                params.append(selected_source)
            if selected_category:
                conditions.append("category=%s")
                params.append(selected_category)
            if search_term:
                conditions.append("search_vector @@ plainto_tsquery('english',%s)")
                params.append(search_term)
            where_clause = f"where {' and '.join(conditions)}" if conditions else ""
            count_query = f"select count(*) from events {where_clause}"
            self.cursor.execute(count_query, tuple(params))
            total_events = self.cursor.fetchone()[0]
            total_pages = (total_events + self.per_page - 1) // self.per_page
            order_clause = "order by ts_rank(search_vector,plainto_tsquery('english',%s)) desc" if search_term else "order by event_date asc, name asc"
            final_query = f"select * from events {where_clause} {order_clause} limit %s offset %s"            
            final_params = list(params)
            if search_term:
                final_params.append(search_term) 
            final_params.extend([self.per_page, offset])
            self.cursor.execute(final_query, tuple(final_params))
            colnames = [desc[0] for desc in self.cursor.description]
            events = [dict(zip(colnames, row)) for row in self.cursor.fetchall()]
            return events, sources, categories, total_pages, total_events
        except Exception as e:
            print(f"error extracting data from postgresql: {e}", file=sys.stderr)
            return events, sources, categories, 0, 0            
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
