import os
import sys
import subprocess
import psycopg2
from celery import Celery, chain
from celery.schedules import crontab
import pymupdf
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from transform_data import run_transformations

def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])
celery_app=Celery('tasks',broker='redis://redis:6379/0',backend='redis://redis:6379/0')
@celery_app.task
def run_all_spiders_task():
    print("--- Scrape and Cleanup ---")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("TRUNCATE TABLE events, raw_data RESTART IDENTITY CASCADE;")
        conn.commit()
        print("Database tables events and raw_data truncated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during database TRUNCATE: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    project_dir='/app/scraper'
    scrapy_executable="scrapy"
    env=os.environ.copy()
    env['PYTHONPATH']='/app'

    try:
        result=subprocess.run([scrapy_executable,"list"],cwd=project_dir,capture_output=True,text=True,check=True,env=env)
        spider_names=result.stdout.strip().split('\n')
        print(f"Found spiders: {spider_names}")
    except subprocess.CalledProcessError as e:
        print("--- Could not find spiders. ---")
        print(f"--- STDERR: {e.stderr} ---")
        raise e
    except Exception as e:
        print(f"An unexpected error occurred while trying to list spiders: {e}")
        raise e

    for spider_name in spider_names:
        print(f"--- Running spider: {spider_name} ---")
        try:
            subprocess.run([scrapy_executable,"crawl",spider_name],cwd=project_dir,check=True,env=env)
        except Exception as e:
            print(f"--- Spider '{spider_name}' failed with an error: {e} ---")
            
    print("--- All scraping commands issued. ---")
    return "All spiders have finished."

@celery_app.task
def transform_data_task(previous_task_result):
    print(f"--- Transformation task starting (triggered by completion of previous task) ---")
    run_transformations()
    print("--- all done transforming. ---")
    return "Transformation complete."

@celery_app.task(name='tasks.scrape_and_transform_chain')
def scrape_and_transform_chain():
    workflow=chain(run_all_spiders_task.s(),transform_data_task.s())
    workflow.apply_async()

celery_app.conf.beat_schedule={'run-full-etl-every-3-hours':
    {'task':'tasks.scrape_and_transform_chain','schedule':
        crontab(minute=0,hour='*/3'),'args':()}}
celery_app.conf.timezone='UTC'


@celery_app.task
def process_document_task(filepath, file_extension):
    print(f"--- Document processing task received ---")
    print(f"Filepath: {filepath}")
    print(f"File type: {file_extension}")
    
    raw_data_payload = {
        "source_spider": f"manual_upload_{file_extension}",
        "raw_json": None
    }
    
    if file_extension == 'pdf':
        print("Processing PDF...")
        try:
            doc = pymupdf.open(filepath)
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()
            
            raw_data_payload["raw_json"] = {
                "text": full_text,
                "original_filepath": filepath
            }
            print(f"Extracted {len(full_text)} characters from PDF.")
            
        except Exception as e:
            print(f"Error processing PDF {filepath}: {e}")
            return f"PDF processing failed for {filepath}"
        
    elif file_extension in ['csv', 'json', 'xlsx', 'xls']:
        print(f"This is a {file_extension}. TODO: Add spreadsheet/JSON logic here.")
        
    else:
        print(f"Unsupported file type: {file_extension}")
        return f"Unsupported file type: {file_extension}"

    if raw_data_payload["raw_json"]:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO raw_data (source_spider, raw_json)
            VALUES (%s, %s)
            """
            cursor.execute(insert_query, (
                raw_data_payload["source_spider"],
                json.dumps(raw_data_payload["raw_json"])
            ))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Successfully inserted raw data for {filepath} into database.")
            print(f"--- Now dispatching transformation task for {filepath} ---")
            transform_data_task.delay(raw_data_payload["source_spider"])
            
        except Exception as e:
            print(f"Database insertion failed for {filepath}: {e}")
            return f"Database insertion failed for {filepath}"
            
    return f"Document processing finished for {filepath}"

