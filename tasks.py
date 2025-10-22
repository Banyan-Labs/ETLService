from transform_data import run_transformations
import os
import sys
import subprocess
from celery import Celery, chain
from celery.schedules import crontab
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
SCRAPY_PROJECT_DIR = '/app/scraper'
DOCUMENT_PROCESSING_TIMEOUT = 300  # 5 minutes max per document

celery_app = Celery('tasks', broker='redis://redis:6379/0',
                    backend='redis://redis:6379/0')


def cleanup_file(filepath):
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"✓ Cleaned up: {os.path.basename(filepath)}")
            return True
    except Exception as e:
        print(f"⚠ Could not delete {os.path.basename(filepath)}: {e}")
    return False


@celery_app.task
def run_all_spiders_task():
    print("--- Celery worker received job: Run All Spiders ---")
    scrapy_executable = "scrapy"
    env = os.environ.copy()
    env['PYTHONPATH'] = '/app'
    try:
        result = subprocess.run(
            [scrapy_executable, "list"],
            cwd=SCRAPY_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True,
            env=env
        )
        spider_names = result.stdout.strip().split('\n')
        spider_names = [s for s in spider_names if s != 'document']
        print(f"Found spiders: {spider_names}")
    except subprocess.CalledProcessError as e:
        print("--- Could not find spiders. The 'scrapy list' command failed. ---")
        print(f"--- STDERR: {e.stderr} ---")
        raise
    except Exception as e:
        print(
            f"An unexpected error occurred while trying to list spiders: {e}")
        raise
    for spider_name in spider_names:
        print(f"--- Running spider: {spider_name} ---")
        try:
            subprocess.run(
                [scrapy_executable, "crawl", spider_name],
                cwd=SCRAPY_PROJECT_DIR,
                check=True,
                env=env
            )
        except Exception as e:
            print(f"--- Spider '{spider_name}' failed with an error: {e} ---")
    return "All spiders have finished."


@celery_app.task
def transform_data_task(previous_task_result):
    """Transform raw data and load into events table."""
    print("--- Celery worker starting transformation task ---")
    run_transformations()
    print("--- Transformation task finished. ---")
    return "Transformation complete."


@celery_app.task(bind=True, max_retries=2, default_retry_delay=10)
def process_document_task(self, filepath, file_type):
    filename = os.path.basename(filepath)
    print(f"📄 Processing document: {filename} ({file_type})")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    env = os.environ.copy()
    env['PYTHONPATH'] = '/app'
    try:
        result = subprocess.run(
            [
                "scrapy", "crawl", "document",
                "-a", f"filepath={filepath}",
                "-a", f"file_type={file_type}"
            ],
            cwd=SCRAPY_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=DOCUMENT_PROCESSING_TIMEOUT,
            check=False,
            env=env
        )

        # ✅ ADD THIS - Show spider output
        print("="*70)
        print("SPIDER OUTPUT:")
        print(result.stdout)
        print("="*70)
        if result.stderr:
            print("SPIDER ERRORS/WARNINGS:")
            print(result.stderr)
            print("="*70)

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout or "Unknown error"
            raise Exception(
                f"Spider failed (exit {result.returncode}): {error_msg[:200]}")
            # error_msg = f"Scrapy command: {' '.join(result.args)}\n"
            # error_msg += f"STDERR: {result.stderr.strip() or 'No STDERR output'}\n"
            # error_msg += f"STDOUT: {result.stdout.strip() or 'No STDOUT output'}\n"
            print(
                f"✗ DOCUMENT SPIDER FAILED (exit {result.returncode})", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            raise Exception(
                f"Spider failed (exit {result.returncode}): {error_msg[:200]}")
        print(f"✓ Document processing complete: {filename}")
        return f"Successfully processed {filename}"
    except subprocess.TimeoutExpired:
        print(
            f"⏱ Timeout processing {filename} (>{DOCUMENT_PROCESSING_TIMEOUT}s)")
        raise Exception(f"Processing timeout ({DOCUMENT_PROCESSING_TIMEOUT}s)")
    except Exception as e:
        print(f"✗ Error processing {filename}: {str(e)[:200]}")
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e)
        raise
    finally:
        cleanup_file(filepath)


@celery_app.task(name='tasks.scrape_and_transform_chain')
def scrape_and_transform_chain():
    """Chain together scraping and transformation tasks."""
    workflow = chain(run_all_spiders_task.s(), transform_data_task.s())
    workflow.apply_async()


celery_app.conf.beat_schedule = {
    'run-full-etl-every-3-hours': {
        'task': 'tasks.scrape_and_transform_chain',
        'schedule': crontab(minute=0, hour='*/3'),
        'args': ()
    }
}
celery_app.conf.timezone = 'UTC'
