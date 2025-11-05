import sys
from flask import Flask, render_template_string, redirect, url_for, request, flash, jsonify
from datetime import datetime
from werkzeug.utils import secure_filename
from tasks import scrape_and_transform_chain, process_document_task
from db_extractor import PostgresExtractor
import os
import redis
UPLOAD_FOLDER = '/app/uploads'
ALLOWED_EXTENSIONS = {'csv', 'json', 'pdf', 'xlsx', 'xls', 'docx'}
PER_PAGE = 25
app = Flask(__name__)
app.config['SECRET_KEY'] = 'thisismykey'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
db_manager = PostgresExtractor()
def get_redis_connection():
    try:
        r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        r.ping()
        print("Connected to Redis successfully!")
        return r
    except Exception as e:
        print(f"Error connecting to Redis: {e}", file=sys.stderr)
        return None
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
def format_date_filter(iso_date_str):
    if not iso_date_str:
        return ""
    try:
        dt_object = datetime.fromisoformat(
            iso_date_str.replace('Z', '+00:00').split('+')[0])
        return dt_object.strftime('%b %d, %Y at %I:%M %p')
    except (ValueError, TypeError):
        return iso_date_str
def get_pagination_range(current_page, total_pages, max_visible=5):
    start_page = max(1, current_page - max_visible // 2)
    end_page = min(total_pages, start_page + max_visible - 1)
    if end_page - start_page + 1 < max_visible:
        start_page = max(1, end_page - max_visible + 1)
    pages = list(range(start_page, end_page + 1))
    return {
        'show_first': start_page > 1,
        'show_last': end_page < total_pages,
        'show_left_ellipsis': start_page > 2,
        'show_right_ellipsis': end_page < total_pages - 1,
        'pages': pages
    }
app.jinja_env.filters['format_date'] = format_date_filter
@app.route('/')
def index():
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        try:
            os.makedirs(app.config['UPLOAD_FOLDER'])
        except OSError as e:
            print(f"Error creating upload directory: {e}", file=sys.stderr)            
    page = request.args.get('page', 1, type=int)
    selected_source = request.args.get('source', '')
    selected_category = request.args.get('category', '')
    search_term = request.args.get('search', '').strip()    
    events, sources, categories, total_pages, total_events = [], [], [], 0, 0    
    scrape_in_progress = False
    redis_client = get_redis_connection()
    if redis_client:
        try:
            if redis_client.get('scrape_status') == 'running':
                scrape_in_progress = True
                print("Scrape in progress, skipping database query on page load.")
        except Exception as e:
            print(f"Error checking Redis status: {e}", file=sys.stderr)
    if not scrape_in_progress:
        try:
            events, sources, categories, total_pages, total_events = db_manager.fetch_paginated_data(
                page, selected_source, selected_category, search_term
            )
        except Exception as e:
            print(f"Error fetching data from database: {e}", file=sys.stderr)
            flash('Error fetching data from the database.', 'error')
    else:
        flash('Hang Tight! Im working on getting all the information you need!!!', 'success')        
    pagination = get_pagination_range(page, total_pages)    
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETL Service</title>
    <style>
        body { font-family: sans-serif; margin: 2em; background-color: #f4f4f4; color: #333; }
        h1, h3 { color: #555; }
        a { color: #007bff; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .container { background-color: #fff; padding: 2em; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .upload-form { padding: 15px; border: 1px dashed #ccc; border-radius: 5px; background-color: #f9f9f9; margin-bottom: 20px; }
        .upload-form input[type="file"], .upload-form button { margin-top: 10px; padding: 8px 12px; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; table-layout: fixed; background-color: #fff; }
        th, td { border: 1px solid #ddd; text-align: left; padding: 10px; vertical-align: top; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        th { background-color: #e9ecef; font-weight: bold; }
        tr:nth-child(even) { background-color: #f8f9fa; }
        .controls-container { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; gap: 10px; }
        .filter-group { display: flex; gap: 10px; flex-wrap: wrap; }
        .filter-group select, .filter-group input[type="text"], .filter-group button, .action-button { padding: 8px 15px; font-size: 1rem; border: 1px solid #ccc; border-radius: 4px; }
        .action-button { background-color: #007bff; color: white; cursor: pointer; border: none; }
        .action-button:hover { background-color: #0056b3; }
        .clear-button { background-color: #dc3545; }
        .clear-button:hover { background-color: #c82333; }
        .manual-run-button { background-color: #28a745; }
        .manual-run-button:hover { background-color: #218838; }
        .process-file-button { background-color: #17a2b8; }
        .process-file-button:hover { background-color: #138496; }
        .pagination { margin-top: 20px; display: flex; justify-content: center; gap: 5px; align-items: center; flex-wrap: wrap; }
        .pagination a, .pagination span {
            color: #007bff; padding: 8px 16px; text-decoration: none; transition: background-color .3s;
            border: 1px solid #ddd; border-radius: 4px; display: inline-block; background-color: #fff;
        }
        .pagination a:hover { background-color: #e9ecef; }
        .pagination a.active { background-color: #007bff; color: white; border: 1px solid #007bff; }
        .pagination span.disabled { color: #6c757d; cursor: not-allowed; background-color: #e9ecef; border-color: #dee2e6; }
        .pagination .ellipsis { border: none; background: none; padding: 8px 4px; color: #6c757d; }
        .flash-message { padding: 10px; margin-bottom: 15px; border-radius: 4px; }
        .flash-error { background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .flash-success { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        #loading-overlay { 
            display: none; 
            position: fixed; 
            top: 0; 
            left: 0; 
            width: 100%; 
            height: 100%; 
            background: rgba(0,0,0,0.5); 
            color: white; 
            z-index: 1000; 
            text-align: center; 
            padding-top: 20%; 
        }
        @media (max-width: 768px) {
            .controls-container { flex-direction: column; align-items: stretch; }
            .filter-group { flex-direction: column; align-items: stretch; }
            .filter-group select, .filter-group input[type="text"], .filter-group button { width: 100%; box-sizing: border-box; margin-bottom: 5px; }
            .action-button { width: 100%; box-sizing: border-box; margin-bottom: 5px;}
            th, td { white-space: normal; }
        }
    </style>
</head>
<body>
    <div id="loading-overlay">
        <h2>Hang Tight! I'm working on getting all the information you need!!!</h2>
        <p>This page will automatically refresh when complete.</p>
    </div>
    <div class="container">
        <h1>ETL Service</h1>
        {% with messages = get_flashed_messages(with_categories=true) %}
          {% if messages %}
            {% for category, message in messages %}
              <div class="flash-message flash-{{ category }}">{{ message }}</div>
            {% endfor %}
          {% endif %}
        {% endwith %}
        <form action="{{ url_for('upload_document') }}" method="post" enctype="multipart/form-data" class="upload-form" id="upload-form">
            <h3>Upload Documents for Processing (PDF, CSV, JSON, Excel)</h3>
            <input type="file" name="document" required multiple>
            <button type="submit" class="action-button process-file-button">Process Files</button>
        </form>        
        <div class="controls-container">
            <form action="{{ url_for('index') }}" method="get" class="filter-group">
                <select name="source">
                    <option value="">All Sources</option>
                    {% for source in sources %}
                        <option value="{{ source }}" {% if source == selected_source %}selected{% endif %}>{{ source }}</option>
                    {% endfor %}
                </select>
                <select name="category">
                    <option value="">All Categories</option>
                    {% for category in categories %}
                        <option value="{{ category }}" {% if category == selected_category %}selected{% endif %}>{{ category }}</option>
                    {% endfor %}
                </select>
                 <input type="text" name="search" placeholder="Search events..." value="{{ search_term }}">
                <button type="submit" class="action-button">Filter/Search</button>
                <a href="{{ url_for('index') }}" class="action-button clear-button" style="text-decoration: none;">Reset Filters</a>
            </form>
            <div style="display: flex; gap: 10px;">
                <form action="/clear" method="post" style="display: inline-block;">
                    <button type="submit" class="action-button clear-button">CLEAR ALL DATA</button>
                </form>
                <form action="/launch_manual_scrape" method="post" style="display: inline-block;" id="scrape-form">
                    <button type="submit" class="action-button manual-run-button">Manual Scrape Run</button>
                </form>
            </div>
        </div>
        {% if total_events == 0 and not search_term and not selected_source and not selected_category %}
             <p>No events found. Data refreshes automatically or run a manual scrape.</p>
        {% elif total_events == 0 %}
             <p>No events found matching your criteria.</p>
        {% else %}
            <p>Displaying {{ events|length }} events on page {{ page }}. Total matching events: <strong>{{ total_events }}</strong></p>
            <table>
                <thead>
                    <tr><th>Name</th><th>Date / Season</th><th>Venue</th><th>Address</th><th>Source</th></tr>
                </thead>
                <tbody>
                    {% for event in events %}
                    <tr>
                        <td><a href="{{ event.url or '#' }}" target="_blank" rel="noopener noreferrer">{{ event.name or 'N/A' }}</a></td>
                        <td>{% if event.event_date %}{{ event.event_date | format_date }}{% elif event.season %}{{ event.season }}{% else %}N/A{% endif %}</td>
                        <td>{{ event.venue_name or 'N/A' }}</td>
                        <td>{{ event.venue_address or 'N/A' }}</td>
                        <td>{{ event.source or 'N/A' }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>            
            {% if total_pages > 1 %}
            <div class="pagination">
                {% set page_args = {'source': selected_source, 'category': selected_category, 'search': search_term} %}
                {% if page > 1 %}
                    <a href="{{ url_for('index', page=page-1, **page_args) }}">&laquo; Back</a>
                {% else %}
                    <span class="disabled">&laquo; Back</span>
                {% endif %}                
                {% if pagination.show_first %}
                    <a href="{{ url_for('index', page=1, **page_args) }}">1</a>
                {% endif %}
                {% if pagination.show_left_ellipsis %}
                    <span class="ellipsis">...</span>
                {% endif %}                
                {% for p in pagination.pages %}
                    {% if p == page %}
                        <a href="#" class="active">{{ p }}</a>
                    {% else %}
                        <a href="{{ url_for('index', page=p, **page_args) }}">{{ p }}</a>
                    {% endif %}
                {% endfor %}                
                {% if pagination.show_right_ellipsis %}
                    <span class="ellipsis">...</span>
                {% endif %}
                {% if pagination.show_last %}
                    <a href="{{ url_for('index', page=total_pages, **page_args) }}">{{ total_pages }}</a>
                {% endif %}                
                {% if page < total_pages %}
                    <a href="{{ url_for('index', page=page+1, **page_args) }}">Next &raquo;</a>
                {% else %}
                    <span class="disabled">Next &raquo;</span>
                {% endif %}
            </div>
            {% endif %}
        {% endif %}
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const loadingOverlay = document.getElementById('loading-overlay');
            const scrapeForm = document.getElementById('scrape-form');
            const uploadForm = document.getElementById('upload-form');            
            async function checkStatus() {
                try {
                    const response = await fetch('/scrape_status');
                    if (!response.ok) {
                        console.error('Scrape status check failed:', response.status);
                        loadingOverlay.style.display = 'none';
                        return;
                    }
                    const data = await response.json();                    
                    if (data.status === 'running') {
                        loadingOverlay.style.display = 'block';
                        setTimeout(checkStatus, 5000); 
                    } else if (data.status === 'complete') {
                        loadingOverlay.style.display = 'none';
                        window.location.href = window.location.pathname + window.location.search; 
                    } else {
                        loadingOverlay.style.display = 'none';
                    }
                } catch (error) {
                    console.error('Error checking scrape status:', error);
                    loadingOverlay.style.display = 'none';
                }
            }            
            checkStatus();
            if (scrapeForm) {
                scrapeForm.addEventListener('submit', function() {
                    loadingOverlay.style.display = 'block';
                });
            }            
            if (uploadForm) {
                uploadForm.addEventListener('submit', function() {
                    loadingOverlay.style.display = 'block';
                });
            }
        });
    </script>
</body>
</html>
    """
    return render_template_string(
        html,
        events=events,
        page=page,
        total_pages=total_pages,
        pagination=pagination,
        sources=sources,
        categories=categories,
        selected_source=selected_source,
        selected_category=selected_category,
        search_term=search_term,
        total_events=total_events
    )
@app.route('/scrape_status')
def scrape_status():
    redis_client = get_redis_connection()
    if not redis_client:
        return jsonify({'status': 'idle', 'error': 'Redis not connected'})        
    status = 'idle'
    try:
        status_from_redis = redis_client.get('scrape_status')
        if status_from_redis == 'complete':
            redis_client.set('scrape_status', 'idle') 
            status = 'complete'
        elif status_from_redis == 'running':
            status = 'running'
        else:
            status = 'idle'
    except Exception as e:
        print(f"Error reading Redis status: {e}", file=sys.stderr)
        status = 'idle'         
    return jsonify({'status': status})
@app.route('/upload_document', methods=['POST'])
def upload_document():
    uploaded_files = request.files.getlist('document')
    if not uploaded_files or uploaded_files[0].filename == '':
        print("ALERT: No files selected.")
        flash('No files selected for upload.', 'error')
        return redirect(url_for('index'))
    redis_client = get_redis_connection()
    if redis_client:
        try:
            redis_client.set('scrape_status', 'running')
            print("Set scrape_status to 'running' for file upload.")
        except Exception as e:
            print(f"Error setting Redis status: {e}", file=sys.stderr)
    else:
        print("Redis client not available, skipping status set.")
    files_processed = 0
    files_skipped = 0    
    for file in uploaded_files:
        if file and allowed_file(file.filename):
            try:
                filename = secure_filename(file.filename)
                file_extension = filename.rsplit('.', 1)[1].lower()
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)
                print(f"✓ Saved file for processing: {filepath}")
                process_document_task.delay(filepath, file_extension)
                print(f"Dispatched document task for {filename}.")
                files_processed += 1
            except Exception as e:
                print(
                    f"ERROR saving or dispatching task for {file.filename}: {e}", file=sys.stderr)
                files_skipped += 1
        elif file:
            print(f"ALERT: File type not allowed, skipped: {file.filename}")
            files_skipped += 1            
    if files_processed > 0:
        flash(
            f'Successfully dispatched {files_processed} file(s) for processing.', 'success')
    if files_skipped > 0:
        flash(
            f'Skipped {files_skipped} file(s) due to errors or disallowed types.', 'warning')            
    return redirect(url_for('index'))
@app.route('/clear', methods=['POST'])
def clear_data():
    conn = None
    try:
        conn = db_manager._get_connection()
        if conn is None:
            raise ConnectionError("Failed to get database connection.")
        cursor = conn.cursor()
        cursor.execute(
            "TRUNCATE TABLE events, raw_data RESTART IDENTITY CASCADE;")
        conn.commit()
        print("Database cleared by user action.")
        flash('All event and raw data cleared successfully.', 'success')
        cursor.close()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error clearing PostgreSQL database: {e}", file=sys.stderr)
        flash('Error clearing database.', 'error')
    finally:
        if conn:
            conn.close()
    return redirect(url_for('index'))
@app.route('/launch_manual_scrape', methods=['POST'])
def launch_manual_scrape():
    try:
        redis_client = get_redis_connection()
        if redis_client:
            try:
                redis_client.set('scrape_status', 'running')
                print("Set scrape_status to 'running' in Redis.")
            except Exception as e:
                 print(f"Error setting Redis status: {e}", file=sys.stderr)
        else:
            print("Redis client not available, skipping status set.")            
        print("Dispatching ETL chain to Celery worker.")
        scrape_and_transform_chain.delay()
        flash('Manual scrape and transform process initiated.', 'success')
    except Exception as e:
        print(f"Error dispatching scrape task: {e}", file=sys.stderr)
        flash('Error initiating manual scrape.', 'error')
    return redirect(url_for('index'))