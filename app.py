import os
import psycopg2
import subprocess
import sys
from flask import Flask, render_template_string, redirect, url_for, request, flash
from werkzeug.utils import secure_filename
import uuid
from datetime import datetime
from urllib.parse import urlencode
from celery import chain
from tasks import scrape_and_transform_chain
app = Flask(__name__)
app.secret_key = os.environ.get(
    'SECRET_KEY', 'dev-secret-key-change-in-production')
UPLOAD_FOLDER = '/app/uploaded_documents'
ALLOWED_EXTENSIONS = {'csv', 'json', 'xlsx', 'xls'}
MAX_FILE_SIZE = 16 * 1024 * 1024  # 16MB
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE
PER_PAGE = 25


def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def format_date_filter(iso_date_str):
    if not iso_date_str:
        return ""
    try:
        dt_object = datetime.fromisoformat(iso_date_str.split('+')[0])
        return dt_object.strftime('%b %d, %Y at %I:%M %p')
    except (ValueError, TypeError):
        return iso_date_str


def get_pagination_range(current_page, total_pages, max_visible=5):
    """Calculate smart pagination range"""
    if total_pages <= max_visible + 2:
        return {
            'show_first': False,
            'show_last': False,
            'show_left_ellipsis': False,
            'show_right_ellipsis': False,
            'pages': list(range(1, total_pages + 1))
        }

    if current_page <= 3:
        return {
            'show_first': False,
            'show_last': True,
            'show_left_ellipsis': False,
            'show_right_ellipsis': True,
            'pages': list(range(1, min(6, total_pages)))
        }
    elif current_page >= total_pages - 2:
        return {
            'show_first': True,
            'show_last': False,
            'show_left_ellipsis': True,
            'show_right_ellipsis': False,
            'pages': list(range(max(total_pages - 4, 2), total_pages + 1))
        }
    else:
        return {
            'show_first': True,
            'show_last': True,
            'show_left_ellipsis': True,
            'show_right_ellipsis': True,
            'pages': [current_page - 1, current_page, current_page + 1]
        }


app.jinja_env.filters['format_date'] = format_date_filter


def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


UPLOAD_FORM_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Upload Document - Nashville ETL</title>
    <style>
        body { font-family: sans-serif; margin: 2em; max-width: 800px; }
        .upload-box { 
            border: 2px dashed #ccc; 
            padding: 40px; 
            text-align: center; 
            border-radius: 8px;
            background: #f9f9f9;
        }
        .btn { 
            background-color: #008CBA; 
            color: white; 
            padding: 12px 24px; 
            border: none; 
            border-radius: 4px; 
            cursor: pointer;
            font-size: 16px;
        }
        .btn:hover { background-color: #006a8e; }
        .back-link { margin-top: 20px; display: block; }
        .file-input { margin: 20px 0; }
        .info-box {
            background: #e3f2fd;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
            text-align: left;
        }
        .flash-messages { margin: 20px 0; }
        .flash {
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 10px;
        }
        .flash.success {
            background-color: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .flash.error {
            background-color: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
    </style>
</head>
<body>
    <h1>Upload Document for Processing</h1>
    
    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            <div class="flash-messages">
                {% for category, message in messages %}
                    <div class="flash {{ category }}">{{ message }}</div>
                {% endfor %}
            </div>
        {% endif %}
    {% endwith %}
    
    <div class="info-box">
        <h3>📄 Supported File Types:</h3>
        <ul>
            <li><strong>CSV</strong> - Comma-separated values</li>
            <li><strong>JSON</strong> - JSON arrays or objects</li>
            <li><strong>Excel</strong> - .xlsx or .xls files</li>
        </ul>
        <p><strong>Max file size:</strong> 16MB</p>
        <p><strong>Expected columns:</strong> name, url, venue_name, venue_address, event_date, description, category, venue_city</p>
    </div>
    
    <form method="POST" enctype="multipart/form-data" class="upload-box">
        <h2>Choose a file to upload</h2>
        <div class="file-input">
            <input type="file" name="file" accept=".csv,.json,.xlsx,.xls" required>
        </div>
        <button type="submit" class="btn">Upload and Process</button>
    </form>
    
    <a href="{{ url_for('index') }}" class="back-link">← Back to Dashboard</a>
</body>
</html>
"""


@app.route('/')
def index():
    """Main dashboard showing all events."""
    page = request.args.get('page', 1, type=int)
    selected_source = request.args.get('source', '')
    selected_category = request.args.get('category', '')
    search_term = request.args.get('search', '').strip()
    offset = (page - 1) * PER_PAGE
    conn = None
    cursor = None
    events, sources, categories, total_pages, total_events = [], [], [], 0, 0
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT to_regclass('public.events');")
        table_exists = cursor.fetchone()[0]
        conn.rollback()
        if not table_exists:
            pass
        else:
            cursor.execute(
                "SELECT DISTINCT source FROM events WHERE source IS NOT NULL ORDER BY source")
            sources = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                "SELECT DISTINCT category FROM events WHERE category IS NOT NULL ORDER BY category")
            categories = [row[0] for row in cursor.fetchall()]
            conditions = []
            params = []
            if selected_source:
                conditions.append("source=%s")
                params.append(selected_source)
            if selected_category:
                conditions.append("category=%s")
                params.append(selected_category)
            if search_term:
                conditions.append(
                    "search_vector @@ plainto_tsquery('english',%s)")
                params.append(search_term)
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            count_query = f"SELECT COUNT(*) FROM events {where_clause}"
            cursor.execute(count_query, tuple(params))
            total_events = cursor.fetchone()[0]
            total_pages = (total_events + PER_PAGE - 1) // PER_PAGE
            order_clause = "ORDER BY ts_rank(search_vector,plainto_tsquery('english',%s)) DESC" if search_term else "ORDER BY event_date ASC, name ASC"
            final_query = f"SELECT * FROM events {where_clause} {order_clause} LIMIT %s OFFSET %s"
            final_params = list(params)
            if search_term:
                final_params.append(search_term)
            final_params.extend([PER_PAGE, offset])
            cursor.execute(final_query, tuple(final_params))
            colnames = [desc[0] for desc in cursor.description]
            events = [dict(zip(colnames, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(
            f"Error querying events,sources,categories: {e}", file=sys.stderr)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    pagination = get_pagination_range(page, total_pages)
    current_filters = {
        'source': selected_source,
        'category': selected_category,
        'search': search_term
    }

    html = """
<!DOCTYPE html>
<html>
<head>
    <title>Nashville ETL Dashboard</title>
    <style>
        body { font-family: sans-serif; margin: 2em; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; table-layout: fixed; }
        th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; vertical-align: top; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        th:nth-child(1) { width: 30%; }
        th:nth-child(2) { width: 15%; }
        th:nth-child(3) { width: 15%; }
        th:nth-child(4) { width: 25%; }
        th:nth-child(5) { width: 10%; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .controls-container { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap;}
        .filter-group { display: flex; gap: 10px; }
        .filter-group select, .filter-group input[type="text"], .filter-group button { padding: 8px; font-size: 16px; border: 1px solid #ccc; border-radius: 4px; }
        .action-button { background-color: #008CBA; color: white; padding: 10px 24px; text-align: center; font-size: 16px; cursor: pointer; border: none; margin-right: 10px; border-radius: 4px;}
        .clear-button { background-color: #f44336; }
        .pagination { margin-top: 10px; display: flex; gap: 5px; align-items: center; }
        .pagination a, .pagination span {
            color: black;
            padding: 8px 16px;
            text-decoration: none;
            transition: background-color .3s;
            border: 1px solid #ddd;
            border-radius: 4px;
            display: inline-block;
        }
        .pagination a.active { background-color: #008CBA; color: white; border: 1px solid #008CBA; }
        .pagination a:hover:not(.active) { background-color: #ddd; }
        .pagination span.disabled {
            color: #ccc;
            cursor: not-allowed;
            background-color: #f5f5f5;
        }
        .pagination .ellipsis {
            border: none;
            background: none;
            padding: 8px 4px;
        }
        .flash-messages { margin: 20px 0; }
        .flash {
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 10px;
        }
        .flash.success {
            background-color: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .flash.error {
            background-color: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
    </style>
</head>
<body>
    <h1>Nashville ETL Dashboard (Pre-Scheduled Data)</h1>
    
    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            <div class="flash-messages">
                {% for category, message in messages %}
                    <div class="flash {{ category }}">{{ message }}</div>
                {% endfor %}
            </div>
        {% endif %}
    {% endwith %}
    
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
            <button type="submit" class="action-button">Filter/Search</button>
            <a href="{{ url_for('index') }}" class="action-button clear-button" style="text-decoration: none;">Reset Filters</a>
        </form>
        <a href="{{ url_for('upload_document') }}" class="action-button" style="background-color: #4CAF50; text-decoration: none;">
            📤 Upload Document
        </a>
        <form action="/clear" method="post" style="display: inline-block;">
            <button class="action-button clear-button">CLEAR ALL DATA</button>
        </form>
        <form action="/launch_manual_scrape" method="post" style="display: inline-block;">
            <button class="action-button" style="background-color: green;">Manual Run</button>
        </form>
    </div>
    {% if total_events == 0 %}
        <p>No events found matching your criteria. Data refreshes automatically every 3 hours.</p>
    {% else %}
        <p>Displaying {{ events|length }} events on this page. Total matching events: <strong>{{ total_events }}</strong></p>
        <table>
            <tr><th>Name</th><th>Date / Season</th><th>Venue</th><th>Address</th><th>Source</th></tr>
            {% for event in events %}
            <tr>
                <td><a href="{{ event.url }}" target="_blank">{{ event.name }}</a></td>
                <td>{% if event.event_date %}{{ event.event_date | format_date }}{% elif event.season %}{{ event.season }}{% else %}N/A{% endif %}</td>
                <td>{{ event.venue_name }}</td>
                <td>{{ event.venue_address }}</td>
                <td>{{ event.source }}</td>
            </tr>
            {% endfor %}
        </table>
        
        {% if total_pages > 1 %}
        <div class="pagination">
            {% set page_args = {'source': selected_source, 'category': selected_category, 'search': search_term} %}
            
            <!-- Back Button -->
            {% if page > 1 %}
                <a href="{{ url_for('index', page=page-1, **page_args) }}">&laquo; Back</a>
            {% else %}
                <span class="disabled">&laquo; Back</span>
            {% endif %}
            
            <!-- First Page -->
            {% if pagination.show_first %}
                <a href="{{ url_for('index', page=1, **page_args) }}">1</a>
            {% endif %}
            
            <!-- Left Ellipsis -->
            {% if pagination.show_left_ellipsis %}
                <span class="ellipsis">...</span>
            {% endif %}
            
            <!-- Page Numbers -->
            {% for p in pagination.pages %}
                {% if p == page %}
                    <a href="#" class="active">{{ p }}</a>
                {% else %}
                    <a href="{{ url_for('index', page=p, **page_args) }}">{{ p }}</a>
                {% endif %}
            {% endfor %}
            
            <!-- Right Ellipsis -->
            {% if pagination.show_right_ellipsis %}
                <span class="ellipsis">...</span>
            {% endif %}
            
            <!-- Last Page -->
            {% if pagination.show_last %}
                <a href="{{ url_for('index', page=total_pages, **page_args) }}">{{ total_pages }}</a>
            {% endif %}
            
            <!-- Next Button -->
            {% if page < total_pages %}
                <a href="{{ url_for('index', page=page+1, **page_args) }}">Next &raquo;</a>
            {% else %}
                <span class="disabled">Next &raquo;</span>
            {% endif %}
        </div>
        {% endif %}
    {% endif %}
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


@app.route('/upload_document', methods=['GET', 'POST'])
def upload_document():
    """Handle document uploads from users."""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(url_for('index'))
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(url_for('index'))
        if file and allowed_file(file.filename):
            original_filename = secure_filename(file.filename)
            unique_id = str(uuid.uuid4())[:8]
            file_extension = original_filename.rsplit('.', 1)[1].lower()
            unique_filename = f"{unique_id}_{original_filename}"
            filepath = os.path.join(
                app.config['UPLOAD_FOLDER'], unique_filename)

            try:
                os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
                file.save(filepath)
                from tasks import process_document_task
                task = process_document_task.delay(filepath, file_extension)
                flash(
                    f'File "{original_filename}" uploaded successfully! Processing in background...', 'success')
                return redirect(url_for('index'))
            except Exception as e:
                flash(f'Upload failed: {str(e)}', 'error')
                return redirect(url_for('index'))
        else:
            flash(
                f'Invalid file type. Allowed: {", ".join(ALLOWED_EXTENSIONS)}', 'error')
            return redirect(url_for('index'))

    # GET request - show upload form
    return render_template_string(UPLOAD_FORM_HTML)


@app.route('/clear', methods=['POST'])
def clear_data():
    """Clear all data from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "TRUNCATE TABLE events, raw_data RESTART IDENTITY CASCADE;")
        conn.commit()
        flash('All data cleared successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error clearing data: {str(e)}', 'error')
        print(f"Error clearing PostgreSQL database: {e}", file=sys.stderr)
    finally:
        cursor.close()
        conn.close()
    return redirect(url_for('index'))


@app.route('/launch_manual_scrape', methods=['POST'])
def launch_manual_scrape():
    """Trigger manual scraping of all sources."""
    clear_data()
    print("Dispatching ETL chain to Celery worker.")
    scrape_and_transform_chain.delay()
    flash('Manual scrape initiated! Data will refresh in background.', 'success')
    return redirect(url_for('index'))


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)
