from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import Flask, abort, flash, redirect, render_template_string, request, send_from_directory, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
INSTANCE_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = INSTANCE_DIR / "ppt_portal.db"
ALLOWED_UPLOADS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "xls", "xlsx", "csv", "txt"}

ADMIN_EMAIL = "admin@pinnacleperformancetax.com"
ADMIN_PASSWORD = "admin123"

BRAND = {
    "business_name": "Pinnacle Performance Tax and Accounting",
    "website": "www.pinnacleperformancetax.com",
    "email": "pinnacleperformancetax@gmail.com",
    "phone": "478-338-1632",
}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "ppt-dev-secret-change-me")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query_db(sql, args=(), one=False):
    conn = get_db()
    try:
        rows = conn.execute(sql, args).fetchall()
        return (rows[0] if rows else None) if one else rows
    finally:
        conn.close()


def execute_db(sql, args=()):
    conn = get_db()
    try:
        cur = conn.execute(sql, args)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def money(value):
    try:
        return "${:,.2f}".format(float(value or 0))
    except Exception:
        return "$0.00"


app.jinja_env.filters["money"] = money


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_UPLOADS


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT UNIQUE,
        password_hash TEXT,
        role TEXT DEFAULT 'client',
        client_id INTEGER,
        is_active INTEGER DEFAULT 1
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        business_name TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        client_type TEXT DEFAULT 'Individual',
        status TEXT DEFAULT 'Active',
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        date TEXT,
        description TEXT,
        type TEXT,
        category TEXT,
        amount REAL DEFAULT 0,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        document_name TEXT,
        name TEXT,
        filename TEXT,
        original_filename TEXT,
        tax_year TEXT,
        category TEXT,
        status TEXT DEFAULT 'Received',
        notes TEXT,
        uploaded_by TEXT,
        uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS tax_returns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        tax_year TEXT,
        service TEXT,
        status TEXT DEFAULT 'Waiting on Client',
        due_date TEXT,
        invoice_id INTEGER,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        invoice_number TEXT,
        amount REAL DEFAULT 0,
        status TEXT DEFAULT 'Unpaid',
        due_date TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER,
        client_id INTEGER,
        date TEXT DEFAULT CURRENT_TIMESTAMP,
        method TEXT,
        amount REAL DEFAULT 0,
        reference TEXT,
        notes TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        title TEXT,
        start_at TEXT,
        end_at TEXT,
        location TEXT,
        meeting_link TEXT,
        status TEXT DEFAULT 'Requested',
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS crm_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        name TEXT,
        email TEXT,
        phone TEXT,
        source TEXT,
        status TEXT DEFAULT 'New',
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        sender_role TEXT,
        sender_name TEXT,
        subject TEXT,
        body TEXT,
        reply TEXT,
        status TEXT DEFAULT 'Open',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Guarantee admin login every app start/init.
    c.execute("DELETE FROM users WHERE lower(email) IN (?, ?)", (ADMIN_EMAIL.lower(), "admin@example.com"))
    c.execute(
        "INSERT INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,?,?,1)",
        ("PPT Admin", ADMIN_EMAIL, generate_password_hash(ADMIN_PASSWORD), "admin", None),
    )

    c.execute("SELECT COUNT(*) AS total FROM clients")
    if c.fetchone()["total"] == 0:
        c.execute(
            "INSERT INTO clients(name,business_name,email,phone,client_type,status,notes) VALUES (?,?,?,?,?,?,?)",
            ("Sample Client", "Sample Business LLC", "client@example.com", "478-555-0110", "Business", "Active", "Demo client"),
        )
        client_id = c.lastrowid
        c.execute(
            "INSERT OR IGNORE INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,?,?,1)",
            ("Sample Client", "client@example.com", generate_password_hash("client123"), "client", client_id),
        )
        c.execute(
            "INSERT INTO transactions(client_id,date,description,type,category,amount,notes) VALUES (?,?,?,?,?,?,?)",
            (client_id, "2026-04-20", "Monthly bookkeeping retainer", "income", "Bookkeeping Income", 450.00, "Demo income"),
        )
        c.execute(
            "INSERT INTO invoices(client_id,invoice_number,amount,status,due_date,notes) VALUES (?,?,?,?,?,?)",
            (client_id, "PPT-1001", 375.00, "Unpaid", "2026-04-15", "Demo invoice"),
        )
        invoice_id = c.lastrowid
        c.execute(
            "INSERT INTO tax_returns(client_id,tax_year,service,status,due_date,invoice_id,notes) VALUES (?,?,?,?,?,?,?)",
            (client_id, "2025", "1040 + Schedule C", "Waiting on Client", "2026-04-15", invoice_id, "Demo return"),
        )
        c.execute(
            "INSERT INTO documents(client_id,document_name,name,filename,original_filename,tax_year,category,status,notes,uploaded_by) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (client_id, "W-2", "W-2", "", "sample_w2.pdf", "2025", "Tax Documents", "Requested", "Admin"),
        )

    conn.commit()
    conn.close()


class CurrentUser:
    def __init__(self, row=None):
        self.row = row

    @property
    def is_authenticated(self):
        return self.row is not None

    def __getattr__(self, item):
        if self.row is not None and item in self.row.keys():
            return self.row[item]
        raise AttributeError(item)


def current_user():
    uid = session.get("user_id")
    if not uid:
        return CurrentUser()
    return CurrentUser(query_db("SELECT * FROM users WHERE id=?", (uid,), one=True))


@app.context_processor
def inject():
    return {"brand": BRAND, "current_user": current_user()}


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user().is_authenticated:
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user.is_authenticated:
            return redirect(url_for("login"))
        if user.role != "admin":
            abort(403)
        return fn(*args, **kwargs)
    return wrapper


def client_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user.is_authenticated:
            return redirect(url_for("login"))
        if user.role != "client":
            abort(403)
        return fn(*args, **kwargs)
    return wrapper


BASE_HTML = """
<!doctype html><html><head><meta charset="utf-8"><title>PPT Portal</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#f6f8f6;color:#111827;font-family:Arial,Helvetica,sans-serif}.layout{display:flex;min-height:100vh}.side{width:245px;background:linear-gradient(180deg,#0b5f2a,#063f1d);color:white;padding:24px;position:sticky;top:0;height:100vh}.brand{font-size:22px;font-weight:900;line-height:1.05;margin-bottom:16px}.contact{font-size:11px;line-height:1.35;margin-bottom:24px}nav a{display:block;color:white;text-decoration:none;font-weight:800;padding:10px 12px;border-radius:10px;margin:4px 0}nav a:hover{background:rgba(255,255,255,.16)}main{flex:1;padding:28px}.hero{background:linear-gradient(135deg,#0b5f2a,#174d2f);color:white;border-radius:18px;padding:24px;margin-bottom:20px}h1{margin:0 0 12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px}.card{background:white;border:1px solid #d9e0da;border-radius:16px;padding:18px;margin-bottom:16px;box-shadow:0 8px 25px rgba(15,23,42,.06)}.metric{font-size:28px;font-weight:900}.label{font-size:12px;text-transform:uppercase;color:#64748b;font-weight:900}input,select,textarea{width:100%;padding:12px;border:1px solid #cfd8d1;border-radius:10px;background:white}textarea{min-height:90px}button,.btn{background:#111827;color:white;border:none;border-radius:10px;padding:12px 18px;font-weight:900;text-decoration:none;display:inline-block;cursor:pointer}table{width:100%;border-collapse:collapse;background:white}th,td{text-align:left;border-bottom:1px solid #e5e7eb;padding:12px;font-size:14px}th{font-size:12px;text-transform:uppercase;color:#475569}.badge{display:inline-block;border-radius:999px;background:#e8f5ec;color:#0b5f2a;padding:5px 10px;font-weight:900;font-size:12px}.flash{background:#e8f5ec;border:1px solid #b9e2c6;color:#0b5f2a;border-radius:12px;padding:12px;margin-bottom:15px}.row{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:12px}
</style></head><body><div class="layout"><aside class="side"><div class="brand">Pinnacle<br>Performance Tax<br>and Accounting</div><div class="contact">{{ brand.website }}<br>{{ brand.email }}<br>{{ brand.phone }}</div><nav>
{% if current_user.is_authenticated and current_user.role == 'admin' %}
<a href="{{ url_for('dashboard') }}">Dashboard</a><a href="{{ url_for('clients') }}">Clients</a><a href="{{ url_for('transactions') }}">Bookkeeping</a><a href="{{ url_for('tax_returns') }}">Tax Returns</a><a href="{{ url_for('documents') }}">Documents</a><a href="{{ url_for('invoices') }}">Invoices</a><a href="{{ url_for('payments') }}">Payments</a><a href="{{ url_for('appointments') }}">Appointments</a><a href="{{ url_for('crm') }}">CRM</a><a href="{{ url_for('messages') }}">Messages</a><a href="{{ url_for('users') }}">Users</a><a href="{{ url_for('logout') }}">Sign Out</a>
{% elif current_user.is_authenticated %}
<a href="{{ url_for('client_dashboard') }}">Dashboard</a><a href="{{ url_for('my_invoices') }}">My Invoices</a><a href="{{ url_for('my_payments') }}">My Payments</a><a href="{{ url_for('my_appointments') }}">My Appointments</a><a href="{{ url_for('my_bookkeeping') }}">My Bookkeeping</a><a href="{{ url_for('my_tax_returns') }}">My Tax Returns</a><a href="{{ url_for('my_documents') }}">My Documents</a><a href="{{ url_for('my_messages') }}">My Messages</a><a href="{{ url_for('my_year_end') }}">My Year-End Summary</a><a href="{{ url_for('logout') }}">Sign Out</a>
{% endif %}
</nav></aside><main>{% with messages = get_flashed_messages(with_categories=true) %}{% for cat,msg in messages %}<div class="flash">{{ msg }}</div>{% endfor %}{% endwith %}{{ content|safe }}</main></div></body></html>
"""


def page(content, **ctx):
    return render_template_string(BASE_HTML, content=render_template_string(content, **ctx))


@app.route("/init")
def init_route():
    init_db()
    return f"Database initialized. Admin: {ADMIN_EMAIL} / {ADMIN_PASSWORD} | Client: client@example.com / client123"


@app.route("/reset-admin-2026")
def reset_admin_2026():
    init_db()
    return f"PPT admin reset complete. Login with {ADMIN_EMAIL} / {ADMIN_PASSWORD}"


@app.route("/")
def home():
    init_db()
    user = current_user()
    if not user.is_authenticated:
        return redirect(url_for("login"))
    return redirect(url_for("dashboard" if user.role == "admin" else "client_dashboard"))


@app.route("/login", methods=["GET", "POST"])
def login():
    init_db()
    error = ""
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        user = query_db("SELECT * FROM users WHERE lower(email)=? AND is_active=1", (email,), one=True)

        if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
            admin = query_db("SELECT * FROM users WHERE lower(email)=?", (ADMIN_EMAIL,), one=True)
            session["user_id"] = admin["id"]
            return redirect(url_for("dashboard"))

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            return redirect(url_for("dashboard" if user["role"] == "admin" else "client_dashboard"))

        error = "Invalid login."

    return render_template_string("""
    <html><head><style>
    body{font-family:Arial;background:#f6f8f6;display:flex;align-items:center;justify-content:center;min-height:100vh}.box{width:440px;background:white;border-radius:18px;padding:28px;box-shadow:0 20px 50px rgba(0,0,0,.12)}input{width:100%;padding:13px;margin:8px 0;border:1px solid #ccd5cf;border-radius:10px}button{width:100%;padding:13px;background:#0b5f2a;color:white;border:0;border-radius:10px;font-weight:900}
    </style></head><body><div class="box"><h1>PPT Portal Login</h1><p style="color:red">{{ error }}</p><form method="post"><input name="email" placeholder="Email" value="admin@pinnacleperformancetax.com"><input name="password" type="password" placeholder="Password"><button>Login</button></form><p>Admin: admin@pinnacleperformancetax.com / admin123<br>Client: client@example.com / client123</p></div></body></html>
    """, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
@admin_required
def dashboard():
    income = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", one=True)["total"]
    expenses = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", one=True)["total"]
    unpaid = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]
    clients_count = query_db("SELECT COUNT(*) total FROM clients", one=True)["total"]
    docs_count = query_db("SELECT COUNT(*) total FROM documents", one=True)["total"]
    returns_count = query_db("SELECT COUNT(*) total FROM tax_returns", one=True)["total"]
    messages_count = query_db("SELECT COUNT(*) total FROM messages WHERE status='Open'", one=True)["total"]
    docs = query_db("SELECT d.*, c.name client_name FROM documents d LEFT JOIN clients c ON c.id=d.client_id ORDER BY d.id DESC LIMIT 5")
    return page("""
    <div class="hero"><h1>PPT Executive Dashboard</h1><p>Pinnacle Performance Tax & Accounting</p></div><h1>Dashboard</h1>
    <div class="grid"><div class="card"><div class="label">Income</div><div class="metric">{{ income|money }}</div></div><div class="card"><div class="label">Expenses</div><div class="metric">{{ expenses|money }}</div></div><div class="card"><div class="label">Unpaid</div><div class="metric">{{ unpaid|money }}</div></div><div class="card"><div class="label">Clients</div><div class="metric">{{ clients_count }}</div></div><div class="card"><div class="label">Docs</div><div class="metric">{{ docs_count }}</div></div><div class="card"><div class="label">Returns</div><div class="metric">{{ returns_count }}</div></div><div class="card"><div class="label">Messages</div><div class="metric">{{ messages_count }}</div></div></div>
    <div class="card"><h2>Recent Documents</h2><table><tr><th>Client</th><th>Document</th><th>Status</th></tr>{% for d in docs %}<tr><td>{{ d.client_name }}</td><td>{{ d.document_name or d.name }}</td><td>{{ d.status }}</td></tr>{% endfor %}</table></div>
    """, income=income, expenses=expenses, unpaid=unpaid, clients_count=clients_count, docs_count=docs_count, returns_count=returns_count, messages_count=messages_count, docs=docs)


@app.route("/clients", methods=["GET", "POST"])
@login_required
@admin_required
def clients():
    if request.method == "POST":
        execute_db("INSERT INTO clients(name,business_name,email,phone,address,client_type,status,notes) VALUES (?,?,?,?,?,?,?,?)", (request.form.get("name"), request.form.get("business_name"), request.form.get("email"), request.form.get("phone"), request.form.get("address"), request.form.get("client_type"), request.form.get("status"), request.form.get("notes")))
        flash("Client saved.", "success")
        return redirect(url_for("clients"))
    rows = query_db("SELECT * FROM clients ORDER BY name")
    return page("<h1>Clients</h1><div class='card'><form method='post'><div class='row'><input name='name' placeholder='Client name'><input name='business_name' placeholder='Business'><input name='email' placeholder='Email'></div><div class='row'><input name='phone' placeholder='Phone'><input name='address' placeholder='Address'><select name='client_type'><option>Individual</option><option>Business</option></select></div><div class='row'><select name='status'><option>Active</option><option>Inactive</option></select><textarea name='notes' placeholder='Notes'></textarea><button>Save Client</button></div></form></div><div class='card'><table><tr><th>Name</th><th>Business</th><th>Email</th><th>Phone</th><th>Type</th></tr>{% for r in rows %}<tr><td>{{ r.name }}</td><td>{{ r.business_name }}</td><td>{{ r.email }}</td><td>{{ r.phone }}</td><td>{{ r.client_type }}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/transactions", methods=["GET", "POST"])
@app.route("/bookkeeping", methods=["GET", "POST"])
@login_required
@admin_required
def transactions():
    clients = query_db("SELECT * FROM clients ORDER BY name")
    if request.method == "POST":
        execute_db("INSERT INTO transactions(client_id,date,description,type,category,amount,notes) VALUES (?,?,?,?,?,?,?)", (request.form.get("client_id"), request.form.get("date"), request.form.get("description"), request.form.get("type"), request.form.get("category"), request.form.get("amount") or 0, request.form.get("notes")))
        flash("Transaction saved.", "success")
        return redirect(url_for("transactions"))
    rows = query_db("SELECT t.*, c.name client_name FROM transactions t LEFT JOIN clients c ON c.id=t.client_id ORDER BY t.date DESC, t.id DESC")
    return page("<h1>Bookkeeping</h1><div class='card'><form method='post'><div class='row'><input type='date' name='date'><input name='description' placeholder='Description'><select name='type'><option>income</option><option>expense</option></select></div><div class='row'><input name='category' placeholder='Category'><select name='client_id'>{% for c in clients %}<option value='{{c.id}}'>{{c.name}}</option>{% endfor %}</select><input name='amount' placeholder='Amount'></div><textarea name='notes' placeholder='Notes'></textarea><br><br><button>Save</button></form></div><div class='card'><table><tr><th>Date</th><th>Description</th><th>Type</th><th>Category</th><th>Client</th><th>Amount</th></tr>{% for r in rows %}<tr><td>{{r.date}}</td><td>{{r.description}}</td><td>{{r.type}}</td><td>{{r.category}}</td><td>{{r.client_name}}</td><td>{{r.amount|money}}</td></tr>{% endfor %}</table></div>", rows=rows, clients=clients)


@app.route("/tax-returns", methods=["GET", "POST"])
@app.route("/tax_returns", methods=["GET", "POST"])
@login_required
@admin_required
def tax_returns():
    clients = query_db("SELECT * FROM clients ORDER BY name")
    if request.method == "POST":
        execute_db("INSERT INTO tax_returns(client_id,tax_year,service,status,due_date,notes) VALUES (?,?,?,?,?,?)", (request.form.get("client_id"), request.form.get("tax_year"), request.form.get("service"), request.form.get("status"), request.form.get("due_date"), request.form.get("notes")))
        flash("Tax return saved.", "success")
        return redirect(url_for("tax_returns"))
    rows = query_db("SELECT tr.*, c.name client_name FROM tax_returns tr LEFT JOIN clients c ON c.id=tr.client_id ORDER BY tr.id DESC")
    return page("<h1>Tax Returns</h1><div class='card'><form method='post'><div class='row'><select name='client_id'>{% for c in clients %}<option value='{{c.id}}'>{{c.name}}</option>{% endfor %}</select><input name='tax_year' placeholder='Tax Year'><input name='service' placeholder='Service'></div><div class='row'><select name='status'><option>Waiting on Client</option><option>In Review</option><option>Ready to File</option><option>Filed</option></select><input type='date' name='due_date'><button>Save Return</button></div><textarea name='notes' placeholder='Notes'></textarea></form></div><div class='card'><table><tr><th>Client</th><th>Year</th><th>Service</th><th>Status</th><th>Due</th></tr>{% for r in rows %}<tr><td>{{r.client_name}}</td><td>{{r.tax_year}}</td><td>{{r.service}}</td><td>{{r.status}}</td><td>{{r.due_date}}</td></tr>{% endfor %}</table></div>", rows=rows, clients=clients)


@app.route("/documents", methods=["GET", "POST"])
@login_required
def documents():
    user = current_user()
    clients = query_db("SELECT * FROM clients ORDER BY name")
    if request.method == "POST":
        client_id = request.form.get("client_id") or user.client_id or 1
        document_name = request.form.get("document_name") or request.form.get("name") or "Document"
        category = request.form.get("category") or "Tax Documents"
        tax_year = request.form.get("tax_year") or ""
        status = request.form.get("status") or "Received"
        notes = request.form.get("notes") or ""
        uploaded_file = request.files.get("file")
        filename = ""
        original_filename = ""
        if uploaded_file and uploaded_file.filename and allowed_file(uploaded_file.filename):
            original_filename = secure_filename(uploaded_file.filename)
            filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{client_id}_{original_filename}"
            uploaded_file.save(UPLOAD_DIR / filename)
        execute_db("INSERT INTO documents(client_id,document_name,name,filename,original_filename,tax_year,category,status,notes,uploaded_by) VALUES (?,?,?,?,?,?,?,?,?,?)", (client_id, document_name, document_name, filename, original_filename, tax_year, category, status, notes, user.name))
        flash("Document saved.", "success")
        return redirect(url_for("documents" if user.role == "admin" else "my_documents"))
    rows = query_db("SELECT d.*, c.name client_name FROM documents d LEFT JOIN clients c ON c.id=d.client_id ORDER BY d.id DESC")
    return page("<h1>Document Center</h1><div class='card'><form method='post' enctype='multipart/form-data'><div class='row'><select name='client_id'>{% for c in clients %}<option value='{{c.id}}'>{{c.name}}</option>{% endfor %}</select><input name='document_name' placeholder='Document Name'><select name='category'><option>Tax Documents</option><option>Identification</option><option>Payroll</option><option>Receipts</option><option>Bank Statements</option></select></div><div class='row'><input name='tax_year' placeholder='Tax Year'><select name='status'><option>Received</option><option>Requested</option><option>Missing</option><option>Reviewed</option></select><input type='file' name='file'></div><textarea name='notes' placeholder='Notes'></textarea><br><br><button>Save Document</button></form></div><div class='card'><h2>All Client Documents</h2><table><tr><th>Client</th><th>Date</th><th>Document</th><th>Original File</th><th>Category</th><th>Year</th><th>Status</th><th>File</th></tr>{% for r in rows %}<tr><td>{{r.client_name}}</td><td>{{r.uploaded_at}}</td><td>{{r.document_name or r.name}}</td><td>{{r.original_filename}}</td><td>{{r.category}}</td><td>{{r.tax_year}}</td><td><span class='badge'>{{r.status}}</span></td><td>{% if r.filename %}<a href='{{ url_for('download_document', document_id=r.id) }}'>Download</a>{% else %}No file{% endif %}</td></tr>{% endfor %}</table></div>", rows=rows, clients=clients)


@app.route("/documents/download/<int:document_id>")
@login_required
def download_document(document_id):
    user = current_user()
    doc = query_db("SELECT * FROM documents WHERE id=?", (document_id,), one=True)
    if not doc or not doc["filename"]:
        abort(404)
    if user.role != "admin" and doc["client_id"] != user.client_id:
        abort(403)
    return send_from_directory(UPLOAD_DIR, doc["filename"], as_attachment=True)


@app.route("/invoices")
@login_required
@admin_required
def invoices():
    rows = query_db("SELECT i.*, c.name client_name FROM invoices i LEFT JOIN clients c ON c.id=i.client_id ORDER BY i.id DESC")
    return page("<h1>Invoices</h1><div class='card'><table><tr><th>Invoice</th><th>Client</th><th>Status</th><th>Amount</th><th>Due</th></tr>{% for r in rows %}<tr><td>{{r.invoice_number}}</td><td>{{r.client_name}}</td><td>{{r.status}}</td><td>{{r.amount|money}}</td><td>{{r.due_date}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/payments")
@login_required
@admin_required
def payments():
    rows = query_db("SELECT p.*, i.invoice_number, c.name client_name FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id LEFT JOIN clients c ON c.id=p.client_id ORDER BY p.id DESC")
    return page("<h1>Payments</h1><div class='card'><table><tr><th>Date</th><th>Invoice</th><th>Client</th><th>Method</th><th>Amount</th></tr>{% for r in rows %}<tr><td>{{r.date}}</td><td>{{r.invoice_number}}</td><td>{{r.client_name}}</td><td>{{r.method}}</td><td>{{r.amount|money}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/appointments")
@login_required
@admin_required
def appointments():
    rows = query_db("SELECT a.*, c.name client_name FROM appointments a LEFT JOIN clients c ON c.id=a.client_id ORDER BY a.start_at DESC")
    return page("<h1>Appointments</h1><div class='card'><table><tr><th>Client</th><th>Title</th><th>Start</th><th>Status</th></tr>{% for r in rows %}<tr><td>{{r.client_name}}</td><td>{{r.title}}</td><td>{{r.start_at}}</td><td>{{r.status}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/crm")
@login_required
@admin_required
def crm():
    rows = query_db("SELECT l.*, c.name client_name FROM crm_leads l LEFT JOIN clients c ON c.id=l.client_id ORDER BY l.id DESC")
    return page("<h1>CRM</h1><div class='card'><table><tr><th>Client</th><th>Name</th><th>Status</th><th>Notes</th></tr>{% for r in rows %}<tr><td>{{r.client_name}}</td><td>{{r.name}}</td><td>{{r.status}}</td><td>{{r.notes}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/messages", methods=["GET", "POST"])
@login_required
@admin_required
def messages():
    clients = query_db("SELECT * FROM clients ORDER BY name")
    if request.method == "POST":
        execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,?)", (request.form.get("client_id"), "admin", "PPT Office", request.form.get("subject"), request.form.get("body"), "Open"))
        flash("Message sent.", "success")
        return redirect(url_for("messages"))
    rows = query_db("SELECT m.*, c.name client_name FROM messages m LEFT JOIN clients c ON c.id=m.client_id ORDER BY m.id DESC")
    return page("<h1>Messages</h1><div class='card'><h2>Send Client Message</h2><form method='post'><div class='row'><select name='client_id'>{% for c in clients %}<option value='{{c.id}}'>{{c.name}}</option>{% endfor %}</select><input name='subject' placeholder='Subject'></div><textarea name='body' placeholder='Message'></textarea><br><br><button>Send Message</button></form></div><div class='card'><h2>Message History</h2><table><tr><th>Date</th><th>Client</th><th>From</th><th>Subject</th><th>Message</th><th>Status</th></tr>{% for r in rows %}<tr><td>{{r.created_at}}</td><td>{{r.client_name}}</td><td>{{r.sender_name}}</td><td>{{r.subject}}</td><td>{{r.body}}</td><td><span class='badge'>{{r.status}}</span></td></tr>{% endfor %}</table></div>", rows=rows, clients=clients)


@app.route("/users")
@login_required
@admin_required
def users():
    rows = query_db("SELECT * FROM users ORDER BY id DESC")
    return page("<h1>Users</h1><div class='card'><table><tr><th>Name</th><th>Email</th><th>Role</th><th>Active</th></tr>{% for r in rows %}<tr><td>{{r.name}}</td><td>{{r.email}}</td><td>{{r.role}}</td><td>{{r.is_active}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/client")
@app.route("/client-dashboard")
@app.route("/client_dashboard")
@login_required
@client_required
def client_dashboard():
    user = current_user()
    return page("<h1>Client Dashboard</h1><p>Welcome, {{ current_user.name }}.</p><div class='card'><p>Use the sidebar to view documents, tax returns, messages, invoices, and bookkeeping.</p></div>")


@app.route("/my/documents", methods=["GET", "POST"])
@app.route("/my-documents", methods=["GET", "POST"])
@login_required
@client_required
def my_documents():
    return documents()


@app.route("/my/tax-returns")
@app.route("/my-tax-returns")
@login_required
@client_required
def my_tax_returns():
    user = current_user()
    rows = query_db("SELECT * FROM tax_returns WHERE client_id=? ORDER BY id DESC", (user.client_id,))
    return page("<h1>My Tax Returns</h1><div class='card'><table><tr><th>Year</th><th>Service</th><th>Status</th><th>Due</th></tr>{% for r in rows %}<tr><td>{{r.tax_year}}</td><td>{{r.service}}</td><td>{{r.status}}</td><td>{{r.due_date}}</td></tr>{% endfor %}</table></div><div class='card'><form method='post' action='{{ url_for('my_tax_return_question') }}'><textarea name='body' placeholder='Ask a tax return question'></textarea><br><br><button>Send Question</button></form></div>", rows=rows)


@app.route("/my/tax-return-question", methods=["POST"])
@login_required
@client_required
def my_tax_return_question():
    user = current_user()
    execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,?)", (user.client_id, "client", user.name, "Tax Return Question", request.form.get("body") or "", "Open"))
    flash("Tax return question sent to the office.", "success")
    return redirect(url_for("my_tax_returns"))


@app.route("/my/messages", methods=["GET", "POST"])
@app.route("/my-messages", methods=["GET", "POST"])
@login_required
@client_required
def my_messages():
    user = current_user()
    if request.method == "POST":
        execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,?)", (user.client_id, "client", user.name, request.form.get("subject") or "Client Message", request.form.get("body"), "Open"))
        flash("Message sent to the office.", "success")
        return redirect(url_for("my_messages"))
    rows = query_db("SELECT * FROM messages WHERE client_id=? ORDER BY id DESC", (user.client_id,))
    return page("<h1>My Messages</h1><div class='card'><form method='post'><input name='subject' placeholder='Subject'><textarea name='body' placeholder='Message'></textarea><br><br><button>Send Message</button></form></div><div class='card'><table><tr><th>Date</th><th>From</th><th>Subject</th><th>Message</th><th>Status</th></tr>{% for r in rows %}<tr><td>{{r.created_at}}</td><td>{{r.sender_name}}</td><td>{{r.subject}}</td><td>{{r.body}}</td><td>{{r.status}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/my/invoices")
@app.route("/my-invoices")
@login_required
@client_required
def my_invoices():
    rows = query_db("SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC", (current_user().client_id,))
    return page("<h1>My Invoices</h1><div class='card'><table><tr><th>Invoice</th><th>Status</th><th>Amount</th><th>Due</th></tr>{% for r in rows %}<tr><td>{{r.invoice_number}}</td><td>{{r.status}}</td><td>{{r.amount|money}}</td><td>{{r.due_date}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/my/payments")
@app.route("/my-payments")
@login_required
@client_required
def my_payments():
    rows = query_db("SELECT p.*, i.invoice_number FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id WHERE p.client_id=? ORDER BY p.id DESC", (current_user().client_id,))
    return page("<h1>My Payments</h1><div class='card'><table><tr><th>Date</th><th>Invoice</th><th>Method</th><th>Amount</th></tr>{% for r in rows %}<tr><td>{{r.date}}</td><td>{{r.invoice_number}}</td><td>{{r.method}}</td><td>{{r.amount|money}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/my/appointments", methods=["GET", "POST"])
@app.route("/my-appointments", methods=["GET", "POST"])
@login_required
@client_required
def my_appointments():
    rows = query_db("SELECT * FROM appointments WHERE client_id=? ORDER BY id DESC", (current_user().client_id,))
    return page("<h1>My Appointments</h1><div class='card'><table><tr><th>Title</th><th>Start</th><th>Status</th></tr>{% for r in rows %}<tr><td>{{r.title}}</td><td>{{r.start_at}}</td><td>{{r.status}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/my/bookkeeping")
@app.route("/my-bookkeeping")
@login_required
@client_required
def my_bookkeeping():
    user = current_user()
    rows = query_db("SELECT * FROM transactions WHERE client_id=? ORDER BY date DESC, id DESC", (user.client_id,))
    return page("<h1>My Bookkeeping</h1><div class='card'><table><tr><th>Date</th><th>Description</th><th>Type</th><th>Category</th><th>Amount</th></tr>{% for r in rows %}<tr><td>{{r.date}}</td><td>{{r.description}}</td><td>{{r.type}}</td><td>{{r.category}}</td><td>{{r.amount|money}}</td></tr>{% endfor %}</table></div>", rows=rows)


@app.route("/my/year-end")
@app.route("/my-year-end-summary")
@login_required
@client_required
def my_year_end():
    return page("<h1>My Year-End Summary</h1><div class='card'><p>Year-end summary center loaded.</p></div>")


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
