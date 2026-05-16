from __future__ import annotations
import os, sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from flask import Flask, abort, flash, g, redirect, render_template, render_template_string, request, send_from_directory, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
INSTANCE_DIR.mkdir(exist_ok=True); UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = INSTANCE_DIR / "ppt_portal.db"
ALLOWED_UPLOADS = {"pdf","png","jpg","jpeg","doc","docx","xls","xlsx","csv","txt"}
BRAND = {"business_name":"Pinnacle Performance Tax and Accounting","website":"www.pinnacleperformancetax.com","email":"pinnacleperformancetax@gmail.com","phone":"478-338-1632"}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "ppt-dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024
login_manager = LoginManager(app); login_manager.login_view = "login"

def get_db():
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        g.db = conn
    return g.db

@app.teardown_appcontext
def close_db(error=None):
    conn = g.pop("db", None)
    if conn: conn.close()

def query_db(sql,args=(),one=False):
    rows = get_db().execute(sql,args).fetchall()
    return (rows[0] if rows else None) if one else rows

def execute_db(sql,args=()):
    cur = get_db().execute(sql,args); get_db().commit(); return cur.lastrowid

def money(v):
    try: return round(float(str(v or "0").replace("$","").replace(",","")),2)
    except Exception: return 0.0

@app.template_filter("currency")
def currency(v): return "${:,.2f}".format(money(v))

@app.context_processor
def inject_globals(): return {"brand":BRAND}

class User(UserMixin):
    def __init__(self,row):
        self.id=str(row["id"]); self.name=row["name"]; self.email=row["email"]; self.role=row["role"]; self.client_id=row["client_id"]

@login_manager.user_loader
def load_user(user_id):
    row=query_db("SELECT * FROM users WHERE id=? AND is_active=1",(user_id,),one=True)
    return User(row) if row else None

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args,**kwargs):
        if not current_user.is_authenticated: return redirect(url_for("login"))
        if current_user.role != "admin":
            flash("Admin access required.","danger"); return redirect(url_for("client_dashboard"))
        return fn(*args,**kwargs)
    return wrapper

def client_required(fn):
    @wraps(fn)
    def wrapper(*args,**kwargs):
        if not current_user.is_authenticated: return redirect(url_for("login"))
        if current_user.role == "admin": return redirect(url_for("dashboard"))
        if not current_user.client_id:
            flash("Your login is not linked to a client profile.","danger"); return redirect(url_for("client_dashboard"))
        return fn(*args,**kwargs)
    return wrapper

def add_column_if_missing(table,column,definition):
    cols=[r["name"] for r in get_db().execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        get_db().execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}"); get_db().commit()

def allowed_file(filename): return "." in filename and filename.rsplit(".",1)[1].lower() in ALLOWED_UPLOADS

def init_db():
    db=get_db()
    db.executescript('''
    CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,business_name TEXT,email TEXT,phone TEXT,address TEXT,client_type TEXT DEFAULT 'Individual',status TEXT DEFAULT 'Active',notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,role TEXT DEFAULT 'client',client_id INTEGER,is_active INTEGER DEFAULT 1,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,kind TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT,date TEXT NOT NULL,description TEXT NOT NULL,type TEXT NOT NULL,category_id INTEGER,client_id INTEGER,amount REAL NOT NULL,notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS invoices (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,invoice_number TEXT,issue_date TEXT,due_date TEXT,amount REAL DEFAULT 0,status TEXT DEFAULT 'Draft',description TEXT,paid_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS payments (id INTEGER PRIMARY KEY AUTOINCREMENT,invoice_id INTEGER,client_id INTEGER,amount REAL DEFAULT 0,method TEXT DEFAULT 'Manual Entry',reference TEXT,status TEXT DEFAULT 'Paid',notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,document_name TEXT NOT NULL DEFAULT 'Document',name TEXT,filename TEXT,tax_year TEXT,status TEXT DEFAULT 'Received',notes TEXT,category TEXT DEFAULT 'Tax Documents',uploaded_by TEXT DEFAULT 'Admin',reviewed_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS tax_returns (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,tax_year TEXT,service_type TEXT,status TEXT DEFAULT 'In Progress',due_date TEXT,fee REAL DEFAULT 0,notes TEXT,invoice_id INTEGER,completed_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS appointments (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,title TEXT,start_at TEXT,end_at TEXT,location TEXT,meeting_link TEXT,status TEXT DEFAULT 'Scheduled',notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS crm_leads (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,phone TEXT,email TEXT,status TEXT DEFAULT 'New',source TEXT,follow_up_date TEXT,notes TEXT,client_id INTEGER,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,sender_role TEXT,sender_name TEXT,subject TEXT,body TEXT,status TEXT DEFAULT 'Open',created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    ''')
    for table,column,definition in [("users","client_id","INTEGER"),("clients","business_name","TEXT"),("clients","email","TEXT"),("clients","phone","TEXT"),("clients","address","TEXT"),("clients","client_type","TEXT DEFAULT 'Individual'"),("clients","status","TEXT DEFAULT 'Active'"),("clients","notes","TEXT"),("invoices","paid_at","TEXT"),("payments","method","TEXT DEFAULT 'Manual Entry'"),("payments","reference","TEXT"),("payments","client_id","INTEGER"),("tax_returns","invoice_id","INTEGER"),("tax_returns","completed_at","TEXT"),("documents","document_name","TEXT DEFAULT 'Document'"),("documents","name","TEXT"),("documents","filename","TEXT"),("documents","tax_year","TEXT"),("documents","status","TEXT DEFAULT 'Received'"),("documents","notes","TEXT"),("documents","category","TEXT DEFAULT 'Tax Documents'"),("documents","uploaded_by","TEXT DEFAULT 'Admin'"),("documents","reviewed_at","TEXT"),("crm_leads","client_id","INTEGER"),("messages","status","TEXT DEFAULT 'Open'")]: add_column_if_missing(table,column,definition)
    cats=[("Tax Preparation Income","income"),("Bookkeeping Income","income"),("Consulting Income","income"),("Sales Income","income"),("Office Supplies","expense"),("Software & Subscriptions","expense"),("Advertising & Marketing","expense"),("Meals","expense"),("Travel","expense"),("Payroll","expense"),("Contract Labor","expense"),("Bank Fees","expense"),("Professional Fees","expense"),("Vehicle & Mileage","expense"),("Rent","expense"),("Utilities","expense"),("Insurance","expense"),("Other Expense","expense")]
    for name,kind in cats:
        if not db.execute("SELECT id FROM categories WHERE LOWER(TRIM(name))=LOWER(TRIM(?)) AND kind=?",(name,kind)).fetchone(): db.execute("INSERT INTO categories(name,kind) VALUES (?,?)",(name,kind))
    db.commit()
    for d in db.execute("SELECT LOWER(TRIM(name)) clean,kind,MIN(id) keep_id,COUNT(*) c FROM categories GROUP BY LOWER(TRIM(name)),kind HAVING COUNT(*)>1").fetchall():
        for r in db.execute("SELECT id FROM categories WHERE LOWER(TRIM(name))=? AND kind=? AND id<>?",(d["clean"],d["kind"],d["keep_id"])).fetchall():
            db.execute("UPDATE transactions SET category_id=? WHERE category_id=?",(d["keep_id"],r["id"])); db.execute("DELETE FROM categories WHERE id=?",(r["id"],))
    db.commit()
    admin_email="admin@pinnacleperformancetax.com"; admin_pw=os.environ.get("ADMIN_PASSWORD","ChangeMe123")
    if db.execute("SELECT id FROM users WHERE lower(email)=?",(admin_email,)).fetchone(): db.execute("UPDATE users SET name=?,password_hash=?,role='admin',is_active=1 WHERE lower(email)=?",("PPT Admin",generate_password_hash(admin_pw),admin_email))
    else: db.execute("INSERT INTO users(name,email,password_hash,role,is_active) VALUES (?,?,?,'admin',1)",("PPT Admin",admin_email,generate_password_hash(admin_pw)))
    if db.execute("SELECT COUNT(*) c FROM clients").fetchone()["c"]==0:
        cid=db.execute("INSERT INTO clients(name,business_name,email,phone,client_type,status,notes) VALUES (?,?,?,?,?,?,?)",("Sample Client","Sample Business LLC","client@example.com","478-555-0110","Full Service","Active","Demo client")).lastrowid
        db.execute("INSERT OR IGNORE INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,'client',?,1)",("Sample Client","client@example.com",generate_password_hash("Temp123!"),cid))
    db.commit()

LOGIN_PAGE_HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>PPT Portal Login</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,#123d22,#0f172a);font-family:Arial,Helvetica,sans-serif}
.card{width:100%;max-width:440px;background:white;border-radius:24px;padding:34px;box-shadow:0 24px 70px rgba(0,0,0,.35)}
h1{margin:0 0 8px;color:#123d22;font-size:30px;line-height:1.05}
p{color:#475569}
label{display:block;font-size:13px;font-weight:800;margin:14px 0 6px}
input{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:12px;padding:13px}
button{width:100%;margin-top:18px;border:0;border-radius:12px;padding:14px;background:#123d22;color:white;font-weight:900}
.flash{padding:12px 14px;border-radius:14px;background:#fef2f2;border:1px solid #fecaca;margin:12px 0;color:#991b1b}
.small{font-size:12px;color:#64748b;margin-top:14px}
</style>
</head>
<body>
<div class="card">
<h1>Pinnacle<br>Performance Tax<br>Portal</h1>
<p>Secure client and admin login</p>
{% with messages = get_flashed_messages(with_categories=true) %}
  {% for cat,msg in messages %}
    <div class="flash">{{ msg }}</div>
  {% endfor %}
{% endwith %}
<form method="POST" action="/login">
<label>Email</label>
<input type="email" name="email" placeholder="Email" required autofocus>
<label>Password</label>
<input type="password" name="password" placeholder="Password" required>
<button type="submit">Sign In</button>
</form>
<div class="small">Pinnacle Performance Tax and Accounting</div>
</div>
</body>
</html>
"""


def ensure_messages_table():
    db = get_db()
    db.execute('''CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        sender_role TEXT DEFAULT 'admin',
        sender_name TEXT,
        subject TEXT,
        body TEXT,
        status TEXT DEFAULT 'Open',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    db.commit()


def ensure_client_template_columns():
    fields = [
        ("tax_year", "TEXT"),
        ("service_package", "TEXT"),
        ("entity_type", "TEXT"),
        ("ein", "TEXT"),
        ("ssn_last4", "TEXT"),
        ("dob", "TEXT"),
        ("occupation", "TEXT"),
        ("spouse_name", "TEXT"),
        ("filing_status", "TEXT"),
        ("preferred_contact", "TEXT"),
        ("onboarding_status", "TEXT DEFAULT 'New'"),
    ]
    for column, definition in fields:
        add_column_if_missing("clients", column, definition)


# === PPT WORKFLOW STABILITY UPGRADE V1 START ===
def ensure_workflow_tables():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS document_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            title TEXT NOT NULL,
            category TEXT DEFAULT 'Tax Documents',
            tax_year TEXT,
            status TEXT DEFAULT 'Requested',
            due_date TEXT,
            notes TEXT,
            requested_by TEXT DEFAULT 'Admin',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            activity_type TEXT,
            title TEXT,
            details TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    # Safe column checks for existing tables
    try:
        add_column_if_missing("tax_returns", "workflow_stage", "TEXT DEFAULT 'Pending'")
    except Exception:
        pass
    try:
        add_column_if_missing("appointments", "admin_decision", "TEXT")
    except Exception:
        pass
    try:
        add_column_if_missing("invoices", "payment_badge", "TEXT")
    except Exception:
        pass

def log_activity(client_id, activity_type, title, details=""):
    ensure_workflow_tables()
    execute_db(
        "INSERT INTO activity_logs(client_id,activity_type,title,details) VALUES (?,?,?,?)",
        (client_id, activity_type, title, details),
    )
# === PPT WORKFLOW STABILITY UPGRADE V1 END ===


# === PPT ELITE OPERATIONS SUITE V2.2 START ===
def ensure_elite_operations_tables():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS intake_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            template_type TEXT DEFAULT 'Client Intake',
            content TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS client_timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            event_type TEXT,
            title TEXT,
            details TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS internal_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            note TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS automation_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subject TEXT,
            body TEXT,
            template_type TEXT DEFAULT 'Email',
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()

    for col, definition in [
        ("workflow_status", "TEXT DEFAULT 'Lead'"),
        ("priority", "TEXT DEFAULT 'Standard'"),
        ("assigned_preparer", "TEXT"),
        ("lead_source", "TEXT"),
        ("follow_up_date", "TEXT"),
        ("service_type", "TEXT"),
        ("conversion_status", "TEXT DEFAULT 'Open'")
    ]:
        try:
            add_column_if_missing("clients", col, definition)
        except Exception:
            pass

    for col, definition in [
        ("priority", "TEXT DEFAULT 'Normal'"),
        ("follow_up_date", "TEXT"),
        ("lead_source", "TEXT"),
        ("conversion_status", "TEXT DEFAULT 'Open'")
    ]:
        try:
            add_column_if_missing("crm_leads", col, definition)
        except Exception:
            pass

def elite_log(client_id, event_type, title, details=""):
    ensure_elite_operations_tables()
    execute_db(
        "INSERT INTO client_timeline(client_id,event_type,title,details) VALUES (?,?,?,?)",
        (client_id, event_type, title, details),
    )
# === PPT ELITE OPERATIONS SUITE V2.2 END ===

@app.route('/init')
def init_route(): init_db(); ensure_elite_operations_tables(); ensure_workflow_tables(); ensure_client_template_columns(); ensure_messages_table(); return 'INIT COMPLETE - client modules repaired and categories deduped'
@app.route('/')
def home(): return redirect(url_for('login')) if not current_user.is_authenticated else redirect(url_for('dashboard') if current_user.role=='admin' else url_for('client_dashboard'))


@app.route('/login-test')
def login_test():
    return '<h1 style="font-family:Arial;color:green;">PPT LOGIN TEST VISIBLE</h1><p>If you see this, browser display works.</p>'


@app.route('/login', methods=['GET', 'POST'])
def login():
    init_db()
    if current_user.is_authenticated:
        return redirect(url_for('dashboard') if current_user.role == 'admin' else url_for('client_dashboard'))

    error = ""
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        row = query_db("SELECT * FROM users WHERE lower(email)=? AND is_active=1", (email,), one=True)
        if row and check_password_hash(row['password_hash'], password):
            login_user(User(row))
            return redirect(url_for('dashboard') if row['role'] == 'admin' else url_for('client_dashboard'))
        error = "<div style='background:#fef2f2;border:1px solid #fecaca;color:#991b1b;padding:12px;margin:12px 0;border-radius:12px;font-weight:bold;'>Invalid login.</div>"

    return f"""<!doctype html>
<html>
<head><meta charset="utf-8"><title>PPT Portal Login</title><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;background:#f4f7f4;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
<div style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;background:linear-gradient(135deg,#f8fafc 0%,#e8f5ec 55%,#ffffff 100%);">
<div style="width:100%;max-width:460px;background:white;padding:34px;border-radius:24px;border:1px solid #dfe7df;box-shadow:0 22px 60px rgba(15,23,42,.13);">
<div style="color:#123d22;font-weight:900;font-size:30px;line-height:1.05;margin-bottom:8px;">Pinnacle<br>Performance Tax<br>Portal</div>
<div style="height:5px;background:#123d22;border-radius:999px;width:120px;margin:14px 0 18px;"></div>
<p style="font-size:15px;color:#475569;margin-bottom:18px;">Secure Admin and Client Login</p>
{error}
<form method="POST" action="/login">
<label style="display:block;font-weight:800;margin:14px 0 6px;">Email</label>
<input style="width:100%;padding:13px;font-size:16px;border:1px solid #cbd5e1;border-radius:12px;box-sizing:border-box;background:#fff;" type="email" name="email" required autofocus>
<label style="display:block;font-weight:800;margin:14px 0 6px;">Password</label>
<input style="width:100%;padding:13px;font-size:16px;border:1px solid #cbd5e1;border-radius:12px;box-sizing:border-box;background:#fff;" type="password" name="password" required>
<button style="width:100%;margin-top:20px;background:#123d22;color:white;padding:14px;font-size:16px;font-weight:900;border:0;border-radius:12px;" type="submit">SIGN IN</button>
</form>
<p style="font-size:12px;color:#64748b;margin-top:18px;">Pinnacle Performance Tax and Accounting</p>
</div></div></body></html>"""

@app.route('/messages', methods=['GET', 'POST'])
@login_required
@admin_required
def messages():
    ensure_messages_table()
    clients = query_db("SELECT id,name,email FROM clients ORDER BY name")
    if request.method == 'POST':
        client_id = request.form.get('client_id') or None
        subject = request.form.get('subject') or 'Message from Pinnacle Performance Tax'
        body = request.form.get('body') or ''
        execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
                   (client_id, 'admin', current_user.name, subject, body))
        flash('Message sent.', 'success')
        return redirect(url_for('messages'))
    items = query_db("""SELECT m.*, c.name client_name
                        FROM messages m LEFT JOIN clients c ON c.id=m.client_id
                        ORDER BY m.id DESC LIMIT 100""")
    return render_template('messages.html', messages=items, clients=clients)

@app.route('/my/messages', methods=['GET', 'POST'])
@login_required
@client_required
def my_messages():
    ensure_messages_table()
    if request.method == 'POST':
        subject = request.form.get('subject') or 'Client Message'
        body = request.form.get('body') or ''
        execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
                   (current_user.client_id, 'client', current_user.name, subject, body))
        flash('Message sent to the office.', 'success')
        return redirect(url_for('my_messages'))
    items = query_db("SELECT * FROM messages WHERE client_id=? OR client_id IS NULL ORDER BY id DESC LIMIT 100", (current_user.client_id,))
    return render_template('my_messages.html', messages=items)


# === PPT INVOICE STATEMENT UPGRADE START ===
def ppt_client_money_totals(client_id):
    billed_row = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE client_id=?", (client_id,), one=True)
    paid_row = query_db("SELECT COALESCE(SUM(amount),0) total FROM payments WHERE client_id=?", (client_id,), one=True)
    billed = money(billed_row["total"] if billed_row else 0)
    paid = money(paid_row["total"] if paid_row else 0)
    return {"billed": billed, "paid": paid, "balance": billed - paid}

@app.route('/invoice/<int:invoice_id>/print')
@login_required
def invoice_print(invoice_id):
    invoice = query_db("""SELECT i.*, c.name client_name, c.business_name, c.email client_email, c.phone client_phone, c.address client_address
                          FROM invoices i LEFT JOIN clients c ON c.id=i.client_id
                          WHERE i.id=?""", (invoice_id,), one=True)
    if not invoice:
        abort(404)
    if current_user.role != 'admin' and invoice['client_id'] != current_user.client_id:
        abort(403)
    payments = query_db("SELECT * FROM payments WHERE invoice_id=? ORDER BY id DESC", (invoice_id,))
    paid = sum([money(p["amount"]) for p in payments])
    balance = money(invoice["amount"]) - paid
    return render_template("invoice_print.html", invoice=invoice, payments=payments, paid=paid, balance=balance)

@app.route('/client-statement/<int:client_id>')
@login_required
@admin_required
def client_statement(client_id):
    client = query_db("SELECT * FROM clients WHERE id=?", (client_id,), one=True)
    if not client:
        abort(404)
    invoices = query_db("SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC", (client_id,))
    payments = query_db("""SELECT p.*, i.invoice_number
                           FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id
                           WHERE p.client_id=? ORDER BY p.id DESC""", (client_id,))
    totals = ppt_client_money_totals(client_id)
    return render_template("client_statement.html", client=client, invoices=invoices, payments=payments, totals=totals)

@app.route('/my/statement')
@login_required
@client_required
def my_statement():
    client = query_db("SELECT * FROM clients WHERE id=?", (current_user.client_id,), one=True)
    if not client:
        flash("Client profile not linked.", "danger")
        return redirect(url_for("client_dashboard"))
    invoices = query_db("SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC", (current_user.client_id,))
    payments = query_db("""SELECT p.*, i.invoice_number
                           FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id
                           WHERE p.client_id=? ORDER BY p.id DESC""", (current_user.client_id,))
    totals = ppt_client_money_totals(current_user.client_id)
    return render_template("client_statement.html", client=client, invoices=invoices, payments=payments, totals=totals)
# === PPT INVOICE STATEMENT UPGRADE END ===


# === PPT WORKFLOW STABILITY UPGRADE V1 ROUTES START ===
@app.route('/workflow')
@login_required
@admin_required
def workflow_dashboard():
    ensure_workflow_tables()
    missing_docs = query_db("""SELECT dr.*, c.name client_name
                               FROM document_requests dr
                               LEFT JOIN clients c ON c.id=dr.client_id
                               WHERE dr.status!='Completed'
                               ORDER BY dr.id DESC LIMIT 25""")
    open_invoices = query_db("""SELECT i.*, c.name client_name
                                FROM invoices i
                                LEFT JOIN clients c ON c.id=i.client_id
                                WHERE i.status!='Paid'
                                ORDER BY i.id DESC LIMIT 25""")
    requested_appointments = query_db("""SELECT a.*, c.name client_name
                                         FROM appointments a
                                         LEFT JOIN clients c ON c.id=a.client_id
                                         WHERE a.status='Requested'
                                         ORDER BY a.id DESC LIMIT 25""")
    open_returns = query_db("""SELECT tr.*, c.name client_name
                               FROM tax_returns tr
                               LEFT JOIN clients c ON c.id=tr.client_id
                               WHERE tr.status NOT IN ('Completed','Filed')
                               ORDER BY tr.id DESC LIMIT 25""")
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>Workflow Hub</h1><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin-bottom:20px"><div class="metric"><span>Missing Docs</span><strong style="color:#ef4444">{{missing_docs|length}}</strong></div><div class="metric"><span>Open Invoices</span><strong style="color:#f59e0b">{{open_invoices|length}}</strong></div><div class="metric"><span>Appt Requests</span><strong style="color:#0891b2">{{requested_appointments|length}}</strong></div><div class="metric"><span>Open Returns</span><strong style="color:#11823b">{{open_returns|length}}</strong></div></div><div style="display:grid;grid-template-columns:1fr 1fr;gap:20px"><div class="card"><h2 style="margin-top:0">Missing Documents</h2>{%if missing_docs%}<div class="table-wrap"><table><thead><tr><th>Client</th><th>Document</th><th>Due</th><th></th></tr></thead><tbody>{%for d in missing_docs%}<tr><td>{{d.client_name or"--"}}</td><td><strong>{{d.title}}</strong></td><td style="font-size:12px">{{d.due_date or"--"}}</td><td><form method="POST" action="/document-requests/{{d.id}}/complete"><button style="padding:4px 8px;font-size:11px;background:#e8f5ec;color:#123d22;border:0;border-radius:8px;cursor:pointer">Done</button></form></td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:16px">None</p>{%endif%}<div style="margin-top:12px"><a href="/document-requests" class="btn" style="font-size:12px;padding:8px 12px">+ Request Doc</a></div></div><div class="card"><h2 style="margin-top:0">Open Invoices</h2>{%if open_invoices%}<div class="table-wrap"><table><thead><tr><th>Client</th><th>Invoice</th><th>Amount</th><th>Status</th></tr></thead><tbody>{%for i in open_invoices%}<tr><td>{{i.client_name or"--"}}</td><td style="font-size:12px">{{i.invoice_number or"--"}}</td><td style="font-weight:900">${{"%.2f"|format(i.amount|float)}}</td><td><span class="pill{%if i.status=="Overdue"%} warn{%endif%}">{{i.status}}</span></td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:16px">None</p>{%endif%}</div><div class="card"><h2 style="margin-top:0">Appointment Requests</h2>{%if requested_appointments%}<div class="table-wrap"><table><thead><tr><th>Client</th><th>Title</th><th>When</th><th></th></tr></thead><tbody>{%for a in requested_appointments%}<tr><td>{{a.client_name or"--"}}</td><td>{{a.title or"Appt"}}</td><td style="font-size:12px">{{a.start_at or"--"}}</td><td><form method="POST" action="/appointments/{{a.id}}/approve" style="display:inline"><button style="padding:4px 8px;font-size:11px;background:#e8f5ec;color:#123d22;border:0;border-radius:8px;cursor:pointer">OK</button></form><form method="POST" action="/appointments/{{a.id}}/decline" style="display:inline;margin-left:4px"><button style="padding:4px 8px;font-size:11px;background:#fef2f2;color:#b91c1c;border:0;border-radius:8px;cursor:pointer">No</button></form></td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:16px">None</p>{%endif%}</div><div class="card"><h2 style="margin-top:0">Tax Returns In Progress</h2>{%if open_returns%}<div class="table-wrap"><table><thead><tr><th>Client</th><th>Year</th><th>Status</th><th>Due</th></tr></thead><tbody>{%for r in open_returns%}<tr><td>{{r.client_name or"--"}}</td><td>{{r.tax_year}}</td><td><span class="pill warn">{{r.status}}</span></td><td style="font-size:12px">{{r.due_date or"--"}}</td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:16px">None</p>{%endif%}</div></div><div class="card"><h2 style="margin-top:0">Quick Actions</h2><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px"><a class="btn" href="/service-entry">Quick Entry</a><a class="btn" href="/bookkeeping/csv-import">CSV Import</a><a class="btn" href="/bookkeeping/recurring">Recurring</a><a class="btn" href="/bookkeeping/rules">Auto Rules</a><a class="btn btn-dark" href="/notifications/list">Notifications</a></div></div>{%endblock%}""", missing_docs=missing_docs, open_invoices=open_invoices, requested_appointments=requested_appointments, open_returns=open_returns)

@app.route('/document-requests', methods=['GET', 'POST'])
@login_required
@admin_required
def document_requests():
    ensure_workflow_tables()
    if request.method == 'POST':
        client_id = request.form.get('client_id')
        title = request.form.get('title') or 'Requested Document'
        category = request.form.get('category') or 'Tax Documents'
        tax_year = request.form.get('tax_year') or ''
        due_date = request.form.get('due_date') or ''
        notes = request.form.get('notes') or ''
        execute_db(
            "INSERT INTO document_requests(client_id,title,category,tax_year,due_date,notes,requested_by) VALUES (?,?,?,?,?,?,?)",
            (client_id, title, category, tax_year, due_date, notes, current_user.name),
        )
        log_activity(client_id, "Document Request", title, notes)
        flash("Document request created.", "success")
        return redirect(url_for("document_requests"))
    clients = query_db("SELECT id,name,email FROM clients ORDER BY name")
    requests = query_db("""SELECT dr.*, c.name client_name
                           FROM document_requests dr
                           LEFT JOIN clients c ON c.id=dr.client_id
                           ORDER BY dr.id DESC LIMIT 200""")
    return render_template("document_requests.html", clients=clients, requests=requests)

@app.route('/document-requests/<int:req_id>/complete', methods=['POST'])
@login_required
def complete_document_request(req_id):
    ensure_workflow_tables()
    req = query_db("SELECT * FROM document_requests WHERE id=?", (req_id,), one=True)
    if not req:
        abort(404)
    if current_user.role != 'admin' and req['client_id'] != current_user.client_id:
        abort(403)
    execute_db("UPDATE document_requests SET status='Completed', completed_at=CURRENT_TIMESTAMP WHERE id=?", (req_id,))
    log_activity(req["client_id"], "Document Request Completed", req["title"], "Marked complete")
    flash("Document request marked complete.", "success")
    return redirect(url_for("document_requests") if current_user.role == "admin" else url_for("my_document_requests"))

@app.route('/my/document-requests')
@login_required
@client_required
def my_document_requests():
    ensure_workflow_tables()
    requests = query_db("SELECT * FROM document_requests WHERE client_id=? ORDER BY id DESC", (current_user.client_id,))
    return render_template("my_document_requests.html", requests=requests)

@app.route('/tax-tracker', methods=['GET', 'POST'])
@login_required
@admin_required
def tax_tracker():
    ensure_workflow_tables()
    if request.method == 'POST':
        return_id = request.form.get('return_id')
        stage = request.form.get('workflow_stage') or request.form.get('status') or 'Pending'
        execute_db("UPDATE tax_returns SET workflow_stage=?, status=? WHERE id=?", (stage, stage, return_id))
        row = query_db("SELECT client_id,tax_year FROM tax_returns WHERE id=?", (return_id,), one=True)
        if row:
            log_activity(row["client_id"], "Tax Return Stage", f"Tax return moved to {stage}", f"Tax year {row['tax_year']}")
        flash("Tax return stage updated.", "success")
        return redirect(url_for("tax_tracker"))
    returns = query_db("""SELECT tr.*, c.name client_name
                          FROM tax_returns tr
                          LEFT JOIN clients c ON c.id=tr.client_id
                          ORDER BY tr.tax_year DESC, tr.id DESC LIMIT 300""")
    return render_template("tax_tracker.html", returns=returns)

@app.route('/tax-tracker/<int:return_id>/stage', methods=['POST'])
@login_required
@admin_required
def tax_tracker_stage(return_id):
    ensure_workflow_tables()
    stage = request.form.get('workflow_stage') or 'Pending'
    execute_db("UPDATE tax_returns SET workflow_stage=?, status=? WHERE id=?", (stage, stage, return_id))
    row = query_db("SELECT client_id,tax_year FROM tax_returns WHERE id=?", (return_id,), one=True)
    if row:
        log_activity(row["client_id"], "Tax Return Stage", f"Tax return moved to {stage}", f"Tax year {row['tax_year']}")
    flash("Tax return stage updated.", "success")
    return redirect(url_for("tax_tracker"))

@app.route('/appointments/review')
@login_required
@admin_required
def appointments_review():
    ensure_workflow_tables()
    appointments = query_db("""SELECT a.*, c.name client_name
                               FROM appointments a
                               LEFT JOIN clients c ON c.id=a.client_id
                               ORDER BY a.id DESC LIMIT 200""")
    return render_template("appointments_review.html", appointments=appointments)

@app.route('/appointments/<int:appointment_id>/approve', methods=['POST'])
@login_required
@admin_required
def approve_appointment(appointment_id):
    ensure_workflow_tables()
    appt = query_db("SELECT * FROM appointments WHERE id=?", (appointment_id,), one=True)
    if not appt:
        abort(404)
    execute_db("UPDATE appointments SET status='Approved', admin_decision='Approved' WHERE id=?", (appointment_id,))
    log_activity(appt["client_id"], "Appointment Approved", appt["title"] or "Appointment", appt["start_at"] or "")
    flash("Appointment approved.", "success")
    return redirect(url_for("appointments_review"))

@app.route('/appointments/<int:appointment_id>/decline', methods=['POST'])
@login_required
@admin_required
def decline_appointment(appointment_id):
    ensure_workflow_tables()
    appt = query_db("SELECT * FROM appointments WHERE id=?", (appointment_id,), one=True)
    if not appt:
        abort(404)
    execute_db("UPDATE appointments SET status='Declined', admin_decision='Declined' WHERE id=?", (appointment_id,))
    log_activity(appt["client_id"], "Appointment Declined", appt["title"] or "Appointment", appt["start_at"] or "")
    flash("Appointment declined.", "success")
    return redirect(url_for("appointments_review"))

@app.route('/activity-log')
@login_required
@admin_required
def activity_log():
    ensure_workflow_tables()
    logs = query_db("""SELECT al.*, c.name client_name
                       FROM activity_logs al
                       LEFT JOIN clients c ON c.id=al.client_id
                       ORDER BY al.id DESC LIMIT 300""")
    return render_template("activity_log.html", logs=logs)
# === PPT WORKFLOW STABILITY UPGRADE V1 ROUTES END ===


# === PPT ELITE OPERATIONS SUITE V2.2 ROUTES START ===
@app.route('/elite-dashboard')
@login_required
@admin_required
def elite_dashboard():
    ensure_elite_operations_tables()
    status_rows = query_db("SELECT COALESCE(workflow_status,'Lead') status, COUNT(*) count FROM clients GROUP BY COALESCE(workflow_status,'Lead')")
    priority_rows = query_db("SELECT COALESCE(priority,'Standard') priority, COUNT(*) count FROM clients GROUP BY COALESCE(priority,'Standard')")
    revenue = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices", one=True)
    payments_total = query_db("SELECT COALESCE(SUM(amount),0) total FROM payments", one=True)
    open_invoices = query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'", one=True)
    recent_timeline = query_db("""SELECT t.*, c.name client_name FROM client_timeline t
                                  LEFT JOIN clients c ON c.id=t.client_id
                                  ORDER BY t.id DESC LIMIT 20""")
    return render_template("elite_dashboard.html",
        status_rows=status_rows, priority_rows=priority_rows,
        revenue=revenue, payments_total=payments_total,
        open_invoices=open_invoices, recent_timeline=recent_timeline)

@app.route('/client-workflow')
@login_required
@admin_required
def client_workflow():
    ensure_elite_operations_tables()
    clients = query_db("SELECT * FROM clients ORDER BY COALESCE(workflow_status,'Lead'), name")
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>Client Workflow</h1><p class="sub">Track every client through the pipeline.</p><div class="card"><div class="table-wrap"><table><thead><tr><th>Client</th><th>Type</th><th>Workflow</th><th>Priority</th><th>Follow-up</th><th>Actions</th></tr></thead><tbody>{%for c in clients%}<tr><td><strong>{{c.name}}</strong>{%if c.business_name%}<br><span style="font-size:11px;color:#475569">{{c.business_name}}</span>{%endif%}</td><td style="font-size:12px">{{c.client_type or"--"}}</td><td><span class="pill warn">{{c.workflow_status or"Lead"}}</span></td><td style="font-size:12px">{{c.priority or"Standard"}}</td><td style="font-size:12px;color:#475569">{{c.follow_up_date or"--"}}</td><td><form method="POST" action="/clients/{{c.id}}/status" style="display:flex;gap:4px;flex-wrap:wrap"><select name="workflow_status" style="padding:4px 6px;font-size:11px;border-radius:8px">{%for s in["Lead","Consultation","Active","Bookkeeping","Planning","Retention","Inactive"]%}<option value="{{s}}"{%if(c.workflow_status or"Lead")==s%} selected{%endif%}>{{s}}</option>{%endfor%}</select><select name="priority" style="padding:4px 6px;font-size:11px;border-radius:8px">{%for p in["Standard","High","Urgent"]%}<option value="{{p}}"{%if(c.priority or"Standard")==p%} selected{%endif%}>{{p}}</option>{%endfor%}</select><button style="padding:4px 8px;font-size:11px;border-radius:8px">Save</button></form></td></tr>{%else%}<tr><td colspan="6" style="text-align:center;padding:20px;color:#475569">No clients.</td></tr>{%endfor%}</tbody></table></div></div>{%endblock%}""", clients=clients)

@app.route('/clients/<int:client_id>/status', methods=['POST'])
@login_required
@admin_required
def update_client_status(client_id):
    ensure_elite_operations_tables()
    status = request.form.get("workflow_status") or "Lead"
    priority = request.form.get("priority") or "Standard"
    assigned_preparer = request.form.get("assigned_preparer") or ""
    follow_up_date = request.form.get("follow_up_date") or ""
    execute_db("UPDATE clients SET workflow_status=?, priority=?, assigned_preparer=?, follow_up_date=? WHERE id=?",
               (status, priority, assigned_preparer, follow_up_date, client_id))
    elite_log(client_id, "Status Update", f"Client moved to {status}", f"Priority: {priority}")
    flash("Client workflow updated.", "success")
    return redirect(url_for("client_workflow"))

@app.route('/clients/<int:client_id>/timeline')
@login_required
@admin_required
def client_timeline_view(client_id):
    ensure_elite_operations_tables()
    client = query_db("SELECT * FROM clients WHERE id=?", (client_id,), one=True)
    if not client:
        abort(404)
    timeline = query_db("SELECT * FROM client_timeline WHERE client_id=? ORDER BY id DESC", (client_id,))
    notes = query_db("SELECT * FROM internal_notes WHERE client_id=? ORDER BY id DESC", (client_id,))
    return render_template("client_timeline.html", client=client, timeline=timeline, notes=notes)

@app.route('/clients/<int:client_id>/notes', methods=['POST'])
@login_required
@admin_required
def add_client_note(client_id):
    ensure_elite_operations_tables()
    note = request.form.get("note") or ""
    execute_db("INSERT INTO internal_notes(client_id,note,created_by) VALUES (?,?,?)", (client_id, note, current_user.name))
    elite_log(client_id, "Internal Note", "Staff note added", note)
    flash("Internal note saved.", "success")
    return redirect(url_for("client_timeline_view", client_id=client_id))

@app.route('/template-center', methods=['GET', 'POST'])
@login_required
@admin_required
def template_center():
    ensure_elite_operations_tables()
    if request.method == 'POST':
        execute_db("INSERT INTO intake_templates(name,template_type,content,is_active) VALUES (?,?,?,1)",
                   (request.form.get("name"), request.form.get("template_type"), request.form.get("content")))
        flash("Template saved.", "success")
        return redirect(url_for("template_center"))
    templates = query_db("SELECT * FROM intake_templates ORDER BY id DESC")
    return render_template("template_center.html", templates=templates)

@app.route('/template-center/<int:template_id>/toggle', methods=['POST'])
@login_required
@admin_required
def toggle_template(template_id):
    ensure_elite_operations_tables()
    row = query_db("SELECT is_active FROM intake_templates WHERE id=?", (template_id,), one=True)
    if row:
        execute_db("UPDATE intake_templates SET is_active=? WHERE id=?", (0 if row["is_active"] else 1, template_id))
    flash("Template updated.", "success")
    return redirect(url_for("template_center"))

@app.route('/automation-templates', methods=['GET', 'POST'])
@login_required
@admin_required
def automation_templates():
    ensure_elite_operations_tables()
    if request.method == 'POST':
        execute_db("INSERT INTO automation_templates(name,subject,body,template_type,is_active) VALUES (?,?,?,?,1)",
                   (request.form.get("name"), request.form.get("subject"), request.form.get("body"), request.form.get("template_type")))
        flash("Automation template saved.", "success")
        return redirect(url_for("automation_templates"))
    templates = query_db("SELECT * FROM automation_templates ORDER BY id DESC")
    return render_template("automation_templates.html", templates=templates)

@app.route('/crm-upgrade')
@login_required
@admin_required
def crm_upgrade():
    ensure_elite_operations_tables()
    leads = query_db("SELECT * FROM crm_leads ORDER BY id DESC LIMIT 300")
    return render_template("crm_upgrade.html", leads=leads)
# === PPT ELITE OPERATIONS SUITE V2.2 ROUTES END ===


# === PPT CLIENT CRM FIX START ===
@app.route('/my/crm', methods=['GET', 'POST'])
@login_required
@client_required
def my_crm():
    if request.method == 'POST':
        topic = request.form.get('topic') or 'Client Request'
        notes = request.form.get('notes') or ''
        try:
            execute_db(
                "INSERT INTO crm_leads(name,email,status,source,notes,client_id) VALUES (?,?,?,?,?,?)",
                (current_user.name, current_user.email, 'Client Request', 'Client Portal', f"{topic}: {notes}", current_user.client_id)
            )
        except Exception:
            pass
        try:
            ensure_messages_table()
            execute_db(
                "INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
                (current_user.client_id, 'client', current_user.name, topic, notes)
            )
        except Exception:
            pass
        flash("Your request was sent to the office.", "success")
        return redirect(url_for("my_crm"))
    try:
        requests = query_db("SELECT * FROM crm_leads WHERE client_id=? ORDER BY id DESC LIMIT 100", (current_user.client_id,))
    except Exception:
        requests = []
    return render_template("my_crm.html", requests=requests)
# === PPT CLIENT CRM FIX END ===


@app.route('/ppt-green-theme.css')
def ppt_green_theme_css():
    return '''html,body{background:#f4f6f4!important;color:#1f2937!important}
aside,.sidebar,[class*="sidebar"]{background:linear-gradient(180deg,#134f2c 0%,#0f3d22 100%)!important;color:#fff!important;border-right:0!important}
aside *, .sidebar *, [class*="sidebar"] *{color:#fff!important}
button,.btn,input[type="submit"]{background:#134f2c!important;color:#fff!important;border:0!important}
button:hover,.btn:hover{background:#1f6f3d!important}
.card,.metric,.box,.panel,section{background:#fff!important;border-color:#d8e2dc!important}
.metric strong,.amount,.total{color:#1f6f3d!important}''', 200, {'Content-Type': 'text/css'}


@app.route('/ppt-ui-v3.css')
def ppt_ui_v3_css():
    css = """
html,body{background:#f6f8f6!important;color:#1f2937!important;font-family:Arial,Helvetica,sans-serif!important}
.layout{background:linear-gradient(135deg,#f8faf8 0%,#eef7f1 100%)!important}
aside,.sidebar,[class*="sidebar"]{background:linear-gradient(180deg,#11823b 0%,#0b5f2a 100%)!important;color:#fff!important;border-right:0!important;box-shadow:10px 0 30px rgba(17,130,59,.18)!important}
aside *, .sidebar *, [class*="sidebar"] *{color:#fff!important}
aside a,.sidebar a,[class*="sidebar"] a{color:#fff!important;border-radius:12px!important;font-weight:900!important}
aside a:hover,.sidebar a:hover,[class*="sidebar"] a:hover{background:rgba(255,255,255,.18)!important}
.card,.metric,.box,.panel,section{background:#fff!important;border:1px solid #d8e2dc!important;border-radius:22px!important;box-shadow:0 12px 30px rgba(15,23,42,.06)!important}
.metric strong,.amount,.total{color:#11823b!important}
button,.btn,input[type="submit"]{background:#11823b!important;color:#fff!important;border:0!important;border-radius:13px!important;font-weight:900!important}
button:hover,.btn:hover,input[type="submit"]:hover{background:#0b5f2a!important}
.btn-dark{background:#0b5f2a!important}.btn-light{background:#e8f5ec!important;color:#0b5f2a!important}
input,select,textarea{border:1px solid #cbd5d1!important;background:white!important;border-radius:13px!important}
input:focus,select:focus,textarea:focus{outline:3px solid rgba(17,130,59,.16)!important;border-color:#11823b!important}
.pill,.badge{background:#e8f5ec!important;color:#0b5f2a!important;border-radius:999px!important;font-weight:900!important}
.badge-paid,.status-paid{background:#e8f5ec!important;color:#0b5f2a!important}.badge-pending,.status-pending{background:#fff7ed!important;color:#b7791f!important}.badge-overdue,.status-overdue{background:#fef2f2!important;color:#b91c1c!important}
.table-wrap{border-radius:18px!important;border:1px solid #d8e2dc!important;overflow:auto!important;background:#fff!important}
table{background:#fff!important}th{background:#f7faf8!important;color:#475569!important}
.searchbar{display:flex;gap:10px;margin:12px 0 18px}.searchbar input{max-width:420px}
.upload-zone{border:2px dashed #11823b!important;background:#f1faf4!important;border-radius:22px!important;padding:26px!important;text-align:center!important;color:#0b5f2a!important;font-weight:900!important}
"""
    return css, 200, {'Content-Type':'text/css'}

@app.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('login'))
@app.route('/dashboard')
@login_required
@admin_required
def dashboard():
    counts={k:query_db(v,one=True)['c'] for k,v in {'clients':'SELECT COUNT(*) c FROM clients','open_invoices':"SELECT COUNT(*) c FROM invoices WHERE status!='Paid'",'documents':'SELECT COUNT(*) c FROM documents','returns':'SELECT COUNT(*) c FROM tax_returns','messages':"SELECT COUNT(*) c FROM messages WHERE status='Open'"}.items()}
    return render_template('dashboard.html',income=query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'",one=True)['total'],expenses=query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'",one=True)['total'],unpaid=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'",one=True)['total'],counts=counts,recent_documents=query_db("SELECT d.*,COALESCE(d.document_name,d.name,'Document') display_name,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC LIMIT 8"),open_messages=query_db("SELECT m.*,cl.name client_name FROM messages m LEFT JOIN clients cl ON cl.id=m.client_id WHERE m.status='Open' ORDER BY m.id DESC LIMIT 5"))
@app.route('/client-dashboard')
@app.route('/client')
@login_required
def client_dashboard():
    if current_user.role=='admin': return redirect(url_for('dashboard'))
    if not current_user.client_id: return render_template('client_dashboard.html',client=None,invoices=[],payments=[],appointments=[],documents=[],tax_returns=[],transactions=[],crm_items=[],messages=[])
    cid=current_user.client_id
    return render_template('client_dashboard.html',client=query_db('SELECT * FROM clients WHERE id=?',(cid,),one=True),invoices=query_db('SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC',(cid,)),payments=query_db('SELECT p.*,i.invoice_number FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id WHERE p.client_id=? ORDER BY p.id DESC',(cid,)),appointments=query_db('SELECT * FROM appointments WHERE client_id=? ORDER BY id DESC',(cid,)),documents=query_db("SELECT *,COALESCE(document_name,name,'Document') display_name FROM documents WHERE client_id=? ORDER BY id DESC",(cid,)),tax_returns=query_db('SELECT tr.*,i.invoice_number FROM tax_returns tr LEFT JOIN invoices i ON i.id=tr.invoice_id WHERE tr.client_id=? ORDER BY tr.id DESC',(cid,)),transactions=query_db('SELECT t.*,c.name category_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? ORDER BY t.date DESC,t.id DESC',(cid,)),crm_items=query_db('SELECT * FROM crm_leads WHERE client_id=? ORDER BY id DESC',(cid,)),messages=query_db('SELECT * FROM messages WHERE client_id=? ORDER BY id DESC',(cid,)))
@app.route('/client/upload',methods=['POST'])
@login_required
@client_required
def client_upload():
    f=request.files.get('file')
    if not f or not f.filename or not allowed_file(f.filename): flash('Choose a valid file.','danger'); return redirect(url_for('client_dashboard'))
    filename=f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{current_user.client_id}_{secure_filename(f.filename)}"; f.save(UPLOAD_DIR/filename); doc=request.form.get('document_name') or f.filename
    execute_db("INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by) VALUES (?,?,?,?,?,'Uploaded by Client',?,?, 'Client')",(current_user.client_id,doc,doc,filename,request.form.get('tax_year'),request.form.get('notes'),request.form.get('category') or 'Tax Documents'))
    flash('Document uploaded.','success'); return redirect(url_for('client_dashboard'))
@app.route('/documents/download/<int:document_id>')
@login_required
def download_document(document_id):
    doc=query_db('SELECT * FROM documents WHERE id=?',(document_id,),one=True)
    if not doc or not doc['filename']: abort(404)
    if current_user.role!='admin' and doc['client_id']!=current_user.client_id: abort(403)
    return send_from_directory(UPLOAD_DIR,doc['filename'],as_attachment=True)

def admin_table_route(table_name, template_name, select_sql, insert_sql=None, redirect_name=None): pass
@app.route('/clients',methods=['GET','POST'])
@login_required
@admin_required
def clients():
    if request.method=='POST': execute_db('INSERT INTO clients(name,business_name,email,phone,address,client_type,status,notes) VALUES (?,?,?,?,?,?,?,?)',(request.form.get('name'),request.form.get('business_name'),request.form.get('email'),request.form.get('phone'),request.form.get('address'),request.form.get('client_type'),request.form.get('status'),request.form.get('notes'))); return redirect(url_for('clients'))
    return render_template('clients.html',clients=query_db('SELECT * FROM clients ORDER BY name'))

@app.route('/clients/<int:client_id>/edit')
@login_required
@admin_required
def edit_client(client_id):
    ensure_client_template_columns()
    client = query_db("SELECT * FROM clients WHERE id=?", (client_id,), one=True)
    if not client:
        abort(404)
    return render_template("client_edit.html", client=client)

@app.route('/clients/<int:client_id>/update', methods=['POST'])
@login_required
@admin_required
def update_client(client_id):
    ensure_client_template_columns()
    fields = [
        "name", "business_name", "email", "phone", "address", "client_type", "status", "notes",
        "tax_year", "service_package", "entity_type", "ein", "ssn_last4", "dob", "occupation",
        "spouse_name", "filing_status", "preferred_contact", "onboarding_status"
    ]
    values = [request.form.get(f) for f in fields]
    set_clause = ",".join([f"{f}=?" for f in fields])
    execute_db(f"UPDATE clients SET {set_clause} WHERE id=?", tuple(values + [client_id]))
    flash("Client template updated successfully.", "success")
    return redirect(url_for("clients"))

@app.route('/transactions',methods=['GET','POST'])
@login_required
@admin_required
def transactions():
    if request.method=='POST': execute_db('INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)',(request.form.get('date'),request.form.get('description'),request.form.get('type'),request.form.get('category_id') or None,request.form.get('client_id') or None,money(request.form.get('amount')),request.form.get('notes'))); return redirect(url_for('transactions'))
    return render_template('transactions.html',transactions=query_db('SELECT t.*,c.name category_name,cl.name client_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id LEFT JOIN clients cl ON cl.id=t.client_id ORDER BY t.id DESC'),categories=query_db('SELECT * FROM categories ORDER BY kind,name'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/invoices',methods=['GET','POST'])
@login_required
@admin_required
def invoices():
    if request.method=='POST':
        import time
        inv_num = request.form.get('invoice_number') or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}-{int(time.time()*1000)%10000}"
        execute_db('INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,?,?)',(request.form.get('client_id'),inv_num,request.form.get('issue_date'),request.form.get('due_date'),money(request.form.get('amount')),request.form.get('status'),request.form.get('description'))); return redirect(url_for('invoices'))
    return render_template('invoices.html',invoices=query_db('SELECT i.*,cl.name client_name FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id ORDER BY i.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/payments',methods=['GET','POST'])
@login_required
@admin_required
def payments():
    if request.method=='POST':
        inv=query_db('SELECT * FROM invoices WHERE id=?',(request.form.get('invoice_id'),),one=True)
        if inv:
            execute_db("INSERT INTO payments(invoice_id,client_id,amount,method,reference,status,notes) VALUES (?,?,?,?,?,'Paid',?)",(inv['id'],inv['client_id'],money(request.form.get('amount')) or money(inv['amount']),request.form.get('method'),request.form.get('reference'),request.form.get('notes'))); execute_db("UPDATE invoices SET status='Paid',paid_at=CURRENT_TIMESTAMP WHERE id=?",(inv['id'],))
        return redirect(url_for('payments'))
    return render_template('payments.html',invoices=query_db('SELECT i.*,cl.name client_name FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id ORDER BY i.id DESC'),payments=query_db('SELECT p.*,i.invoice_number,cl.name client_name FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id LEFT JOIN clients cl ON cl.id=p.client_id ORDER BY p.id DESC'))
@app.route('/appointments',methods=['GET','POST'])
@login_required
@admin_required
def appointments():
    if request.method=='POST': execute_db('INSERT INTO appointments(client_id,title,start_at,end_at,location,meeting_link,status,notes) VALUES (?,?,?,?,?,?,?,?)',(request.form.get('client_id'),request.form.get('title'),request.form.get('start_at'),request.form.get('end_at'),request.form.get('location'),request.form.get('meeting_link'),request.form.get('status'),request.form.get('notes'))); return redirect(url_for('appointments'))
    return render_template('appointments.html',appointments=query_db('SELECT a.*,cl.name client_name FROM appointments a LEFT JOIN clients cl ON cl.id=a.client_id ORDER BY a.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/tax-returns',methods=['GET','POST'])
@app.route('/tax_returns',methods=['GET','POST'])
@login_required
@admin_required
def tax_returns():
    if request.method=='POST':
        inv=execute_db("INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,'Sent',?)",(request.form.get('client_id'),f"TR-{request.form.get('tax_year')}-{datetime.now().strftime('%H%M%S')}",datetime.now().strftime('%Y-%m-%d'),request.form.get('due_date'),money(request.form.get('fee')),'Tax return service'))
        execute_db('INSERT INTO tax_returns(client_id,tax_year,service_type,status,due_date,fee,notes,invoice_id) VALUES (?,?,?,?,?,?,?,?)',(request.form.get('client_id'),request.form.get('tax_year'),request.form.get('service_type'),request.form.get('status'),request.form.get('due_date'),money(request.form.get('fee')),request.form.get('notes'),inv)); return redirect(url_for('tax_returns'))
    return render_template('tax_returns.html',returns=query_db('SELECT tr.*,cl.name client_name,i.invoice_number FROM tax_returns tr LEFT JOIN clients cl ON cl.id=tr.client_id LEFT JOIN invoices i ON i.id=tr.invoice_id ORDER BY tr.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/crm',methods=['GET','POST'])
@login_required
@admin_required
def crm():
    if request.method=='POST': execute_db('INSERT INTO crm_leads(name,phone,email,status,source,follow_up_date,notes,client_id) VALUES (?,?,?,?,?,?,?,?)',(request.form.get('name'),request.form.get('phone'),request.form.get('email'),request.form.get('status'),request.form.get('source'),request.form.get('follow_up_date'),request.form.get('notes'),request.form.get('client_id') or None)); return redirect(url_for('crm'))
    return render_template('crm.html',leads=query_db('SELECT l.*,c.name client_name FROM crm_leads l LEFT JOIN clients c ON c.id=l.client_id ORDER BY l.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'),today=datetime.now().strftime('%Y-%m-%d'))
@app.route('/documents',methods=['GET','POST'])
@login_required
def documents():
    if current_user.role!='admin': return render_template('documents.html',documents=query_db("SELECT *,COALESCE(document_name,name,'Document') display_name FROM documents WHERE client_id=? ORDER BY id DESC",(current_user.client_id,)),clients=[])
    return render_template('documents.html',documents=query_db("SELECT d.*,COALESCE(d.document_name,d.name,'Document') display_name,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC"),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/settings',methods=['GET','POST'])
@login_required
@admin_required
def settings(): return render_template('settings.html',users=query_db('SELECT u.*,cl.name client_name FROM users u LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
# === PPT MY MESSAGES + YEAR END FIX START ===


@app.route("/my/year-end")
@login_required
@client_required
def my_year_end():
    year = request.args.get("year") or str(datetime.now().year)

    income = query_db(
        "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='income' AND substr(date,1,4)=?",
        (current_user.client_id, year),
        one=True,
    )["total"]

    expenses = query_db(
        "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='expense' AND substr(date,1,4)=?",
        (current_user.client_id, year),
        one=True,
    )["total"]

    by_category = query_db(
        """
        SELECT COALESCE(c.name,'Uncategorized') category,
               t.type,
               COALESCE(SUM(t.amount),0) total
        FROM transactions t
        LEFT JOIN categories c ON c.id=t.category_id
        WHERE t.client_id=? AND substr(t.date,1,4)=?
        GROUP BY COALESCE(c.name,'Uncategorized'), t.type
        ORDER BY t.type, total DESC
        """,
        (current_user.client_id, year),
    )

    transactions = query_db(
        """
        SELECT t.*, c.name category_name
        FROM transactions t
        LEFT JOIN categories c ON c.id=t.category_id
        WHERE t.client_id=? AND substr(t.date,1,4)=?
        ORDER BY t.date DESC, t.id DESC
        """,
        (current_user.client_id, year),
    )

    documents = query_db(
        """
        SELECT *, COALESCE(document_name,name,'Document') display_name
        FROM documents
        WHERE client_id=? AND (tax_year=? OR substr(created_at,1,4)=?)
        ORDER BY id DESC
        """,
        (current_user.client_id, year, year),
    )

    invoices = query_db(
        """
        SELECT *
        FROM invoices
        WHERE client_id=? AND (substr(issue_date,1,4)=? OR substr(created_at,1,4)=?)
        ORDER BY id DESC
        """,
        (current_user.client_id, year, year),
    )

    returns = query_db(
        """
        SELECT tr.*, i.invoice_number
        FROM tax_returns tr
        LEFT JOIN invoices i ON i.id=tr.invoice_id
        WHERE tr.client_id=? AND tr.tax_year=?
        ORDER BY tr.id DESC
        """,
        (current_user.client_id, year),
    )

    return render_template(
        "my_year_end.html",
        year=year,
        income=income,
        expenses=expenses,
        profit=money(income) - money(expenses),
        by_category=by_category,
        transactions=transactions,
        documents=documents,
        invoices=invoices,
        returns=returns,
    )


@app.route("/messages/reply/<int:message_id>", methods=["POST"])
@login_required
@admin_required
def reply_message(message_id):
    original = query_db("SELECT * FROM messages WHERE id=?", (message_id,), one=True)
    if not original:
        abort(404)
    execute_db(
        "INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
        (
            original["client_id"],
            "admin",
            current_user.name,
            "RE: " + (original["subject"] or "Client Message"),
            request.form.get("body") or "",
        ),
    )
    flash("Reply added to client portal.", "success")
    return redirect(url_for("messages"))


@app.route("/messages/close/<int:message_id>", methods=["POST"])
@login_required
@admin_required
def close_message(message_id):
    execute_db("UPDATE messages SET status='Closed' WHERE id=?", (message_id,))
    flash("Message closed.", "success")
    return redirect(url_for("messages"))

# === PPT MY MESSAGES + YEAR END FIX END ===


def document_file_exists(doc):
    try:
        if not doc or not doc["filename"]:
            return False
        return (UPLOAD_DIR / doc["filename"]).exists()
    except Exception:
        return False


def sync_missing_document_flags():
    rows = query_db("SELECT id, filename FROM documents")
    for d in rows:
        missing = 1 if d["filename"] and not (UPLOAD_DIR / d["filename"]).exists() else 0
        execute_db("UPDATE documents SET file_missing=? WHERE id=?", (missing, d["id"]))



@app.route("/documents/sync-missing")
@login_required
@admin_required
def sync_documents_missing_route():
    sync_missing_document_flags()
    flash("Document file check complete. Missing files are now flagged.", "success")
    return redirect(url_for("documents"))


@app.route("/documents/storage-status")
@login_required
@admin_required
def document_storage_status():
    total = query_db("SELECT COUNT(*) c FROM documents", one=True)["c"]
    with_files = query_db("SELECT COUNT(*) c FROM documents WHERE filename IS NOT NULL AND filename != ''", one=True)["c"]
    missing = 0
    for d in query_db("SELECT filename FROM documents WHERE filename IS NOT NULL AND filename != ''"):
        if not (UPLOAD_DIR / d["filename"]).exists():
            missing += 1
    return render_template("storage_status.html", upload_dir=str(UPLOAD_DIR), total=total, with_files=with_files, missing=missing)


# === PPT CLIENT SIDE FULL MODULE REPAIR START ===

@app.route("/my/invoices")
@login_required
@client_required
def my_invoices():
    invoices = query_db(
        """
        SELECT *
        FROM invoices
        WHERE client_id=?
        ORDER BY
          CASE WHEN status='Paid' THEN 1 ELSE 0 END,
          due_date DESC,
          id DESC
        """,
        (current_user.client_id,),
    )
    payments = query_db(
        """
        SELECT p.*, i.invoice_number
        FROM payments p
        LEFT JOIN invoices i ON i.id=p.invoice_id
        WHERE p.client_id=?
        ORDER BY p.id DESC
        """,
        (current_user.client_id,),
    )
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>My Invoices</h1><p class="sub">View and pay your invoices.</p>{%if invoices%}<div class="table-wrap"><table><thead><tr><th>Invoice</th><th>Description</th><th>Amount</th><th>Due</th><th>Status</th><th></th></tr></thead><tbody>{%for i in invoices%}<tr><td><strong>{{i.invoice_number or"--"}}</strong></td><td style="font-size:12px">{{i.description or"--"}}</td><td style="font-weight:900">${{"%.2f"|format(i.amount|float)}}</td><td style="font-size:12px">{{i.due_date or"--"}}</td><td><span class="pill{%if i.status=="Overdue"%} warn{%endif%}">{{i.status}}</span></td><td>{%if i.status!="Paid"%}<a href="/invoice/{{i.id}}/pay" class="btn" style="padding:6px 12px;font-size:13px">Pay Now</a>{%else%}<span style="color:#11823b;font-size:13px;font-weight:900">Paid</span>{%endif%}</td></tr>{%endfor%}</tbody></table></div>{%else%}<div class="card"><p style="color:#475569;text-align:center;padding:20px">No invoices yet.</p></div>{%endif%}{%if payments%}<div class="card" style="margin-top:20px"><h2 style="margin-top:0">Payment History</h2><div class="table-wrap"><table><thead><tr><th>Invoice</th><th>Amount</th><th>Method</th><th>Date</th></tr></thead><tbody>{%for p in payments%}<tr><td>{{p.invoice_number or"--"}}</td><td style="font-weight:900;color:#11823b">${{"%.2f"|format(p.amount|float)}}</td><td style="font-size:12px">{{p.method or"--"}}</td><td style="font-size:12px;color:#475569">{{p.created_at[:10]if p.created_at else"--"}}</td></tr>{%endfor%}</tbody></table></div></div>{%endif%}{%endblock%}""", invoices=invoices, payments=payments)


@app.route("/my/payments")
@login_required
@client_required
def my_payments():
    payments = query_db(
        """
        SELECT p.*, i.invoice_number, i.description invoice_description
        FROM payments p
        LEFT JOIN invoices i ON i.id=p.invoice_id
        WHERE p.client_id=?
        ORDER BY p.id DESC
        """,
        (current_user.client_id,),
    )
    open_invoices = query_db(
        "SELECT * FROM invoices WHERE client_id=? AND status!='Paid' ORDER BY due_date DESC,id DESC",
        (current_user.client_id,),
    )
    return render_template("my_payments.html", payments=payments, open_invoices=open_invoices)


@app.route("/my/appointments")
@login_required
@client_required
def my_appointments():
    appointments = query_db(
        """
        SELECT *
        FROM appointments
        WHERE client_id=?
        ORDER BY start_at DESC, id DESC
        """,
        (current_user.client_id,),
    )
    return render_template("my_appointments.html", appointments=appointments)


@app.route("/my/appointment-request", methods=["POST"])
@login_required
@client_required
def my_appointment_request():
    title = request.form.get("title") or "Client Appointment Request"
    preferred_date = request.form.get("preferred_date") or ""
    preferred_time = request.form.get("preferred_time") or ""
    notes = request.form.get("notes") or ""
    start_at = (preferred_date + " " + preferred_time).strip()
    execute_db(
        """
        INSERT INTO appointments(client_id,title,start_at,end_at,location,meeting_link,status,notes)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            current_user.client_id,
            title,
            start_at,
            "",
            request.form.get("location") or "To be confirmed",
            "",
            "Requested",
            notes,
        ),
    )
    execute_db(
        "INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
        (
            current_user.client_id,
            "client",
            current_user.name,
            "Appointment Request",
            f"Preferred: {start_at}. Notes: {notes}",
        ),
    )
    flash("Appointment request sent to the office.", "success")
    return redirect(url_for("my_appointments"))


@app.route("/my/bookkeeping")
@login_required
@client_required
def my_bookkeeping():
    year = request.args.get("year") or str(datetime.now().year)
    transactions = query_db(
        """
        SELECT t.*, c.name category_name
        FROM transactions t
        LEFT JOIN categories c ON c.id=t.category_id
        WHERE t.client_id=? AND substr(t.date,1,4)=?
        ORDER BY t.date DESC, t.id DESC
        """,
        (current_user.client_id, year),
    )
    income = query_db(
        "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='income' AND substr(date,1,4)=?",
        (current_user.client_id, year),
        one=True,
    )["total"]
    expenses = query_db(
        "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='expense' AND substr(date,1,4)=?",
        (current_user.client_id, year),
        one=True,
    )["total"]
    categories = query_db(
        """
        SELECT COALESCE(c.name,'Uncategorized') category, t.type, COALESCE(SUM(t.amount),0) total
        FROM transactions t
        LEFT JOIN categories c ON c.id=t.category_id
        WHERE t.client_id=? AND substr(t.date,1,4)=?
        GROUP BY COALESCE(c.name,'Uncategorized'), t.type
        ORDER BY t.type, total DESC
        """,
        (current_user.client_id, year),
    )
    return render_template(
        "my_bookkeeping.html",
        year=year,
        transactions=transactions,
        categories=categories,
        income=income,
        expenses=expenses,
        profit=money(income) - money(expenses),
    )


@app.route("/my/tax-returns")
@login_required
@client_required
def my_tax_returns():
    returns = query_db(
        """
        SELECT tr.*, i.invoice_number, i.status invoice_status, i.amount invoice_amount
        FROM tax_returns tr
        LEFT JOIN invoices i ON i.id=tr.invoice_id
        WHERE tr.client_id=?
        ORDER BY tr.tax_year DESC, tr.id DESC
        """,
        (current_user.client_id,),
    )
    documents = query_db(
        """
        SELECT *, COALESCE(document_name,name,'Document') display_name
        FROM documents
        WHERE client_id=? AND category IN ('Tax Documents','Identification','Payroll','Receipts','Bank Statements')
        ORDER BY id DESC
        """,
        (current_user.client_id,),
    )
    return render_template("my_tax_returns.html", returns=returns, documents=documents)


@app.route("/my/tax-return-question", methods=["POST"])
@login_required
@client_required
def my_tax_return_question():
    body = request.form.get("body") or ""
    execute_db(
        "INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
        (current_user.client_id, "client", current_user.name, "Tax Return Question", body),
    )
    flash("Tax return question sent to the office.", "success")
    return redirect(url_for("my_tax_returns"))

# ==========================================================
# PPT CONNECTION PACK V41-V45 ROUTES
# ==========================================================

@app.route('/tax-organizer')
@login_required
def tax_organizer():
    return render_template('tax_organizer.html')

@app.route('/review-queue')
@login_required
def review_queue():
    return render_template('review_queue.html')

@app.route('/engagement-letters')
@login_required
def engagement_letters():
    return render_template('engagement_letters.html')

@app.route('/esign-center')
@login_required
def esign_center():
    return render_template('esign_center.html')

@app.route('/staff-tasks')
@login_required
def staff_tasks():
    return render_template('staff_tasks.html')

@app.route('/analytics')
@login_required
def analytics():
    return render_template('analytics.html')

@app.route('/notifications')
@login_required
def notifications():
    return render_template('notifications.html')

@app.route('/client-retention')
@login_required
def client_retention():
    return render_template('client_retention.html')

@app.route('/tax-planning')
@login_required
def tax_planning():
    return render_template('tax_planning.html')

@app.route('/admin-control')
@login_required
def admin_control():
    return render_template('admin_control.html')

# === PPT CLIENT SIDE FULL MODULE REPAIR END ===


# ============================================================
# PPT FULL AUTOMATION UPGRADE PACK
# Modules: Bookkeeping Auto | Documents 2-way | Invoice Auto
#          Notifications | Standalone Service Entry
# ============================================================

# ── SCHEMA UPGRADE ──────────────────────────────────────────

def ensure_upgrade_tables():
    db = get_db()
    db.executescript('''
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        type TEXT,
        message TEXT,
        link TEXT,
        is_read INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS recurring_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        description TEXT NOT NULL,
        type TEXT NOT NULL,
        category_id INTEGER,
        amount REAL NOT NULL,
        frequency TEXT DEFAULT 'monthly',
        next_due TEXT,
        notes TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS categorization_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT NOT NULL,
        category_id INTEGER,
        type TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS csv_imports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        filename TEXT,
        rows_imported INTEGER DEFAULT 0,
        imported_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    # Add visible_to_client flag to documents
    try:
        add_column_if_missing("documents", "visible_to_client", "INTEGER DEFAULT 1")
    except Exception:
        pass
    try:
        add_column_if_missing("documents", "uploaded_for_client", "INTEGER DEFAULT 0")
    except Exception:
        pass
    db.commit()


def push_notification(client_id, ntype, message, link=""):
    try:
        ensure_upgrade_tables()
        execute_db(
            "INSERT INTO notifications(client_id,type,message,link) VALUES (?,?,?,?)",
            (client_id, ntype, message, link),
        )
    except Exception:
        pass


def auto_categorize(description):
    """Return (category_id, type) from rules or None."""
    rules = query_db("SELECT * FROM categorization_rules ORDER BY id")
    desc_lower = description.lower()
    for r in rules:
        if r["keyword"].lower() in desc_lower:
            return r["category_id"], r["type"]
    return None, None


def next_due_date(frequency, from_date=None):
    from datetime import timedelta
    base = datetime.strptime(from_date, "%Y-%m-%d") if from_date else datetime.now()
    if frequency == "weekly":
        return (base + timedelta(weeks=1)).strftime("%Y-%m-%d")
    if frequency == "biweekly":
        return (base + timedelta(weeks=2)).strftime("%Y-%m-%d")
    if frequency == "quarterly":
        month = base.month + 3
        year = base.year + month // 13
        month = month % 12 or 12
        return base.replace(year=year, month=month).strftime("%Y-%m-%d")
    if frequency == "annually":
        return base.replace(year=base.year + 1).strftime("%Y-%m-%d")
    # default monthly
    month = base.month % 12 + 1
    year = base.year + (1 if base.month == 12 else 0)
    return base.replace(year=year, month=month).strftime("%Y-%m-%d")


# ── NOTIFICATION ROUTES ──────────────────────────────────────

@app.route("/notifications/count")
@login_required
def notification_count():
    ensure_upgrade_tables()
    if current_user.role == "admin":
        count = query_db("SELECT COUNT(*) c FROM notifications WHERE is_read=0", one=True)["c"]
    else:
        count = query_db("SELECT COUNT(*) c FROM notifications WHERE client_id=? AND is_read=0",
                         (current_user.client_id,), one=True)["c"]
    from flask import jsonify
    return jsonify({"count": count})


@app.route("/notifications/list")
@login_required
def notifications_list():
    ensure_upgrade_tables()
    if current_user.role == "admin":
        items = query_db("""SELECT n.*, c.name client_name FROM notifications n
                            LEFT JOIN clients c ON c.id=n.client_id
                            ORDER BY n.id DESC LIMIT 60""")
    else:
        items = query_db("SELECT * FROM notifications WHERE client_id=? ORDER BY id DESC LIMIT 40",
                         (current_user.client_id,))
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>Notifications</h1>{%set unread=namespace(count=0)%}{%for n in notifications%}{%if not n.is_read%}{%set unread.count=unread.count+1%}{%endif%}{%endfor%}<p class="sub">{%if unread.count>0%}<span class="pill warn">{{unread.count}} unread</span>{%else%}<span class="pill">All caught up</span>{%endif%}</p><div class="quickbar"><form method="POST" action="/notifications/mark-read"><button type="submit">Mark all read</button></form></div>{%if notifications%}<div class="card" style="padding:0;overflow:hidden">{%for n in notifications%}<div style="display:flex;align-items:flex-start;gap:14px;padding:14px 20px;border-bottom:1px solid #e5e7eb;background:{{"#f0fdf4"if not n.is_read else"#fff"}}"><div style="font-size:20px;min-width:28px;font-weight:900">{%if n.type=="document"%}D{%elif n.type=="invoice"%}I{%elif n.type=="payment"%}P{%elif n.type=="message"%}M{%elif n.type=="appointment"%}A{%else%}N{%endif%}</div><div style="flex:1"><div style="font-size:14px;font-weight:{{"900"if not n.is_read else"400"}}">{{n.message}}</div><div style="font-size:11px;color:#475569;margin-top:3px">{{n.created_at[:16]if n.created_at else""}}{%if not n.is_read%} - New{%endif%}</div></div><div style="display:flex;gap:5px">{%if n.link%}<a href="{{n.link}}" class="btn" style="padding:5px 10px;font-size:12px">View</a>{%endif%}{%if not n.is_read%}<form method="POST" action="/notifications/mark-read"><input type="hidden" name="id" value="{{n.id}}"><button style="padding:5px 10px;font-size:11px;background:#f1f5f9;color:#475569;border:0;border-radius:8px;cursor:pointer">Done</button></form>{%endif%}</div></div>{%endfor%}</div>{%else%}<div class="card"><p style="color:#475569;text-align:center;padding:30px">No notifications yet.</p></div>{%endif%}{%endblock%}""", notifications=items)


@app.route("/notifications/mark-read", methods=["POST"])
@login_required
def mark_notifications_read():
    ensure_upgrade_tables()
    nid = request.form.get("id")
    if nid:
        execute_db("UPDATE notifications SET is_read=1 WHERE id=?", (nid,))
    else:
        if current_user.role == "admin":
            execute_db("UPDATE notifications SET is_read=1")
        else:
            execute_db("UPDATE notifications SET is_read=1 WHERE client_id=?",
                       (current_user.client_id,))
    from flask import jsonify
    return jsonify({"ok": True})


# ── DOCUMENT MANAGEMENT (BOTH SIDES) ─────────────────────────

@app.route("/my/documents", methods=["GET", "POST"])
@login_required
@client_required
def my_documents():
    ensure_upgrade_tables()
    if request.method == "POST":
        f = request.files.get("file")
        if not f or not f.filename or not allowed_file(f.filename):
            flash("Choose a valid file (pdf, png, jpg, doc, docx, xls, xlsx, csv, txt).", "danger")
            return redirect(url_for("my_documents"))
        filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{current_user.client_id}_{secure_filename(f.filename)}"
        f.save(UPLOAD_DIR / filename)
        doc_name = request.form.get("document_name") or f.filename
        execute_db(
            "INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by,visible_to_client) VALUES (?,?,?,?,?,'Uploaded by Client',?,?,'Tax Documents',?,1)",
            (current_user.client_id, doc_name, doc_name, filename,
             request.form.get("tax_year"), request.form.get("notes"),
             current_user.name),
        )
        push_notification(
            current_user.client_id, "document",
            f"You uploaded: {doc_name}",
            "/my/documents",
        )
        flash("Document uploaded successfully.", "success")
        return redirect(url_for("my_documents"))

    docs = query_db(
        "SELECT *, COALESCE(document_name,name,'Document') display_name FROM documents WHERE client_id=? AND visible_to_client=1 ORDER BY id DESC",
        (current_user.client_id,),
    )
    categories = ["Tax Documents", "Identification", "Payroll", "Bank Statements", "Receipts", "Other"]
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>My Documents</h1><p class="sub">Upload and view your documents.</p><div class="card"><h2 style="margin-top:0">Upload Document</h2><form method="POST" enctype="multipart/form-data" class="grid grid-3"><div style="grid-column:span 3"><label>File</label><input type="file" name="file" accept=".pdf,.doc,.docx,.xls,.xlsx,.png,.jpg,.jpeg,.csv,.txt" required></div><div style="grid-column:span 2"><label>Document Name</label><input type="text" name="document_name" placeholder="Auto from filename"></div><div><label>Tax Year</label><input type="text" name="tax_year" placeholder="{{current_year}}"></div><div><label>Category</label><select name="category">{%for cat in categories%}<option>{{cat}}</option>{%endfor%}</select></div><div style="grid-column:span 2"><label>Notes</label><input type="text" name="notes" placeholder="Optional"></div><div style="grid-column:span 3"><button type="submit">Upload Document</button></div></form></div><div class="card"><h2 style="margin-top:0">{{documents|length}} Document{{"s"if documents|length!=1}}</h2>{%if documents%}<div class="table-wrap"><table><thead><tr><th>Document</th><th>Category</th><th>Year</th><th>Status</th><th>Date</th><th></th></tr></thead><tbody>{%for d in documents%}<tr><td><strong>{{d.display_name}}</strong></td><td style="font-size:12px">{{d.category or"--"}}</td><td style="font-size:12px">{{d.tax_year or"--"}}</td><td><span class="pill">{{d.status or"Received"}}</span></td><td style="font-size:12px;color:#475569">{{d.created_at[:10]if d.created_at else"--"}}</td><td>{%if d.filename%}<a href="/documents/download/{{d.id}}" class="btn" style="padding:5px 10px;font-size:12px">Download</a>{%endif%}</td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:20px">No documents yet.</p>{%endif%}</div>{%endblock%}""", documents=docs, categories=categories, current_year=datetime.now().year)


@app.route("/admin/documents/upload", methods=["GET", "POST"])
@login_required
@admin_required
def admin_upload_document():
    ensure_upgrade_tables()
    clients = query_db("SELECT id,name FROM clients ORDER BY name")
    if request.method == "POST":
        client_id = request.form.get("client_id")
        f = request.files.get("file")
        if not f or not f.filename or not allowed_file(f.filename):
            flash("Choose a valid file.", "danger")
            return redirect(url_for("admin_upload_document"))
        filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{client_id}_{secure_filename(f.filename)}"
        f.save(UPLOAD_DIR / filename)
        doc_name = request.form.get("document_name") or f.filename
        visible = 1 if request.form.get("visible_to_client") else 0
        execute_db(
            "INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by,visible_to_client,uploaded_for_client) VALUES (?,?,?,?,?,'Admin Upload',?,?,?,1,?,?)",
            (client_id, doc_name, doc_name, filename,
             request.form.get("tax_year"), request.form.get("notes"),
             request.form.get("category") or "Tax Documents",
             current_user.name, visible, 1),
        )
        if visible:
            push_notification(
                client_id, "document",
                f"New document available: {doc_name}",
                "/my/documents",
            )
        log_activity(client_id, "Document Upload", f"Admin uploaded: {doc_name}", "")
        flash("Document uploaded to client.", "success")
        return redirect(url_for("documents"))
    return render_template("admin_upload_document.html", clients=clients,
                           categories=["Tax Documents", "Identification", "Payroll",
                                       "Bank Statements", "Receipts", "Engagement Letters",
                                       "Signed Returns", "Other"],
                           current_year=datetime.now().year)


@app.route("/documents/<int:doc_id>/toggle-visibility", methods=["POST"])
@login_required
@admin_required
def toggle_document_visibility(doc_id):
    doc = query_db("SELECT * FROM documents WHERE id=?", (doc_id,), one=True)
    if not doc:
        abort(404)
    new_val = 0 if doc["visible_to_client"] else 1
    execute_db("UPDATE documents SET visible_to_client=? WHERE id=?", (new_val, doc_id))
    flash("Document visibility updated.", "success")
    return redirect(url_for("documents"))


@app.route("/documents/<int:doc_id>/delete", methods=["POST"])
@login_required
@admin_required
def delete_document(doc_id):
    doc = query_db("SELECT * FROM documents WHERE id=?", (doc_id,), one=True)
    if not doc:
        abort(404)
    if doc["filename"]:
        try:
            (UPLOAD_DIR / doc["filename"]).unlink(missing_ok=True)
        except Exception:
            pass
    execute_db("DELETE FROM documents WHERE id=?", (doc_id,))
    flash("Document deleted.", "success")
    return redirect(url_for("documents"))


# ── BOOKKEEPING AUTOMATION ───────────────────────────────────

@app.route("/bookkeeping/csv-import", methods=["GET", "POST"])
@login_required
@admin_required
def csv_import():
    ensure_upgrade_tables()
    clients = query_db("SELECT id,name FROM clients ORDER BY name")
    categories = query_db("SELECT * FROM categories ORDER BY kind,name")
    if request.method == "POST":
        import csv, io
        client_id = request.form.get("client_id")
        f = request.files.get("csvfile")
        if not f or not f.filename:
            flash("Please choose a CSV file.", "danger")
            return redirect(url_for("csv_import"))
        content = f.read().decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        imported = 0
        skipped = 0
        for row in rows:
            # flexible column name matching
            date_val = (row.get("Date") or row.get("date") or row.get("DATE") or "").strip()
            desc_val = (row.get("Description") or row.get("description") or row.get("Memo") or row.get("memo") or "").strip()
            amount_raw = (row.get("Amount") or row.get("amount") or row.get("Debit") or row.get("Credit") or "0").strip()
            ttype = (row.get("Type") or row.get("type") or "").strip().lower()
            if not date_val or not desc_val:
                skipped += 1
                continue
            amt = money(amount_raw)
            if amt == 0:
                skipped += 1
                continue
            if not ttype:
                ttype = "income" if amt > 0 else "expense"
            amt = abs(amt)
            cat_id, auto_type = auto_categorize(desc_val)
            if auto_type:
                ttype = auto_type
            execute_db(
                "INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)",
                (date_val, desc_val, ttype, cat_id, client_id, amt, "CSV Import"),
            )
            imported += 1
        execute_db(
            "INSERT INTO csv_imports(client_id,filename,rows_imported,imported_by) VALUES (?,?,?,?)",
            (client_id, f.filename, imported, current_user.name),
        )
        push_notification(client_id, "bookkeeping", f"{imported} transactions imported from {f.filename}", "/my/bookkeeping")
        flash(f"Imported {imported} transactions. Skipped {skipped} rows.", "success")
        return redirect(url_for("transactions"))
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>CSV Bank Import</h1><p class="sub">Import transactions from a bank export or QuickBooks CSV.</p><div class="card"><h2 style="margin-top:0">Import File</h2><form method="POST" enctype="multipart/form-data" class="grid grid-3"><div><label>Client</label><select name="client_id" required><option value="">-- Select client --</option>{%for c in clients%}<option value="{{c.id}}">{{c.name}}</option>{%endfor%}</select></div><div style="grid-column:span 2"><label>CSV File</label><input type="file" name="csvfile" accept=".csv" required></div><div style="grid-column:span 3"><button type="submit">Import Transactions</button></div></form></div><div class="card"><h2>Accepted Columns</h2><p style="font-size:13px;color:#475569">Date, Description, Amount, Type (income/expense) -- negatives become expenses automatically.</p></div>{%endblock%}""", clients=clients, categories=categories)


@app.route("/bookkeeping/recurring", methods=["GET", "POST"])
@login_required
@admin_required
def recurring_transactions():
    ensure_upgrade_tables()
    clients = query_db("SELECT id,name FROM clients ORDER BY name")
    categories = query_db("SELECT * FROM categories ORDER BY kind,name")
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            client_id = request.form.get("client_id")
            freq = request.form.get("frequency") or "monthly"
            execute_db(
                "INSERT INTO recurring_transactions(client_id,description,type,category_id,amount,frequency,next_due,notes) VALUES (?,?,?,?,?,?,?,?)",
                (client_id, request.form.get("description"), request.form.get("type"),
                 request.form.get("category_id") or None,
                 money(request.form.get("amount")), freq,
                 next_due_date(freq), request.form.get("notes")),
            )
            flash("Recurring transaction added.", "success")
        elif action == "toggle":
            rid = request.form.get("id")
            row = query_db("SELECT is_active FROM recurring_transactions WHERE id=?", (rid,), one=True)
            if row:
                execute_db("UPDATE recurring_transactions SET is_active=? WHERE id=?",
                           (0 if row["is_active"] else 1, rid))
            flash("Recurring transaction updated.", "success")
        elif action == "delete":
            execute_db("DELETE FROM recurring_transactions WHERE id=?", (request.form.get("id"),))
            flash("Recurring transaction deleted.", "success")
        elif action == "post_now":
            rid = request.form.get("id")
            rec = query_db("SELECT * FROM recurring_transactions WHERE id=?", (rid,), one=True)
            if rec:
                execute_db(
                    "INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)",
                    (datetime.now().strftime("%Y-%m-%d"), rec["description"], rec["type"],
                     rec["category_id"], rec["client_id"], rec["amount"],
                     f"Auto-posted from recurring #{rid}"),
                )
                execute_db("UPDATE recurring_transactions SET next_due=? WHERE id=?",
                           (next_due_date(rec["frequency"]), rid))
                flash("Transaction posted.", "success")
        return redirect(url_for("recurring_transactions"))
    rows = query_db("""SELECT r.*,cl.name client_name,c.name category_name
                       FROM recurring_transactions r
                       LEFT JOIN clients cl ON cl.id=r.client_id
                       LEFT JOIN categories c ON c.id=r.category_id
                       ORDER BY r.id DESC""")
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>Recurring Transactions</h1><p class="sub">Auto-post repeating income and expenses on schedule.</p><div class="card"><h2 style="margin-top:0">Add Recurring</h2><form method="POST" class="grid grid-3"><input type="hidden" name="action" value="add"><div><label>Client</label><select name="client_id"><option value="">-- None --</option>{%for c in clients%}<option value="{{c.id}}">{{c.name}}</option>{%endfor%}</select></div><div style="grid-column:span 2"><label>Description</label><input type="text" name="description" required></div><div><label>Amount</label><input type="number" name="amount" step="0.01" required></div><div><label>Type</label><select name="type"><option value="income">Income</option><option value="expense">Expense</option></select></div><div><label>Frequency</label><select name="frequency">{%for f in frequencies%}<option value="{{f}}">{{f|title}}</option>{%endfor%}</select></div><div style="grid-column:span 2"><label>Category</label><select name="category_id"><option value="">Uncategorized</option>{%for cat in categories%}<option value="{{cat.id}}">[{{cat.kind|title}}] {{cat.name}}</option>{%endfor%}</select></div><div><label>Notes</label><input type="text" name="notes"></div><div style="grid-column:span 3"><button type="submit">Add Recurring</button></div></form></div><div class="card"><div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px"><h2 style="margin:0">{{rows|length}} Recurring</h2><form method="POST" action="/bookkeeping/post-due"><button>Post All Due</button></form></div>{%if rows%}<div class="table-wrap"><table><thead><tr><th>Description</th><th>Client</th><th>Amount</th><th>Frequency</th><th>Next Due</th><th>Status</th><th>Actions</th></tr></thead><tbody>{%for r in rows%}<tr><td><strong>{{r.description}}</strong></td><td>{{r.client_name or"--"}}</td><td style="font-weight:900">${{"%.2f"|format(r.amount)}}</td><td>{{r.frequency|title}}</td><td style="font-size:12px">{{r.next_due or"--"}}</td><td><span class="pill{%if not r.is_active%} warn{%endif%}">{{"Active"if r.is_active else"Paused"}}</span></td><td><form method="POST" style="display:inline"><input type="hidden" name="action" value="post_now"><input type="hidden" name="id" value="{{r.id}}"><button style="padding:5px 10px;font-size:12px">Post</button></form><form method="POST" style="display:inline;margin-left:4px"><input type="hidden" name="action" value="toggle"><input type="hidden" name="id" value="{{r.id}}"><button style="padding:5px 10px;font-size:12px;background:#f1f5f9;color:#0f172a">{{"Pause"if r.is_active else"Resume"}}</button></form><form method="POST" style="display:inline;margin-left:4px" onsubmit="return confirm('Delete?')"><input type="hidden" name="action" value="delete"><input type="hidden" name="id" value="{{r.id}}"><button style="padding:5px 10px;font-size:12px;background:#fef2f2;color:#b91c1c">Del</button></form></td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:20px">No recurring transactions yet.</p>{%endif%}</div>{%endblock%}""", rows=rows, clients=clients, categories=categories, frequencies=["weekly","biweekly","monthly","quarterly","annually"])


@app.route("/bookkeeping/post-due", methods=["POST"])
@login_required
@admin_required
def post_due_recurring():
    ensure_upgrade_tables()
    today = datetime.now().strftime("%Y-%m-%d")
    due = query_db("SELECT * FROM recurring_transactions WHERE is_active=1 AND next_due<=?", (today,))
    posted = 0
    for rec in due:
        execute_db(
            "INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)",
            (today, rec["description"], rec["type"], rec["category_id"],
             rec["client_id"], rec["amount"], f"Auto-posted recurring #{rec['id']}"),
        )
        execute_db("UPDATE recurring_transactions SET next_due=? WHERE id=?",
                   (next_due_date(rec["frequency"], today), rec["id"]))
        posted += 1
    flash(f"Posted {posted} recurring transaction(s).", "success")
    return redirect(url_for("recurring_transactions"))


@app.route("/bookkeeping/rules", methods=["GET", "POST"])
@login_required
@admin_required
def categorization_rules():
    ensure_upgrade_tables()
    categories = query_db("SELECT * FROM categories ORDER BY kind,name")
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            execute_db(
                "INSERT INTO categorization_rules(keyword,category_id,type) VALUES (?,?,?)",
                (request.form.get("keyword"), request.form.get("category_id"), request.form.get("type")),
            )
            flash("Rule added.", "success")
        elif action == "delete":
            execute_db("DELETE FROM categorization_rules WHERE id=?", (request.form.get("id"),))
            flash("Rule deleted.", "success")
        return redirect(url_for("categorization_rules"))
    rules = query_db("""SELECT r.*,c.name category_name FROM categorization_rules r
                        LEFT JOIN categories c ON c.id=r.category_id ORDER BY r.id DESC""")
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>Auto-Categorization Rules</h1><p class="sub">Keyword rules auto-categorize CSV imports and transactions.</p><div class="card"><h2 style="margin-top:0">Add Rule</h2><form method="POST" class="grid grid-3"><input type="hidden" name="action" value="add"><div><label>Keyword</label><input type="text" name="keyword" placeholder="e.g. QuickBooks" required></div><div><label>Type</label><select name="type"><option value="expense">Expense</option><option value="income">Income</option></select></div><div><label>Category</label><select name="category_id" required><option value="">Select</option>{%for cat in categories%}<option value="{{cat.id}}">[{{cat.kind|title}}] {{cat.name}}</option>{%endfor%}</select></div><div><button type="submit">Add Rule</button></div></form></div><div class="card"><h2 style="margin-top:0">{{rules|length}} Rules</h2>{%if rules%}<div class="table-wrap"><table><thead><tr><th>Keyword</th><th>Type</th><th>Category</th><th>Added</th><th></th></tr></thead><tbody>{%for r in rules%}<tr><td><span class="pill">"{{r.keyword}}"</span></td><td><span class="pill{%if r.type!="income"%} warn{%endif%}">{{r.type|title}}</span></td><td>{{r.category_name or"--"}}</td><td style="font-size:12px;color:#475569">{{r.created_at[:10]if r.created_at else"--"}}</td><td><form method="POST" onsubmit="return confirm('Delete?')"><input type="hidden" name="action" value="delete"><input type="hidden" name="id" value="{{r.id}}"><button style="padding:5px 10px;font-size:12px;background:#fef2f2;color:#b91c1c;border:0;border-radius:8px;cursor:pointer">Delete</button></form></td></tr>{%endfor%}</tbody></table></div>{%else%}<p style="color:#475569;text-align:center;padding:20px">No rules yet.</p>{%endif%}</div>{%endblock%}""", rules=rules, categories=categories)


# ── INVOICE AUTOMATION ───────────────────────────────────────

def mark_overdue_invoices():
    """Call this on any dashboard load — marks past-due unpaid invoices."""
    today = datetime.now().strftime("%Y-%m-%d")
    execute_db(
        "UPDATE invoices SET status='Overdue' WHERE status NOT IN ('Paid','Overdue') AND due_date IS NOT NULL AND due_date < ?",
        (today,),
    )


@app.route("/invoices/mark-overdue", methods=["POST"])
@login_required
@admin_required
def run_mark_overdue():
    mark_overdue_invoices()
    flash("Overdue invoices updated.", "success")
    return redirect(url_for("invoices"))


@app.route("/invoices/<int:invoice_id>/send-reminder", methods=["POST"])
@login_required
@admin_required
def send_invoice_reminder(invoice_id):
    inv = query_db("SELECT i.*,c.name client_name FROM invoices i LEFT JOIN clients c ON c.id=i.client_id WHERE i.id=?", (invoice_id,), one=True)
    if not inv:
        abort(404)
    msg = f"Reminder: Invoice {inv['invoice_number']} for ${inv['amount']:,.2f} is due {inv['due_date'] or 'soon'}. Please log in to your portal to view and pay."
    execute_db(
        "INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
        (inv["client_id"], "admin", current_user.name, f"Payment Reminder – {inv['invoice_number']}", msg),
    )
    push_notification(inv["client_id"], "invoice", f"Payment reminder: {inv['invoice_number']}", "/my/invoices")
    flash(f"Reminder sent to {inv['client_name']}.", "success")
    return redirect(url_for("invoices"))


@app.route("/invoices/<int:invoice_id>/mark-paid", methods=["POST"])
@login_required
@admin_required
def quick_mark_paid(invoice_id):
    inv = query_db("SELECT * FROM invoices WHERE id=?", (invoice_id,), one=True)
    if not inv:
        abort(404)
    execute_db("UPDATE invoices SET status='Paid',paid_at=CURRENT_TIMESTAMP WHERE id=?", (invoice_id,))
    execute_db(
        "INSERT INTO payments(invoice_id,client_id,amount,method,status,notes) VALUES (?,?,?,'Manual Entry','Paid','Marked paid from invoice list')",
        (invoice_id, inv["client_id"], inv["amount"]),
    )
    push_notification(inv["client_id"], "payment", f"Payment received for invoice {inv['invoice_number']}", "/my/invoices")
    flash("Invoice marked paid.", "success")
    return redirect(url_for("invoices"))


# ── STANDALONE SERVICE ENTRY (admin — any single service) ────
# Clients who only want one service (just bookkeeping, just a
# tax return, just an appointment, etc.) can be entered here
# without needing to touch every other module.

@app.route("/service-entry", methods=["GET", "POST"])
@login_required
@admin_required
def service_entry():
    """Universal single-service entry form for admin."""
    ensure_upgrade_tables()
    clients = query_db("SELECT id,name,business_name,email FROM clients ORDER BY name")
    categories = query_db("SELECT * FROM categories ORDER BY kind,name")

    if request.method == "POST":
        service = request.form.get("service_type")
        client_id = request.form.get("client_id") or None

        if service == "transaction":
            execute_db(
                "INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)",
                (request.form.get("date") or datetime.now().strftime("%Y-%m-%d"),
                 request.form.get("description"), request.form.get("ttype"),
                 request.form.get("category_id") or None, client_id,
                 money(request.form.get("amount")), request.form.get("notes")),
            )
            if client_id:
                push_notification(client_id, "bookkeeping", "A transaction was recorded on your account.", "/my/bookkeeping")
            flash("Transaction recorded.", "success")

        elif service == "invoice":
            inv_num = request.form.get("invoice_number") or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            execute_db(
                "INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,?,?)",
                (client_id, inv_num,
                 request.form.get("issue_date") or datetime.now().strftime("%Y-%m-%d"),
                 request.form.get("due_date"), money(request.form.get("amount")),
                 request.form.get("status") or "Sent", request.form.get("description")),
            )
            if client_id:
                push_notification(client_id, "invoice", f"New invoice {inv_num} created.", "/my/invoices")
            flash("Invoice created.", "success")

        elif service == "payment":
            inv_id = request.form.get("invoice_id") or None
            inv = query_db("SELECT * FROM invoices WHERE id=?", (inv_id,), one=True) if inv_id else None
            paid_client = (inv["client_id"] if inv else client_id)
            execute_db(
                "INSERT INTO payments(invoice_id,client_id,amount,method,reference,status,notes) VALUES (?,?,?,?,?,'Paid',?)",
                (inv_id, paid_client, money(request.form.get("amount")),
                 request.form.get("method") or "Manual Entry",
                 request.form.get("reference"), request.form.get("notes")),
            )
            if inv_id:
                execute_db("UPDATE invoices SET status='Paid',paid_at=CURRENT_TIMESTAMP WHERE id=?", (inv_id,))
            if paid_client:
                push_notification(paid_client, "payment", "Payment recorded on your account.", "/my/invoices")
            flash("Payment recorded.", "success")

        elif service == "appointment":
            execute_db(
                "INSERT INTO appointments(client_id,title,start_at,end_at,location,meeting_link,status,notes) VALUES (?,?,?,?,?,?,?,?)",
                (client_id, request.form.get("title") or "Appointment",
                 request.form.get("start_at"), request.form.get("end_at"),
                 request.form.get("location"), request.form.get("meeting_link"),
                 request.form.get("status") or "Scheduled", request.form.get("notes")),
            )
            if client_id:
                push_notification(client_id, "appointment", "An appointment has been scheduled for you.", "/my/appointments")
            flash("Appointment scheduled.", "success")

        elif service == "tax_return":
            inv_id = execute_db(
                "INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,'Sent','Tax return service')",
                (client_id,
                 f"TR-{request.form.get('tax_year')}-{datetime.now().strftime('%H%M%S')}",
                 datetime.now().strftime("%Y-%m-%d"),
                 request.form.get("due_date"),
                 money(request.form.get("fee"))),
            )
            execute_db(
                "INSERT INTO tax_returns(client_id,tax_year,service_type,status,due_date,fee,notes,invoice_id) VALUES (?,?,?,?,?,?,?,?)",
                (client_id, request.form.get("tax_year"),
                 request.form.get("service_type") or "Individual",
                 request.form.get("status") or "In Progress",
                 request.form.get("due_date"), money(request.form.get("fee")),
                 request.form.get("notes"), inv_id),
            )
            if client_id:
                push_notification(client_id, "tax_return", f"Tax return started for {request.form.get('tax_year')}.", "/my/tax-returns")
            flash("Tax return created.", "success")

        elif service == "document":
            f = request.files.get("file")
            if f and f.filename and allowed_file(f.filename):
                filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{client_id}_{secure_filename(f.filename)}"
                f.save(UPLOAD_DIR / filename)
            else:
                filename = None
            doc_name = request.form.get("document_name") or (f.filename if f and f.filename else "Document")
            visible = 1 if request.form.get("visible_to_client") else 0
            execute_db(
                "INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by,visible_to_client) VALUES (?,?,?,?,?,'Admin Entry',?,?,?,?,?)",
                (client_id, doc_name, doc_name, filename,
                 request.form.get("tax_year"), request.form.get("notes"),
                 request.form.get("category") or "Tax Documents",
                 current_user.name, visible),
            )
            if client_id and visible:
                push_notification(client_id, "document", f"New document available: {doc_name}", "/my/documents")
            flash("Document saved.", "success")

        elif service == "message":
            execute_db(
                "INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",
                (client_id, "admin", current_user.name,
                 request.form.get("subject") or "Message from Pinnacle Performance Tax",
                 request.form.get("body")),
            )
            if client_id:
                push_notification(client_id, "message", "You have a new message from the office.", "/my/messages")
            flash("Message sent.", "success")

        elif service == "crm_lead":
            execute_db(
                "INSERT INTO crm_leads(name,phone,email,status,source,follow_up_date,notes,client_id) VALUES (?,?,?,?,?,?,?,?)",
                (request.form.get("lead_name"), request.form.get("lead_phone"),
                 request.form.get("lead_email"),
                 request.form.get("lead_status") or "New",
                 request.form.get("lead_source"), request.form.get("follow_up_date"),
                 request.form.get("notes"), client_id or None),
            )
            flash("CRM lead added.", "success")

        return redirect(url_for("service_entry"))

    # For payment sub-form: list unpaid invoices
    open_invoices = query_db("SELECT i.*,c.name client_name FROM invoices i LEFT JOIN clients c ON c.id=i.client_id WHERE i.status!='Paid' ORDER BY i.id DESC")
    return render_template_string("""{%extends"base.html"%}{%block content%}<h1>Quick Service Entry</h1><p class="sub">Select one service and fill in only what you need.</p><style>.svc-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:4px}.svc-btn{border:2px solid #dfe7df;border-radius:16px;padding:18px 10px;text-align:center;cursor:pointer;background:#fff}.svc-btn:hover,.svc-btn.active{border-color:#11823b;background:#e8f5ec}.svc-btn .ico{font-size:26px;margin-bottom:5px}.svc-btn .lbl{font-size:12px;font-weight:900}.panel{display:none}.panel.active{display:block}</style><div class="card"><h2>Select a Service</h2><div class="svc-grid"><div class="svc-btn" onclick="pick('transaction',this)"><div class="ico">$</div><div class="lbl">Transaction</div></div><div class="svc-btn" onclick="pick('invoice',this)"><div class="ico">#</div><div class="lbl">Invoice</div></div><div class="svc-btn" onclick="pick('payment',this)"><div class="ico">$</div><div class="lbl">Payment</div></div><div class="svc-btn" onclick="pick('appointment',this)"><div class="ico">@</div><div class="lbl">Appointment</div></div><div class="svc-btn" onclick="pick('tax_return',this)"><div class="ico">T</div><div class="lbl">Tax Return</div></div><div class="svc-btn" onclick="pick('document',this)"><div class="ico">D</div><div class="lbl">Document</div></div><div class="svc-btn" onclick="pick('message',this)"><div class="ico">M</div><div class="lbl">Message</div></div><div class="svc-btn" onclick="pick('crm_lead',this)"><div class="ico">L</div><div class="lbl">CRM Lead</div></div></div></div><form method="POST" enctype="multipart/form-data"><input type="hidden" name="service_type" id="svc_field"><div class="card panel" id="block-client"><label>Client</label><select name="client_id"><option value="">-- No client --</option>{%for c in clients%}<option value="{{c.id}}">{{c.name}}</option>{%endfor%}</select></div><div class="card panel" id="panel-transaction"><h2>Transaction</h2><div class="grid grid-3"><div><label>Date</label><input type="date" name="date" value="{{today}}"></div><div><label>Amount</label><input type="number" name="amount" step="0.01" placeholder="0.00"></div><div><label>Type</label><select name="ttype"><option value="income">Income</option><option value="expense">Expense</option></select></div><div style="grid-column:span 2"><label>Description</label><input type="text" name="description"></div><div><label>Category</label><select name="category_id"><option value="">Uncategorized</option>{%for cat in categories%}<option value="{{cat.id}}">[{{cat.kind|title}}] {{cat.name}}</option>{%endfor%}</select></div><div style="grid-column:span 3"><label>Notes</label><textarea name="notes"></textarea></div><div><button type="submit">Save Transaction</button></div></div></div><div class="card panel" id="panel-invoice"><h2>Invoice</h2><div class="grid grid-3"><div><label>Invoice #</label><input type="text" name="invoice_number" placeholder="Auto"></div><div><label>Amount</label><input type="number" name="amount" step="0.01" placeholder="0.00"></div><div><label>Status</label><select name="status"><option value="Sent">Sent</option><option value="Draft">Draft</option><option value="Paid">Paid</option></select></div><div><label>Issue Date</label><input type="date" name="issue_date" value="{{today}}"></div><div><label>Due Date</label><input type="date" name="due_date"></div><div></div><div style="grid-column:span 3"><label>Description</label><input type="text" name="description"></div><div><button type="submit">Save Invoice</button></div></div></div><div class="card panel" id="panel-payment"><h2>Payment</h2><div class="grid grid-3"><div style="grid-column:span 3"><label>Apply to Invoice</label><select name="invoice_id"><option value="">Standalone</option>{%for inv in open_invoices%}<option value="{{inv.id}}">{{inv.invoice_number}} -- {{inv.client_name}} -- ${{inv.amount}}</option>{%endfor%}</select></div><div><label>Amount</label><input type="number" name="amount" step="0.01" placeholder="0.00"></div><div><label>Method</label><select name="method"><option>Manual Entry</option><option>Cash</option><option>Check</option><option>Credit Card</option><option>Zelle</option><option>PayPal</option></select></div><div><label>Reference #</label><input type="text" name="reference"></div><div style="grid-column:span 3"><button type="submit">Save Payment</button></div></div></div><div class="card panel" id="panel-appointment"><h2>Appointment</h2><div class="grid grid-3"><div style="grid-column:span 3"><label>Title</label><input type="text" name="title"></div><div><label>Start</label><input type="datetime-local" name="start_at"></div><div><label>End</label><input type="datetime-local" name="end_at"></div><div><label>Status</label><select name="status"><option>Scheduled</option><option>Confirmed</option><option>Completed</option><option>Cancelled</option></select></div><div><label>Location</label><input type="text" name="location"></div><div style="grid-column:span 2"><label>Meeting Link</label><input type="text" name="meeting_link"></div><div style="grid-column:span 3"><label>Notes</label><textarea name="notes"></textarea></div><div><button type="submit">Save Appointment</button></div></div></div><div class="card panel" id="panel-tax_return"><h2>Tax Return</h2><div class="grid grid-3"><div><label>Tax Year</label><input type="text" name="tax_year" value="{{current_year}}"></div><div><label>Service</label><select name="service_type_tr"><option value="Individual">Individual</option><option value="Business">Business</option><option value="S-Corp">S-Corp</option><option value="Partnership">Partnership</option></select></div><div><label>Status</label><select name="status"><option value="In Progress">In Progress</option><option value="Waiting on Client">Waiting on Client</option><option value="Completed">Completed</option><option value="Filed">Filed</option></select></div><div><label>Fee</label><input type="number" name="fee" step="0.01" placeholder="0.00"></div><div><label>Due Date</label><input type="date" name="due_date"></div><div></div><div style="grid-column:span 3"><label>Notes</label><textarea name="notes"></textarea></div><div><button type="submit">Save Tax Return</button></div></div></div><div class="card panel" id="panel-document"><h2>Document</h2><div class="grid grid-3"><div style="grid-column:span 3"><label>File</label><input type="file" name="file" accept=".pdf,.doc,.docx,.xls,.xlsx,.png,.jpg,.jpeg,.csv,.txt"></div><div style="grid-column:span 2"><label>Document Name</label><input type="text" name="document_name" placeholder="Auto from filename"></div><div><label>Tax Year</label><input type="text" name="tax_year"></div><div><label>Category</label><select name="category"><option>Tax Documents</option><option>Identification</option><option>Payroll</option><option>Bank Statements</option><option>Receipts</option><option>Other</option></select></div><div style="display:flex;align-items:center;gap:8px;padding-top:28px"><input type="checkbox" name="visible_to_client" value="1" checked style="width:auto;margin:0"><label style="margin:0;font-size:13px">Visible to client</label></div><div></div><div style="grid-column:span 3"><label>Notes</label><textarea name="notes"></textarea></div><div><button type="submit">Save Document</button></div></div></div><div class="card panel" id="panel-message"><h2>Message</h2><div class="grid"><div><label>Subject</label><input type="text" name="subject"></div><div><label>Message</label><textarea name="body" style="min-height:130px"></textarea></div><div><button type="submit">Send Message</button></div></div></div><div class="card panel" id="panel-crm_lead"><h2>CRM Lead</h2><div class="grid grid-3"><div><label>Name</label><input type="text" name="lead_name"></div><div><label>Phone</label><input type="tel" name="lead_phone"></div><div><label>Email</label><input type="email" name="lead_email"></div><div><label>Source</label><input type="text" name="lead_source"></div><div><label>Status</label><select name="lead_status"><option>New</option><option>Contacted</option><option>Qualified</option><option>Converted</option><option>Lost</option></select></div><div><label>Follow-up</label><input type="date" name="follow_up_date"></div><div style="grid-column:span 3"><label>Notes</label><textarea name="notes"></textarea></div><div><button type="submit">Save Lead</button></div></div></div></form><script>function pick(svc,el){document.querySelectorAll('.svc-btn').forEach(b=>b.classList.remove('active'));el.classList.add('active');document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));document.getElementById('panel-'+svc).classList.add('active');document.getElementById('block-client').classList.add('active');document.getElementById('svc_field').value=svc;document.getElementById('panel-'+svc).scrollIntoView({behavior:'smooth',block:'start'});}</script>{%endblock%}""", clients=clients, categories=categories, open_invoices=open_invoices, today=datetime.now().strftime("%Y-%m-%d"), current_year=str(datetime.now().year))


# ── WIRING: auto mark overdue on admin dashboard load ────────

@app.before_request
def auto_maintenance():
    """Run lightweight auto-tasks on admin requests."""
    if request.endpoint in ("dashboard", "invoices") and \
       hasattr(current_user, "role") and current_user.is_authenticated and \
       current_user.role == "admin":
        try:
            mark_overdue_invoices()
            ensure_upgrade_tables()
        except Exception:
            pass


# ── INIT ROUTE UPDATE ────────────────────────────────────────

@app.route("/init-upgrade")
def init_upgrade_route():
    with app.app_context():
        init_db()
        ensure_upgrade_tables()
        ensure_elite_operations_tables()
        ensure_workflow_tables()
        ensure_client_template_columns()
        ensure_messages_table()
    return "UPGRADE INIT COMPLETE — all tables ready."

# ============================================================
# END PPT FULL AUTOMATION UPGRADE PACK
# ============================================================


# ============================================================
# PPT PDF EXPORT — Invoice PDF + P&L Report
# ============================================================

def _invoice_html(invoice, payments, paid, balance):
    pay_rows = "".join(
        f"<tr><td>{p['created_at'][:10] if p['created_at'] else '--'}</td>"
        f"<td>{p['method'] or '--'}</td>"
        f"<td style='text-align:right'>${money(p['amount']):,.2f}</td></tr>"
        for p in payments
    )
    pay_section = (
        f"<h3 style='margin:24px 0 8px;color:#11823b'>Payments Received</h3>"
        f"<table><thead><tr><th>Date</th><th>Method</th><th style='text-align:right'>Amount</th></tr></thead>"
        f"<tbody>{pay_rows}</tbody></table>"
        if payments else ""
    )
    status_color = "#0b5f2a" if invoice["status"] == "Paid" else "#9a3412"
    status_bg = "#e8f5ec" if invoice["status"] == "Paid" else "#fff7ed"
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>Invoice {invoice['invoice_number']}</title>
<style>
body{{font-family:Arial,sans-serif;color:#1f2937;margin:0;padding:40px;max-width:800px}}
h1{{font-size:32px;font-weight:900;color:#11823b;margin:0}}
.brand{{font-size:18px;font-weight:900;color:#11823b;line-height:1.3}}
.header{{display:flex;justify-content:space-between;align-items:flex-start;padding-bottom:20px;border-bottom:3px solid #11823b;margin-bottom:28px}}
.meta{{text-align:right;font-size:13px;color:#475569;margin-top:8px}}
.badge{{display:inline-block;padding:4px 14px;border-radius:999px;font-weight:900;font-size:12px;background:{status_bg};color:{status_color}}}
.bill-to{{margin-bottom:28px}}
.label{{font-size:11px;font-weight:900;text-transform:uppercase;color:#9ca3af;margin-bottom:4px}}
table{{width:100%;border-collapse:collapse;margin-bottom:20px}}
th{{background:#f3f4f6;padding:10px 12px;text-align:left;font-size:11px;text-transform:uppercase;color:#6b7280;border-bottom:2px solid #e5e7eb}}
td{{padding:10px 12px;border-bottom:1px solid #f3f4f6;font-size:13px}}
.totals{{max-width:280px;margin-left:auto;margin-top:8px}}
.trow{{display:flex;justify-content:space-between;padding:6px 0;font-size:14px;border-bottom:1px solid #f3f4f6}}
.trow.final{{border-top:2px solid #11823b;border-bottom:none;font-size:18px;font-weight:900;color:#11823b;padding-top:10px;margin-top:4px}}
.footer{{margin-top:40px;padding-top:16px;border-top:1px solid #e5e7eb;text-align:center;color:#9ca3af;font-size:11px}}
@media print{{body{{padding:20px}}.no-print{{display:none}}}}
</style></head><body>
<div class="no-print" style="background:#11823b;color:white;padding:10px 20px;margin:-40px -40px 30px;display:flex;justify-content:space-between;align-items:center">
  <span style="font-weight:900">Invoice {invoice['invoice_number']}</span>
  <button onclick="window.print()" style="background:white;color:#11823b;border:0;border-radius:8px;padding:6px 16px;font-weight:900;cursor:pointer">Print / Save PDF</button>
</div>
<div class="header">
  <div>
    <div class="brand">Pinnacle Performance Tax<br>and Accounting</div>
    <div style="font-size:12px;color:#475569;margin-top:6px">www.pinnacleperformancetax.com<br>pinnacleperformancetax@gmail.com<br>478-338-1632</div>
  </div>
  <div>
    <h1>INVOICE</h1>
    <div class="meta">
      <div><strong>{invoice['invoice_number']}</strong></div>
      <div>Issued: {invoice['issue_date'] or '--'}</div>
      <div>Due: {invoice['due_date'] or '--'}</div>
      <div style="margin-top:6px"><span class="badge">{invoice['status']}</span></div>
    </div>
  </div>
</div>
<div class="bill-to">
  <div class="label">Bill To</div>
  <div style="font-size:15px;line-height:1.7">
    <strong>{invoice.get('client_name') or '--'}</strong><br>
    {(invoice.get('business_name') or '') and (invoice['business_name'] + '<br>') or ''}
    {invoice.get('client_email') or ''}<br>
    {invoice.get('client_phone') or ''}
  </div>
</div>
<table>
  <thead><tr><th>Description</th><th style="text-align:right">Amount</th></tr></thead>
  <tbody><tr>
    <td>{invoice['description'] or 'Tax and Accounting Services'}</td>
    <td style="text-align:right;font-weight:900">${invoice['amount']:,.2f}</td>
  </tr></tbody>
</table>
<div class="totals">
  <div class="trow"><span>Subtotal</span><span>${invoice['amount']:,.2f}</span></div>
  <div class="trow"><span>Amount Paid</span><span style="color:#0b5f2a">${paid:,.2f}</span></div>
  <div class="trow final"><span>Balance Due</span><span>${balance:,.2f}</span></div>
</div>
{pay_section}
<div class="footer">Thank you for your business! — Pinnacle Performance Tax and Accounting</div>
</body></html>"""


def _pl_html(client, year, income, expenses, profit, by_category, transactions):
    income_rows = "".join(
        f"<tr><td>{r['category']}</td><td style='text-align:right'>${r['total']:,.2f}</td></tr>"
        for r in by_category if r["type"] == "income"
    ) or "<tr><td colspan=2 style='color:#9ca3af'>No income</td></tr>"
    expense_rows = "".join(
        f"<tr><td>{r['category']}</td><td style='text-align:right'>${r['total']:,.2f}</td></tr>"
        for r in by_category if r["type"] == "expense"
    ) or "<tr><td colspan=2 style='color:#9ca3af'>No expenses</td></tr>"
    tx_rows = "".join(
        f"<tr><td>{t['date']}</td><td>{t['description']}</td>"
        f"<td style='color:#6b7280;font-size:11px'>{t.get('category_name') or '--'}</td>"
        f"<td style='color:{'#0b5f2a' if t['type']=='income' else '#b91c1c'}'>{t['type'].title()}</td>"
        f"<td style='text-align:right'>${money(t['amount']):,.2f}</td></tr>"
        for t in transactions
    ) or "<tr><td colspan=5 style='color:#9ca3af'>No transactions</td></tr>"
    profit_color = "#0b5f2a" if profit >= 0 else "#b91c1c"
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>P&L {client['name']} {year}</title>
<style>
body{{font-family:Arial,sans-serif;color:#1f2937;margin:0;padding:40px;max-width:900px}}
.brand{{font-size:18px;font-weight:900;color:#11823b}}
h1{{font-size:26px;font-weight:900;margin:8px 0 4px}}
.sub{{color:#475569;font-size:14px;margin-bottom:24px}}
.summary{{display:flex;gap:16px;margin:20px 0 28px}}
.metric{{flex:1;background:#f9fafb;border-radius:10px;padding:16px;text-align:center;border:1px solid #e5e7eb}}
.metric .lbl{{font-size:10px;text-transform:uppercase;color:#6b7280;font-weight:900}}
.metric .val{{font-size:22px;font-weight:900;margin-top:4px}}
h3{{font-size:14px;font-weight:900;text-transform:uppercase;color:#11823b;margin:24px 0 8px;border-bottom:2px solid #e5e7eb;padding-bottom:6px}}
table{{width:100%;border-collapse:collapse;margin-bottom:8px;font-size:13px}}
th{{background:#f3f4f6;padding:8px 10px;text-align:left;font-size:10px;text-transform:uppercase;color:#6b7280}}
td{{padding:8px 10px;border-bottom:1px solid #f9fafb}}
.footer{{margin-top:32px;padding-top:12px;border-top:1px solid #e5e7eb;text-align:center;color:#9ca3af;font-size:11px}}
@media print{{.no-print{{display:none}}}}
</style></head><body>
<div class="no-print" style="background:#11823b;color:white;padding:10px 20px;margin:-40px -40px 30px;display:flex;justify-content:space-between;align-items:center">
  <span style="font-weight:900">P&L Report — {client['name']} {year}</span>
  <button onclick="window.print()" style="background:white;color:#11823b;border:0;border-radius:8px;padding:6px 16px;font-weight:900;cursor:pointer">Print / Save PDF</button>
</div>
<div class="brand">Pinnacle Performance Tax and Accounting</div>
<h1>Profit & Loss Statement</h1>
<div class="sub">{client['name']}{(' — ' + client['business_name']) if client.get('business_name') else ''} | Tax Year {year}</div>
<div class="summary">
  <div class="metric"><div class="lbl">Total Income</div><div class="val" style="color:#0b5f2a">${income:,.2f}</div></div>
  <div class="metric"><div class="lbl">Total Expenses</div><div class="val" style="color:#b91c1c">${expenses:,.2f}</div></div>
  <div class="metric"><div class="lbl">Net Profit</div><div class="val" style="color:{profit_color}">${profit:,.2f}</div></div>
</div>
<h3>Income by Category</h3>
<table><thead><tr><th>Category</th><th style="text-align:right">Amount</th></tr></thead>
<tbody>{income_rows}
<tr style="font-weight:900;background:#f0fdf4"><td>Total Income</td><td style="text-align:right">${income:,.2f}</td></tr>
</tbody></table>
<h3>Expenses by Category</h3>
<table><thead><tr><th>Category</th><th style="text-align:right">Amount</th></tr></thead>
<tbody>{expense_rows}
<tr style="font-weight:900;background:#fef2f2"><td>Total Expenses</td><td style="text-align:right">${expenses:,.2f}</td></tr>
</tbody></table>
<h3>Transaction Detail</h3>
<table><thead><tr><th>Date</th><th>Description</th><th>Category</th><th>Type</th><th style="text-align:right">Amount</th></tr></thead>
<tbody>{tx_rows}</tbody></table>
<div class="footer">Generated {datetime.now().strftime("%B %d, %Y")} — Pinnacle Performance Tax and Accounting</div>
</body></html>"""


@app.route("/invoice/<int:invoice_id>/pdf")
@login_required
def invoice_pdf(invoice_id):
    from flask import Response
    invoice = query_db("""SELECT i.*, c.name client_name, c.business_name,
                          c.email client_email, c.phone client_phone
                          FROM invoices i LEFT JOIN clients c ON c.id=i.client_id
                          WHERE i.id=?""", (invoice_id,), one=True)
    if not invoice: abort(404)
    if current_user.role != "admin" and invoice["client_id"] != current_user.client_id: abort(403)
    payments = query_db("SELECT * FROM payments WHERE invoice_id=? ORDER BY id DESC", (invoice_id,))
    paid = sum(money(p["amount"]) for p in payments)
    balance = money(invoice["amount"]) - paid
    html = _invoice_html(invoice, payments, paid, balance)
    return Response(html, mimetype="text/html")


@app.route("/client/<int:client_id>/pl-report")
@login_required
@admin_required
def pl_report(client_id):
    from flask import Response
    year = request.args.get("year") or str(datetime.now().year)
    client = query_db("SELECT * FROM clients WHERE id=?", (client_id,), one=True)
    if not client: abort(404)
    income = money(query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='income' AND substr(date,1,4)=?", (client_id, year), one=True)["total"])
    expenses = money(query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='expense' AND substr(date,1,4)=?", (client_id, year), one=True)["total"])
    by_category = query_db("SELECT COALESCE(c.name,'Uncategorized') category,t.type,COALESCE(SUM(t.amount),0) total FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? AND substr(t.date,1,4)=? GROUP BY COALESCE(c.name,'Uncategorized'),t.type ORDER BY t.type,total DESC", (client_id, year))
    transactions = query_db("SELECT t.*,c.name category_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? AND substr(t.date,1,4)=? ORDER BY t.date DESC", (client_id, year))
    html = _pl_html(client, year, income, expenses, income - expenses, by_category, transactions)
    return Response(html, mimetype="text/html")


@app.route("/my/pl-report")
@login_required
@client_required
def my_pl_report():
    from flask import Response
    year = request.args.get("year") or str(datetime.now().year)
    client = query_db("SELECT * FROM clients WHERE id=?", (current_user.client_id,), one=True)
    if not client: abort(404)
    income = money(query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='income' AND substr(date,1,4)=?", (current_user.client_id, year), one=True)["total"])
    expenses = money(query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE client_id=? AND type='expense' AND substr(date,1,4)=?", (current_user.client_id, year), one=True)["total"])
    by_category = query_db("SELECT COALESCE(c.name,'Uncategorized') category,t.type,COALESCE(SUM(t.amount),0) total FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? AND substr(t.date,1,4)=? GROUP BY COALESCE(c.name,'Uncategorized'),t.type ORDER BY t.type,total DESC", (current_user.client_id, year))
    transactions = query_db("SELECT t.*,c.name category_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? AND substr(t.date,1,4)=? ORDER BY t.date DESC", (current_user.client_id, year))
    html = _pl_html(client, year, income, expenses, income - expenses, by_category, transactions)
    return Response(html, mimetype="text/html")

# ============================================================
# END PPT PDF EXPORT
# ============================================================

if __name__=='__main__':
    with app.app_context():
        init_db()
        ensure_upgrade_tables()
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',5000)))
