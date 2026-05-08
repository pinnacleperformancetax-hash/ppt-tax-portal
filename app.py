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

@app.route('/init')
def init_route(): init_db(); ensure_messages_table(); return 'INIT COMPLETE - client modules repaired and categories deduped'
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
    if request.method=='POST': execute_db('INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,?,?)',(request.form.get('client_id'),request.form.get('invoice_number') or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}",request.form.get('issue_date'),request.form.get('due_date'),money(request.form.get('amount')),request.form.get('status'),request.form.get('description'))); return redirect(url_for('invoices'))
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
    return render_template('crm.html',leads=query_db('SELECT l.*,c.name client_name FROM crm_leads l LEFT JOIN clients c ON c.id=l.client_id ORDER BY l.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/documents',methods=['GET','POST'])
@login_required
def documents():
    if current_user.role!='admin': return render_template('documents.html',documents=query_db("SELECT *,COALESCE(document_name,name,'Document') display_name FROM documents WHERE client_id=? ORDER BY id DESC",(current_user.client_id,)),clients=[])
    return render_template('documents.html',documents=query_db("SELECT d.*,COALESCE(d.document_name,d.name,'Document') display_name,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC"),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
@app.route('/settings',methods=['GET','POST'])
@login_required
@admin_required
def settings(): return render_template('settings.html',users=query_db('SELECT u.*,cl.name client_name FROM users u LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.id DESC'),clients=query_db('SELECT id,name FROM clients ORDER BY name'))
if __name__=='__main__':
    with app.app_context(): init_db()
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',5000)))
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
    return render_template("my_invoices.html", invoices=invoices, payments=payments)


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

# === PPT CLIENT SIDE FULL MODULE REPAIR END ===

