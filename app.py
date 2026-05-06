
from __future__ import annotations
import csv, io, os, sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from flask import Flask, Response, abort, flash, g, redirect, render_template, request, send_from_directory, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR=Path(__file__).resolve().parent
INSTANCE_DIR=BASE_DIR/"instance"
UPLOAD_DIR=BASE_DIR/"static"/"uploads"
INSTANCE_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH=INSTANCE_DIR/"ppt_portal.db"
ALLOWED_UPLOADS={"pdf","png","jpg","jpeg","doc","docx","xls","xlsx","csv","txt"}
BRAND={"business_name":"Pinnacle Performance Tax and Accounting","website":"www.pinnacleperformancetax.com","email":"pinnacleperformancetax@gmail.com","phone":"478-338-1632"}

app=Flask(__name__)
app.secret_key=os.environ.get("SECRET_KEY","ppt-dev-secret-change-me")
app.config["MAX_CONTENT_LENGTH"]=25*1024*1024
login_manager=LoginManager(app)
login_manager.login_view="login"

def get_db():
    if "db" not in g:
        conn=sqlite3.connect(DB_PATH,timeout=20)
        conn.row_factory=sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        g.db=conn
    return g.db

@app.teardown_appcontext
def close_db(error=None):
    conn=g.pop("db",None)
    if conn: conn.close()

def query_db(sql,args=(),one=False):
    rows=get_db().execute(sql,args).fetchall()
    return (rows[0] if rows else None) if one else rows

def execute_db(sql,args=()):
    cur=get_db().execute(sql,args)
    get_db().commit()
    return cur.lastrowid

def money(v):
    try: return round(float(str(v or "0").replace("$","").replace(",","")),2)
    except Exception: return 0.0

@app.template_filter("currency")
def currency(v): return "${:,.2f}".format(money(v))

@app.context_processor
def inject_globals(): return {"brand":BRAND}

class User(UserMixin):
    def __init__(self,row):
        self.id=str(row["id"])
        self.name=row["name"]
        self.email=row["email"]
        self.role=row["role"]
        self.client_id=row["client_id"]

@login_manager.user_loader
def load_user(user_id):
    row=query_db("SELECT * FROM users WHERE id=? AND is_active=1",(user_id,),one=True)
    return User(row) if row else None

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args,**kwargs):
        if not current_user.is_authenticated or current_user.role!="admin":
            flash("Admin access required.","danger")
            return redirect(url_for("client_dashboard"))
        return fn(*args,**kwargs)
    return wrapper

def client_required(fn):
    @wraps(fn)
    def wrapper(*args,**kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for("login"))
        if current_user.role=="admin":
            return redirect(url_for("dashboard"))
        if not current_user.client_id:
            flash("Your user account is not linked to a client profile.","danger")
            return redirect(url_for("client_dashboard"))
        return fn(*args,**kwargs)
    return wrapper

def allowed_file(filename):
    return "." in filename and filename.rsplit(".",1)[1].lower() in ALLOWED_UPLOADS

def add_column_if_missing(table,column,definition):
    cols=[r["name"] for r in get_db().execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        get_db().execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        get_db().commit()

def dedupe_categories():
    db=get_db()
    rows=db.execute("""
        SELECT LOWER(TRIM(name)) key_name, kind, MIN(id) keep_id, COUNT(*) count
        FROM categories
        GROUP BY LOWER(TRIM(name)), kind
        HAVING COUNT(*) > 1
    """).fetchall()
    for r in rows:
        dupes=db.execute("SELECT id FROM categories WHERE LOWER(TRIM(name))=? AND kind=? AND id<>?",(r["key_name"],r["kind"],r["keep_id"])).fetchall()
        for d in dupes:
            db.execute("UPDATE transactions SET category_id=? WHERE category_id=?",(r["keep_id"],d["id"]))
            db.execute("DELETE FROM categories WHERE id=?",(d["id"],))
    db.commit()

def init_db():
    db=get_db()
    db.executescript("""
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
    """)
    migrations=[
        ("users","client_id","INTEGER"),
        ("clients","business_name","TEXT"),("clients","email","TEXT"),("clients","phone","TEXT"),("clients","address","TEXT"),("clients","client_type","TEXT DEFAULT 'Individual'"),("clients","status","TEXT DEFAULT 'Active'"),("clients","notes","TEXT"),
        ("invoices","paid_at","TEXT"),
        ("payments","method","TEXT DEFAULT 'Manual Entry'"),("payments","reference","TEXT"),("payments","client_id","INTEGER"),
        ("tax_returns","invoice_id","INTEGER"),("tax_returns","completed_at","TEXT"),
        ("documents","document_name","TEXT DEFAULT 'Document'"),("documents","name","TEXT"),("documents","filename","TEXT"),("documents","tax_year","TEXT"),("documents","status","TEXT DEFAULT 'Received'"),("documents","notes","TEXT"),("documents","category","TEXT DEFAULT 'Tax Documents'"),("documents","uploaded_by","TEXT DEFAULT 'Admin'"),("documents","reviewed_at","TEXT"),
        ("crm_leads","client_id","INTEGER"),("messages","status","TEXT DEFAULT 'Open'")
    ]
    for table,column,definition in migrations:
        add_column_if_missing(table,column,definition)

    clean_categories=[
        ("Tax Preparation Income","income"),("Bookkeeping Income","income"),("Consulting Income","income"),("Sales Income","income"),
        ("Office Supplies","expense"),("Software & Subscriptions","expense"),("Advertising & Marketing","expense"),("Meals","expense"),("Travel","expense"),("Payroll","expense"),("Contract Labor","expense"),("Bank Fees","expense"),("Professional Fees","expense"),("Vehicle & Mileage","expense"),("Rent","expense"),("Utilities","expense"),("Insurance","expense"),("Other Expense","expense")
    ]
    for name,kind in clean_categories:
        existing=db.execute("SELECT id FROM categories WHERE LOWER(TRIM(name))=LOWER(TRIM(?)) AND kind=?",(name,kind)).fetchone()
        if not existing:
            db.execute("INSERT INTO categories(name,kind) VALUES (?,?)",(name,kind))
    db.commit()
    dedupe_categories()
    try:
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_unique_name_kind ON categories(LOWER(TRIM(name)), kind)")
        db.commit()
    except Exception:
        pass

    admin_email="admin@pinnacleperformancetax.com"
    admin_pw=os.environ.get("ADMIN_PASSWORD","ChangeMe123")
    if db.execute("SELECT id FROM users WHERE lower(email)=?",(admin_email,)).fetchone():
        db.execute("UPDATE users SET name=?,password_hash=?,role='admin',is_active=1 WHERE lower(email)=?",("PPT Admin",generate_password_hash(admin_pw),admin_email))
    else:
        db.execute("INSERT INTO users(name,email,password_hash,role,is_active) VALUES (?,?,?,'admin',1)",("PPT Admin",admin_email,generate_password_hash(admin_pw)))

    if db.execute("SELECT COUNT(*) c FROM clients").fetchone()["c"]==0:
        cid=db.execute("INSERT INTO clients(name,business_name,email,phone,client_type,status,notes) VALUES (?,?,?,?,?,?,?)",("Sample Client","Sample Business LLC","client@example.com","478-555-0110","Full Service","Active","Demo client")).lastrowid
        db.execute("INSERT OR IGNORE INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,'client',?,1)",("Sample Client","client@example.com",generate_password_hash("Temp123!"),cid))
    db.commit()

@app.route("/init")
def init_route():
    init_db()
    return "INIT COMPLETE - client portal links and category duplicates fixed"

@app.route("/")
def home():
    if not current_user.is_authenticated:
        return redirect(url_for("login"))
    return redirect(url_for("dashboard") if current_user.role=="admin" else url_for("client_dashboard"))

@app.route("/login",methods=["GET","POST"])
def login():
    init_db()
    if request.method=="POST":
        email=request.form.get("email","").strip().lower()
        password=request.form.get("password","")
        row=query_db("SELECT * FROM users WHERE lower(email)=? AND is_active=1",(email,),one=True)
        if row and check_password_hash(row["password_hash"],password):
            login_user(User(row))
            return redirect(url_for("dashboard") if row["role"]=="admin" else url_for("client_dashboard"))
        flash("Invalid login.","danger")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
@admin_required
def dashboard():
    income=query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'",one=True)["total"]
    expenses=query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'",one=True)["total"]
    unpaid=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'",one=True)["total"]
    counts={
        "clients":query_db("SELECT COUNT(*) c FROM clients",one=True)["c"],
        "open_invoices":query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'",one=True)["c"],
        "documents":query_db("SELECT COUNT(*) c FROM documents",one=True)["c"],
        "returns":query_db("SELECT COUNT(*) c FROM tax_returns",one=True)["c"],
        "messages":query_db("SELECT COUNT(*) c FROM messages WHERE status='Open'",one=True)["c"],
    }
    recent_documents=query_db("SELECT d.*,COALESCE(d.document_name,d.name,'Document') display_name,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC LIMIT 8")
    open_messages=query_db("SELECT m.*,cl.name client_name FROM messages m LEFT JOIN clients cl ON cl.id=m.client_id WHERE m.status='Open' ORDER BY m.id DESC LIMIT 5")
    recent_transactions=query_db("SELECT t.*,c.name category_name,cl.name client_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id LEFT JOIN clients cl ON cl.id=t.client_id ORDER BY t.id DESC LIMIT 5")
    return render_template("dashboard.html",income=income,expenses=expenses,balance=income-expenses,unpaid=unpaid,counts=counts,recent_documents=recent_documents,open_messages=open_messages,recent_transactions=recent_transactions)

@app.route("/client")
@app.route("/client-dashboard")
@login_required
def client_dashboard():
    if current_user.role=="admin":
        return redirect(url_for("dashboard"))
    if not current_user.client_id:
        flash("Your user account is not linked to a client profile.","danger")
        return render_template("client_dashboard.html",client=None,documents=[],invoices=[],payments=[],tax_returns=[],appointments=[],messages=[],transactions=[],crm_items=[])
    cid=current_user.client_id
    client=query_db("SELECT * FROM clients WHERE id=?",(cid,),one=True)
    documents=query_db("SELECT *,COALESCE(document_name,name,'Document') display_name FROM documents WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    invoices=query_db("SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    payments=query_db("SELECT p.*,i.invoice_number FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id WHERE p.client_id=? ORDER BY p.id DESC LIMIT 50",(cid,))
    tax_returns=query_db("SELECT tr.*,i.invoice_number,i.status invoice_status FROM tax_returns tr LEFT JOIN invoices i ON i.id=tr.invoice_id WHERE tr.client_id=? ORDER BY tr.id DESC LIMIT 50",(cid,))
    appointments=query_db("SELECT * FROM appointments WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    messages=query_db("SELECT * FROM messages WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    transactions=query_db("SELECT t.*,c.name category_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? ORDER BY t.date DESC,t.id DESC LIMIT 50",(cid,))
    crm_items=query_db("SELECT * FROM crm_leads WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    return render_template("client_dashboard.html",client=client,documents=documents,invoices=invoices,payments=payments,tax_returns=tax_returns,appointments=appointments,messages=messages,transactions=transactions,crm_items=crm_items)

@app.route("/my/invoices")
@login_required
@client_required
def my_invoices():
    invoices=query_db("SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC",(current_user.client_id,))
    payments=query_db("SELECT p.*,i.invoice_number FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id WHERE p.client_id=? ORDER BY p.id DESC",(current_user.client_id,))
    return render_template("my_invoices.html",invoices=invoices,payments=payments)

@app.route("/my/payments")
@login_required
@client_required
def my_payments():
    payments=query_db("SELECT p.*,i.invoice_number FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id WHERE p.client_id=? ORDER BY p.id DESC",(current_user.client_id,))
    return render_template("my_payments.html",payments=payments)

@app.route("/my/appointments")
@login_required
@client_required
def my_appointments():
    appointments=query_db("SELECT * FROM appointments WHERE client_id=? ORDER BY id DESC",(current_user.client_id,))
    return render_template("my_appointments.html",appointments=appointments)

@app.route("/my/bookkeeping")
@login_required
@client_required
def my_bookkeeping():
    transactions=query_db("SELECT t.*,c.name category_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id WHERE t.client_id=? ORDER BY t.date DESC,t.id DESC",(current_user.client_id,))
    return render_template("my_bookkeeping.html",transactions=transactions)

@app.route("/my/tax-returns")
@login_required
@client_required
def my_tax_returns():
    returns=query_db("SELECT tr.*,i.invoice_number,i.status invoice_status FROM tax_returns tr LEFT JOIN invoices i ON i.id=tr.invoice_id WHERE tr.client_id=? ORDER BY tr.id DESC",(current_user.client_id,))
    return render_template("my_tax_returns.html",returns=returns)

@app.route("/my/crm",methods=["GET","POST"])
@login_required
@client_required
def my_crm():
    if request.method=="POST":
        execute_db("INSERT INTO crm_leads(name,phone,email,status,source,follow_up_date,notes,client_id) VALUES (?,?,?,?,?,?,?,?)",(current_user.name,"",current_user.email,"New","Client Portal",None,request.form.get("notes"),current_user.client_id))
        execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",(current_user.client_id,"client",current_user.name,request.form.get("subject") or "Client Request",request.form.get("notes") or ""))
        flash("Request sent to the office.","success")
        return redirect(url_for("my_crm"))
    crm_items=query_db("SELECT * FROM crm_leads WHERE client_id=? ORDER BY id DESC",(current_user.client_id,))
    messages=query_db("SELECT * FROM messages WHERE client_id=? ORDER BY id DESC",(current_user.client_id,))
    return render_template("my_crm.html",crm_items=crm_items,messages=messages)

@app.route("/client/upload",methods=["POST"])
@login_required
@client_required
def client_upload():
    file=request.files.get("file")
    if not file or not file.filename:
        flash("Choose a file to upload.","danger")
        return redirect(url_for("client_dashboard"))
    if not allowed_file(file.filename):
        flash("File type not allowed.","danger")
        return redirect(url_for("client_dashboard"))
    filename=f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{current_user.client_id}_{secure_filename(file.filename)}"
    file.save(UPLOAD_DIR/filename)
    doc_name=request.form.get("document_name") or request.form.get("name") or file.filename
    execute_db("INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by) VALUES (?,?,?,?,?,'Uploaded by Client',?,?, 'Client')",(current_user.client_id,doc_name,doc_name,filename,request.form.get("tax_year"),request.form.get("notes"),request.form.get("category") or "Tax Documents"))
    flash("Document uploaded successfully.","success")
    return redirect(url_for("client_dashboard"))

@app.route("/client/message",methods=["POST"])
@login_required
@client_required
def client_message():
    execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",(current_user.client_id,"client",current_user.name,request.form.get("subject") or "Client Message",request.form.get("body") or ""))
    flash("Message sent to the office.","success")
    return redirect(url_for("client_dashboard"))

@app.route("/documents/download/<int:document_id>")
@login_required
def download_document(document_id):
    doc=query_db("SELECT * FROM documents WHERE id=?",(document_id,),one=True)
    if not doc or not doc["filename"]:
        abort(404)
    if current_user.role!="admin" and doc["client_id"]!=current_user.client_id:
        abort(403)
    return send_from_directory(UPLOAD_DIR,doc["filename"],as_attachment=True)

@app.route("/documents/mark-reviewed/<int:document_id>",methods=["POST"])
@login_required
@admin_required
def mark_document_reviewed(document_id):
    execute_db("UPDATE documents SET status='Reviewed',reviewed_at=CURRENT_TIMESTAMP WHERE id=?",(document_id,))
    flash("Document marked reviewed.","success")
    return redirect(url_for("documents"))

@app.route("/documents/request/<int:client_id>",methods=["POST"])
@login_required
@admin_required
def request_document(client_id):
    doc_name=request.form.get("document_name") or "Requested Document"
    execute_db("INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by) VALUES (?,?,?,?,?,'Requested',?,?, 'Admin')",(client_id,doc_name,doc_name,"",request.form.get("tax_year"),request.form.get("notes"),request.form.get("category") or "Tax Documents"))
    flash("Document request added to client portal.","success")
    return redirect(url_for("documents"))

@app.route("/documents",methods=["GET","POST"])
@login_required
def documents():
    if request.method=="POST":
        cid=request.form.get("client_id") if current_user.role=="admin" else current_user.client_id
        file=request.files.get("file")
        filename=""
        if file and file.filename and allowed_file(file.filename):
            filename=f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{cid}_{secure_filename(file.filename)}"
            file.save(UPLOAD_DIR/filename)
        doc_name=request.form.get("document_name") or request.form.get("name") or (file.filename if file else "Document")
        execute_db("INSERT INTO documents(client_id,document_name,name,filename,tax_year,status,notes,category,uploaded_by) VALUES (?,?,?,?,?,?,?,?,?)",(cid,doc_name,doc_name,filename,request.form.get("tax_year"),request.form.get("status") or "Received",request.form.get("notes"),request.form.get("category") or "Tax Documents","Admin" if current_user.role=="admin" else "Client"))
        flash("Document saved.","success")
        return redirect(url_for("documents") if current_user.role=="admin" else url_for("client_dashboard"))
    if current_user.role=="admin":
        rows=query_db("SELECT d.*,COALESCE(d.document_name,d.name,'Document') display_name,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC LIMIT 500")
        return render_template("documents.html",documents=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))
    rows=query_db("SELECT *,COALESCE(document_name,name,'Document') display_name FROM documents WHERE client_id=? ORDER BY id DESC LIMIT 300",(current_user.client_id,))
    return render_template("documents.html",documents=rows,clients=[])

@app.route("/clients",methods=["GET","POST"])
@login_required
@admin_required
def clients():
    if request.method=="POST":
        execute_db("INSERT INTO clients(name,business_name,email,phone,address,client_type,status,notes) VALUES (?,?,?,?,?,?,?,?)",(request.form.get("name"),request.form.get("business_name"),request.form.get("email"),request.form.get("phone"),request.form.get("address"),request.form.get("client_type"),request.form.get("status"),request.form.get("notes")))
        flash("Client saved.","success")
        return redirect(url_for("clients"))
    return render_template("clients.html",clients=query_db("SELECT * FROM clients ORDER BY name"))

@app.route("/transactions",methods=["GET","POST"])
@login_required
@admin_required
def transactions():
    if request.method=="POST":
        execute_db("INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)",(request.form.get("date"),request.form.get("description"),request.form.get("type"),request.form.get("category_id") or None,request.form.get("client_id") or None,money(request.form.get("amount")),request.form.get("notes")))
        flash("Transaction added.","success")
        return redirect(url_for("transactions"))
    rows=query_db("SELECT t.*,c.name category_name,cl.name client_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id LEFT JOIN clients cl ON cl.id=t.client_id ORDER BY t.date DESC,t.id DESC LIMIT 500")
    categories=query_db("SELECT * FROM categories ORDER BY kind,name")
    clients_list=query_db("SELECT id,name FROM clients ORDER BY name")
    return render_template("transactions.html",transactions=rows,categories=categories,clients=clients_list)

@app.route("/invoices",methods=["GET","POST"])
@login_required
@admin_required
def invoices():
    if request.method=="POST":
        inv=request.form.get("invoice_number") or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        execute_db("INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,?,?)",(request.form.get("client_id"),inv,request.form.get("issue_date"),request.form.get("due_date"),money(request.form.get("amount")),request.form.get("status"),request.form.get("description")))
        flash("Invoice saved.","success")
        return redirect(url_for("invoices"))
    rows=query_db("SELECT i.*,cl.name client_name FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id ORDER BY i.id DESC LIMIT 500")
    return render_template("invoices.html",invoices=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/payments",methods=["GET","POST"])
@login_required
@admin_required
def payments():
    init_db()
    if request.method=="POST":
        invoice=query_db("SELECT * FROM invoices WHERE id=?",(request.form.get("invoice_id"),),one=True)
        if not invoice:
            flash("Select a valid invoice.","danger")
            return redirect(url_for("payments"))
        amount=money(request.form.get("amount")) or money(invoice["amount"])
        execute_db("INSERT INTO payments(invoice_id,client_id,amount,method,reference,status,notes) VALUES (?,?,?,?,?,'Paid',?)",(invoice["id"],invoice["client_id"],amount,request.form.get("method") or "Manual Entry",request.form.get("reference"),request.form.get("notes")))
        execute_db("UPDATE invoices SET status='Paid',paid_at=CURRENT_TIMESTAMP WHERE id=?",(invoice["id"],))
        linked=query_db("SELECT id FROM tax_returns WHERE invoice_id=? LIMIT 1",(invoice["id"],),one=True)
        if linked:
            execute_db("UPDATE tax_returns SET status='Completed',completed_at=CURRENT_TIMESTAMP WHERE id=?",(linked["id"],))
        cat=query_db("SELECT id FROM categories WHERE name='Tax Preparation Income' AND kind='income'",one=True)
        if not query_db("SELECT id FROM transactions WHERE notes=? LIMIT 1",(f"Payment record for invoice #{invoice['id']}",),one=True):
            execute_db("INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,'income',?,?,?,?)",(datetime.now().strftime("%Y-%m-%d"),f"Payment received for invoice {invoice['invoice_number'] or invoice['id']}",cat["id"] if cat else None,invoice["client_id"],amount,f"Payment record for invoice #{invoice['id']}"))
        flash("Payment recorded. Invoice marked paid. Linked tax return completed. Income added.","success")
        return redirect(url_for("payments"))
    invoice_rows=query_db("SELECT i.*,cl.name client_name,tr.id tax_return_id FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id LEFT JOIN tax_returns tr ON tr.invoice_id=i.id ORDER BY i.status='Paid',i.id DESC LIMIT 500")
    rows=query_db("SELECT p.*,i.invoice_number,cl.name client_name FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id LEFT JOIN clients cl ON cl.id=p.client_id ORDER BY p.id DESC LIMIT 200")
    return render_template("payments.html",invoices=invoice_rows,payments=rows,paid_total=query_db("SELECT COALESCE(SUM(amount),0) total FROM payments WHERE status='Paid'",one=True)["total"],unpaid_total=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'",one=True)["total"],paid_invoice_count=query_db("SELECT COUNT(*) c FROM invoices WHERE status='Paid'",one=True)["c"],unpaid_invoice_count=query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'",one=True)["c"])

@app.route("/tax-returns",methods=["GET","POST"])
@app.route("/tax_returns",methods=["GET","POST"])
@login_required
@admin_required
def tax_returns():
    init_db()
    if request.method=="POST":
        cid=request.form.get("client_id")
        year=request.form.get("tax_year")
        service=request.form.get("service_type")
        fee=money(request.form.get("fee"))
        due=request.form.get("due_date")
        inv=f"TR-{year}-{datetime.now().strftime('%H%M%S')}"
        invoice_id=execute_db("INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,'Sent',?)",(cid,inv,datetime.now().strftime("%Y-%m-%d"),due,fee,f"Tax return service: {service or 'Tax Return'} for {year}"))
        execute_db("INSERT INTO tax_returns(client_id,tax_year,service_type,status,due_date,fee,notes,invoice_id) VALUES (?,?,?,?,?,?,?,?)",(cid,year,service,request.form.get("status") or "In Progress",due,fee,request.form.get("notes"),invoice_id))
        flash("Tax return saved and invoice auto-created.","success")
        return redirect(url_for("tax_returns"))
    rows=query_db("SELECT tr.*,cl.name client_name,i.invoice_number,i.status invoice_status FROM tax_returns tr LEFT JOIN clients cl ON cl.id=tr.client_id LEFT JOIN invoices i ON i.id=tr.invoice_id ORDER BY tr.id DESC LIMIT 500")
    return render_template("tax_returns.html",returns=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/appointments",methods=["GET","POST"])
@login_required
@admin_required
def appointments():
    if request.method=="POST":
        execute_db("INSERT INTO appointments(client_id,title,start_at,end_at,location,meeting_link,status,notes) VALUES (?,?,?,?,?,?,?,?)",(request.form.get("client_id"),request.form.get("title"),request.form.get("start_at"),request.form.get("end_at"),request.form.get("location"),request.form.get("meeting_link"),request.form.get("status"),request.form.get("notes")))
        flash("Appointment saved.","success")
        return redirect(url_for("appointments"))
    rows=query_db("SELECT a.*,cl.name client_name FROM appointments a LEFT JOIN clients cl ON cl.id=a.client_id ORDER BY a.id DESC LIMIT 500")
    return render_template("appointments.html",appointments=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/crm",methods=["GET","POST"])
@login_required
@admin_required
def crm():
    if request.method=="POST":
        execute_db("INSERT INTO crm_leads(name,phone,email,status,source,follow_up_date,notes,client_id) VALUES (?,?,?,?,?,?,?,?)",(request.form.get("name"),request.form.get("phone"),request.form.get("email"),request.form.get("status"),request.form.get("source"),request.form.get("follow_up_date"),request.form.get("notes"),request.form.get("client_id") or None))
        flash("Lead saved.","success")
        return redirect(url_for("crm"))
    return render_template("crm.html",leads=query_db("SELECT l.*,c.name client_name FROM crm_leads l LEFT JOIN clients c ON c.id=l.client_id ORDER BY l.id DESC LIMIT 500"),clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/messages",methods=["GET","POST"])
@login_required
@admin_required
def messages():
    if request.method=="POST":
        execute_db("INSERT INTO messages(client_id,sender_role,sender_name,subject,body,status) VALUES (?,?,?,?,?,'Open')",(request.form.get("client_id"),"admin",current_user.name,request.form.get("subject") or "Office Message",request.form.get("body") or ""))
        flash("Message added to client portal.","success")
        return redirect(url_for("messages"))
    rows=query_db("SELECT m.*,cl.name client_name FROM messages m LEFT JOIN clients cl ON cl.id=m.client_id ORDER BY m.id DESC LIMIT 500")
    return render_template("messages.html",messages=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/messages/close/<int:message_id>",methods=["POST"])
@login_required
@admin_required
def close_message(message_id):
    execute_db("UPDATE messages SET status='Closed' WHERE id=?",(message_id,))
    flash("Message closed.","success")
    return redirect(url_for("messages"))

@app.route("/settings",methods=["GET","POST"])
@login_required
@admin_required
def settings():
    if request.method=="POST":
        email=request.form.get("email","").lower().strip()
        password_hash=generate_password_hash(request.form.get("password") or "Temp123!")
        if query_db("SELECT id FROM users WHERE lower(email)=?",(email,),one=True):
            execute_db("UPDATE users SET name=?,password_hash=?,role=?,client_id=?,is_active=1 WHERE lower(email)=?",(request.form.get("name"),password_hash,request.form.get("role") or "client",request.form.get("client_id") or None,email))
            flash("Existing user updated.","success")
        else:
            execute_db("INSERT INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,?,?,1)",(request.form.get("name"),email,password_hash,request.form.get("role") or "client",request.form.get("client_id") or None))
            flash("User created.","success")
        return redirect(url_for("settings"))
    users=query_db("SELECT u.*,cl.name client_name FROM users u LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.id DESC")
    return render_template("settings.html",users=users,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

def report_filters():
    return request.args.get("client_id") or "", request.args.get("year") or str(datetime.now().year), request.args.get("month") or ""

def where_for_transactions(alias="t"):
    cid,year,month=report_filters()
    cond=[]
    args=[]
    if cid:
        cond.append(f"{alias}.client_id=?")
        args.append(cid)
    if year:
        cond.append(f"substr({alias}.date,1,4)=?")
        args.append(year)
    if month:
        cond.append(f"substr({alias}.date,6,2)=?")
        args.append(month.zfill(2))
    return (" WHERE "+" AND ".join(cond)) if cond else "", tuple(args)

@app.route("/reports")
@login_required
@admin_required
def reports():
    cid,year,month=report_filters()
    where,args=where_for_transactions("t")
    income=query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions t {where + (' AND' if where else ' WHERE')} t.type='income'",args,one=True)["total"]
    expenses=query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions t {where + (' AND' if where else ' WHERE')} t.type='expense'",args,one=True)["total"]
    by_category=query_db(f"SELECT COALESCE(c.name,'Uncategorized') category,t.type,COALESCE(SUM(t.amount),0) total FROM transactions t LEFT JOIN categories c ON c.id=t.category_id {where} GROUP BY COALESCE(c.name,'Uncategorized'),t.type ORDER BY t.type,total DESC",args)
    transactions_rows=query_db(f"SELECT t.*,cl.name client_name,c.name category_name FROM transactions t LEFT JOIN clients cl ON cl.id=t.client_id LEFT JOIN categories c ON c.id=t.category_id {where} ORDER BY t.date DESC,t.id DESC LIMIT 1000",args)
    return render_template("reports.html",clients=query_db("SELECT id,name FROM clients ORDER BY name"),selected_client=cid,selected_year=year,selected_month=month,income=income,expenses=expenses,profit=money(income)-money(expenses),by_category=by_category,transactions=transactions_rows,client_revenue=query_db("SELECT cl.name client_name,COALESCE(SUM(p.amount),0) total FROM payments p LEFT JOIN clients cl ON cl.id=p.client_id GROUP BY cl.name ORDER BY total DESC LIMIT 25"),doc_counts=query_db("SELECT category,status,COUNT(*) count FROM documents GROUP BY category,status ORDER BY category,status"),tax_summary=query_db("SELECT status,COUNT(*) count,COALESCE(SUM(fee),0) total FROM tax_returns GROUP BY status ORDER BY status"))

@app.route("/reports/export/transactions.csv")
@login_required
@admin_required
def export_transactions():
    where,args=where_for_transactions("t")
    rows=query_db(f"SELECT t.date,cl.name client,t.description,t.type,c.name category,t.amount,t.notes FROM transactions t LEFT JOIN clients cl ON cl.id=t.client_id LEFT JOIN categories c ON c.id=t.category_id {where} ORDER BY t.date DESC,t.id DESC",args)
    output=io.StringIO()
    w=csv.writer(output)
    w.writerow(["Date","Client","Description","Type","Category","Amount","Notes"])
    for r in rows:
        w.writerow([r["date"],r["client"],r["description"],r["type"],r["category"],r["amount"],r["notes"]])
    return Response(output.getvalue(),mimetype="text/csv",headers={"Content-Disposition":"attachment; filename=ppt_year_end_transactions.csv"})

@app.route("/reports/export/client-documents.csv")
@login_required
@admin_required
def export_documents():
    rows=query_db("SELECT cl.name client,COALESCE(d.document_name,d.name,'Document') document,d.category,d.tax_year,d.status,d.created_at FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY cl.name,d.created_at DESC")
    output=io.StringIO()
    w=csv.writer(output)
    w.writerow(["Client","Document","Category","Tax Year","Status","Uploaded"])
    for r in rows:
        w.writerow([r["client"],r["document"],r["category"],r["tax_year"],r["status"],r["created_at"]])
    return Response(output.getvalue(),mimetype="text/csv",headers={"Content-Disposition":"attachment; filename=ppt_document_tracker.csv"})

if __name__=="__main__":
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0",port=int(os.environ.get("PORT",5000)))
