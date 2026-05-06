
from __future__ import annotations
import csv, io, os, sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any
from flask import Flask, Response, abort, flash, g, redirect, render_template, request, send_from_directory, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
INSTANCE_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = INSTANCE_DIR / "ppt_portal.db"
ALLOWED_UPLOADS = {"pdf","png","jpg","jpeg","doc","docx","xls","xlsx","csv","txt"}
BRAND = {"business_name":"Pinnacle Performance Tax and Accounting","website":"www.pinnacleperformancetax.com","email":"pinnacleperformancetax@gmail.com","phone":"478-338-1632"}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-secret-key")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024
login_manager = LoginManager(app)
login_manager.login_view = "login"

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        g.db = conn
    return g.db

@app.teardown_appcontext
def close_db(error=None):
    conn = g.pop("db", None)
    if conn: conn.close()

def query_db(sql: str, args: tuple[Any, ...] = (), one: bool = False):
    rows = get_db().execute(sql, args).fetchall()
    return (rows[0] if rows else None) if one else rows

def execute_db(sql: str, args: tuple[Any, ...] = ()) -> int:
    cur = get_db().execute(sql, args)
    get_db().commit()
    return cur.lastrowid

def money(value: Any) -> float:
    try: return round(float(str(value or "0").replace("$","").replace(",","")), 2)
    except Exception: return 0.0

@app.template_filter("currency")
def currency(value): return "${:,.2f}".format(money(value))

@app.context_processor
def inject_globals(): return {"brand": BRAND}

class User(UserMixin):
    def __init__(self, row):
        self.id = str(row["id"]); self.name = row["name"]; self.email = row["email"]; self.role = row["role"]; self.client_id = row["client_id"]

@login_manager.user_loader
def load_user(user_id: str):
    row = query_db("SELECT * FROM users WHERE id=? AND is_active=1", (user_id,), one=True)
    return User(row) if row else None

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            flash("Admin access required.", "danger")
            return redirect(url_for("client_dashboard"))
        return fn(*args, **kwargs)
    return wrapper

def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".",1)[1].lower() in ALLOWED_UPLOADS

def add_column_if_missing(table: str, column: str, definition: str) -> None:
    cols = [r["name"] for r in get_db().execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        get_db().execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        get_db().commit()

def init_db() -> None:
    db = get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,business_name TEXT,email TEXT,phone TEXT,address TEXT,client_type TEXT DEFAULT 'Individual',status TEXT DEFAULT 'Active',notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,role TEXT DEFAULT 'client',client_id INTEGER,is_active INTEGER DEFAULT 1,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,kind TEXT NOT NULL,UNIQUE(name,kind));
    CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT,date TEXT NOT NULL,description TEXT NOT NULL,type TEXT NOT NULL,category_id INTEGER,client_id INTEGER,amount REAL NOT NULL,notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS invoices (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,invoice_number TEXT,issue_date TEXT,due_date TEXT,amount REAL DEFAULT 0,status TEXT DEFAULT 'Draft',description TEXT,paid_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS payments (id INTEGER PRIMARY KEY AUTOINCREMENT,invoice_id INTEGER,client_id INTEGER,amount REAL DEFAULT 0,method TEXT DEFAULT 'Manual Entry',reference TEXT,status TEXT DEFAULT 'Paid',notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,name TEXT,filename TEXT,tax_year TEXT,status TEXT DEFAULT 'Received',notes TEXT,category TEXT DEFAULT 'Tax Documents',uploaded_by TEXT DEFAULT 'Admin',reviewed_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS tax_returns (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,tax_year TEXT,service_type TEXT,status TEXT DEFAULT 'In Progress',due_date TEXT,fee REAL DEFAULT 0,notes TEXT,invoice_id INTEGER,completed_at TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS appointments (id INTEGER PRIMARY KEY AUTOINCREMENT,client_id INTEGER,title TEXT,start_at TEXT,end_at TEXT,location TEXT,meeting_link TEXT,status TEXT DEFAULT 'Scheduled',notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS crm_leads (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,phone TEXT,email TEXT,status TEXT DEFAULT 'New',source TEXT,follow_up_date TEXT,notes TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status);
    CREATE INDEX IF NOT EXISTS idx_invoices_client ON invoices(client_id);
    CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
    CREATE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id);
    CREATE INDEX IF NOT EXISTS idx_documents_client ON documents(client_id);
    CREATE INDEX IF NOT EXISTS idx_tax_returns_client ON tax_returns(client_id);
    """)
    for table, column, definition in [
        ("documents","name","TEXT"),
        ("documents", "filename", "TEXT"),
        ("users","client_id","INTEGER"),("invoices","paid_at","TEXT"),("tax_returns","invoice_id","INTEGER"),("tax_returns","completed_at","TEXT"),
        ("payments","method","TEXT DEFAULT 'Manual Entry'"),("payments","reference","TEXT"),("payments","client_id","INTEGER"),
        ("documents","category","TEXT DEFAULT 'Tax Documents'"),("documents","uploaded_by","TEXT DEFAULT 'Admin'"),("documents","reviewed_at","TEXT")
    ]: add_column_if_missing(table, column, definition)
    cats=[("Tax Preparation Income","income"),("Bookkeeping Income","income"),("Consulting Income","income"),("Sales Income","income"),("Advertising & Marketing","expense"),("Meals","expense"),("Office Supplies","expense"),("Software & Subscriptions","expense"),("Travel","expense"),("Payroll","expense"),("Contract Labor","expense"),("Rent","expense"),("Utilities","expense"),("Insurance","expense"),("Bank Fees","expense"),("Professional Fees","expense"),("Vehicle & Mileage","expense"),("Repairs & Maintenance","expense"),("Other Expense","expense")]
    for name,kind in cats: db.execute("INSERT OR IGNORE INTO categories(name,kind) VALUES (?,?)",(name,kind))
    admin_email="admin@pinnacleperformancetax.com"; admin_pw=os.environ.get("ADMIN_PASSWORD","ChangeMe123")
    if db.execute("SELECT id FROM users WHERE lower(email)=?",(admin_email,)).fetchone():
        db.execute("UPDATE users SET name=?,password_hash=?,role='admin',is_active=1 WHERE lower(email)=?",("PPT Admin",generate_password_hash(admin_pw),admin_email))
    else:
        db.execute("INSERT INTO users(name,email,password_hash,role,is_active) VALUES (?,?,?,'admin',1)",("PPT Admin",admin_email,generate_password_hash(admin_pw)))
    if db.execute("SELECT COUNT(*) c FROM clients").fetchone()["c"] == 0:
        cid=db.execute("INSERT INTO clients(name,business_name,email,phone,client_type,status,notes) VALUES (?,?,?,?,?,?,?)",("Sample Client","Sample Business LLC","client@example.com","478-555-0110","Business","Active","Demo client")).lastrowid
        db.execute("INSERT OR IGNORE INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,'client',?,1)",("Sample Client","client@example.com",generate_password_hash("Temp123!"),cid))
    db.commit()

@app.route("/init")
def init_route(): init_db(); return "INIT COMPLETE"

@app.route("/")
def home():
    if not current_user.is_authenticated: return redirect(url_for("login"))
    return redirect(url_for("dashboard") if current_user.role=="admin" else url_for("client_dashboard"))

@app.route("/login", methods=["GET","POST"])
def login():
    init_db()
    if request.method=="POST":
        email=request.form.get("email","").strip().lower(); password=request.form.get("password","")
        row=query_db("SELECT * FROM users WHERE lower(email)=? AND is_active=1",(email,),one=True)
        if row and check_password_hash(row["password_hash"], password):
            login_user(User(row)); return redirect(url_for("dashboard") if row["role"]=="admin" else url_for("client_dashboard"))
        flash("Invalid login.","danger")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout(): logout_user(); return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
@admin_required
def dashboard():
    income=query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'",one=True)["total"]
    expenses=query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'",one=True)["total"]
    unpaid=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'",one=True)["total"]
    counts={"clients":query_db("SELECT COUNT(*) c FROM clients",one=True)["c"],"open_invoices":query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'",one=True)["c"],"documents":query_db("SELECT COUNT(*) c FROM documents",one=True)["c"],"returns":query_db("SELECT COUNT(*) c FROM tax_returns",one=True)["c"]}
    recent_transactions=query_db("SELECT t.*,c.name category_name,cl.name client_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id LEFT JOIN clients cl ON cl.id=t.client_id ORDER BY t.id DESC LIMIT 5")
    recent_documents=query_db("SELECT d.*,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC LIMIT 5")
    return render_template("dashboard.html",income=income,expenses=expenses,balance=income-expenses,unpaid=unpaid,counts=counts,recent_transactions=recent_transactions,recent_documents=recent_documents)

@app.route("/client")
@app.route("/client-dashboard")
@login_required
def client_dashboard():
    if current_user.role=="admin": return redirect(url_for("dashboard"))
    user=query_db("SELECT * FROM users WHERE id=?",(current_user.id,),one=True)
    if not user or not user["client_id"]:
        flash("Your user account is not linked to a client profile yet.","danger")
        return render_template("client_dashboard.html",client=None,documents=[],invoices=[],payments=[],tax_returns=[])
    cid=user["client_id"]
    client=query_db("SELECT * FROM clients WHERE id=?",(cid,),one=True)
    documents=query_db("SELECT * FROM documents WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    invoices=query_db("SELECT * FROM invoices WHERE client_id=? ORDER BY id DESC LIMIT 50",(cid,))
    payments=query_db("SELECT p.*,i.invoice_number FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id WHERE p.client_id=? ORDER BY p.id DESC LIMIT 50",(cid,))
    tax_returns=query_db("SELECT tr.*,i.invoice_number,i.status invoice_status FROM tax_returns tr LEFT JOIN invoices i ON i.id=tr.invoice_id WHERE tr.client_id=? ORDER BY tr.id DESC LIMIT 50",(cid,))
    return render_template("client_dashboard.html",client=client,documents=documents,invoices=invoices,payments=payments,tax_returns=tax_returns)

@app.route("/client/upload", methods=["POST"])
@login_required
def client_upload():
    if current_user.role=="admin": flash("Admin uploads should be handled from Documents.","danger"); return redirect(url_for("documents"))
    if not current_user.client_id: flash("Your account is not linked to a client profile.","danger"); return redirect(url_for("client_dashboard"))
    file=request.files.get("file")
    if not file or not file.filename: flash("Choose a file to upload.","danger"); return redirect(url_for("client_dashboard"))
    if not allowed_file(file.filename): flash("File type not allowed.","danger"); return redirect(url_for("client_dashboard"))
    filename=f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{current_user.client_id}_{secure_filename(file.filename)}"
    file.save(UPLOAD_DIR/filename)
    execute_db("INSERT INTO documents(client_id,name,filename,tax_year,status,notes,category,uploaded_by) VALUES (?,?,?,?,?,?,?,'Client')",(current_user.client_id,request.form.get("name") or file.filename,filename,request.form.get("tax_year"),"Uploaded by Client",request.form.get("notes"),request.form.get("category") or "Tax Documents"))
    flash("Document uploaded successfully.","success")
    return redirect(url_for("client_dashboard"))

@app.route("/documents/download/<int:document_id>")
@login_required
def download_document(document_id:int):
    doc=query_db("SELECT * FROM documents WHERE id=?",(document_id,),one=True)
    if not doc or not doc["filename"]: abort(404)
    if current_user.role!="admin" and doc["client_id"]!=current_user.client_id: abort(403)
    return send_from_directory(UPLOAD_DIR,doc["filename"],as_attachment=True)

@app.route("/documents/mark-reviewed/<int:document_id>", methods=["POST"])
@login_required
@admin_required
def mark_document_reviewed(document_id:int):
    execute_db("UPDATE documents SET status='Reviewed',reviewed_at=CURRENT_TIMESTAMP WHERE id=?",(document_id,))
    flash("Document marked reviewed.","success"); return redirect(url_for("documents"))

@app.route("/clients", methods=["GET","POST"])
@login_required
@admin_required
def clients():
    if request.method=="POST":
        execute_db("INSERT INTO clients(name,business_name,email,phone,address,client_type,status,notes) VALUES (?,?,?,?,?,?,?,?)",(request.form.get("name"),request.form.get("business_name"),request.form.get("email"),request.form.get("phone"),request.form.get("address"),request.form.get("client_type"),request.form.get("status"),request.form.get("notes")))
        flash("Client saved.","success"); return redirect(url_for("clients"))
    return render_template("clients.html",clients=query_db("SELECT * FROM clients ORDER BY name"))

@app.route("/transactions", methods=["GET","POST"])
@login_required
def transactions():
    if current_user.role!="admin": return redirect(url_for("client_dashboard"))
    if request.method=="POST":
        execute_db("INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,?,?,?,?,?)",(request.form.get("date"),request.form.get("description"),request.form.get("type"),request.form.get("category_id") or None,request.form.get("client_id") or None,money(request.form.get("amount")),request.form.get("notes")))
        flash("Transaction added.","success"); return redirect(url_for("transactions"))
    rows=query_db("SELECT t.*,c.name category_name,cl.name client_name FROM transactions t LEFT JOIN categories c ON c.id=t.category_id LEFT JOIN clients cl ON cl.id=t.client_id ORDER BY t.date DESC,t.id DESC LIMIT 300")
    return render_template("transactions.html",transactions=rows,categories=query_db("SELECT * FROM categories ORDER BY kind,name"),clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/invoices", methods=["GET","POST"])
@login_required
def invoices():
    if current_user.role!="admin": return redirect(url_for("client_dashboard"))
    if request.method=="POST":
        inv=request.form.get("invoice_number") or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        execute_db("INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,?,?)",(request.form.get("client_id"),inv,request.form.get("issue_date"),request.form.get("due_date"),money(request.form.get("amount")),request.form.get("status"),request.form.get("description")))
        flash("Invoice saved.","success"); return redirect(url_for("invoices"))
    rows=query_db("SELECT i.*,cl.name client_name FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id ORDER BY i.id DESC LIMIT 300")
    return render_template("invoices.html",invoices=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/payments", methods=["GET","POST"])
@login_required
@admin_required
def payments():
    init_db()
    if request.method=="POST":
        invoice=query_db("SELECT * FROM invoices WHERE id=?",(request.form.get("invoice_id"),),one=True)
        if not invoice: flash("Select a valid invoice.","danger"); return redirect(url_for("payments"))
        amount=money(request.form.get("amount")) or money(invoice["amount"])
        execute_db("INSERT INTO payments(invoice_id,client_id,amount,method,reference,status,notes) VALUES (?,?,?,?,?,'Paid',?)",(invoice["id"],invoice["client_id"],amount,request.form.get("method") or "Manual Entry",request.form.get("reference"),request.form.get("notes")))
        execute_db("UPDATE invoices SET status='Paid',paid_at=CURRENT_TIMESTAMP WHERE id=?",(invoice["id"],))
        linked=query_db("SELECT id FROM tax_returns WHERE invoice_id=? LIMIT 1",(invoice["id"],),one=True)
        if linked: execute_db("UPDATE tax_returns SET status='Completed',completed_at=CURRENT_TIMESTAMP WHERE id=?",(linked["id"],))
        cat=query_db("SELECT id FROM categories WHERE name='Tax Preparation Income' AND kind='income'",one=True)
        if not query_db("SELECT id FROM transactions WHERE notes=? LIMIT 1",(f"Payment record for invoice #{invoice['id']}",),one=True):
            execute_db("INSERT INTO transactions(date,description,type,category_id,client_id,amount,notes) VALUES (?,?,'income',?,?,?,?)",(datetime.now().strftime("%Y-%m-%d"),f"Payment received for invoice {invoice['invoice_number'] or invoice['id']}",cat["id"] if cat else None,invoice["client_id"],amount,f"Payment record for invoice #{invoice['id']}"))
        flash("Payment recorded. Invoice marked paid. Linked tax return completed. Income added.","success"); return redirect(url_for("payments"))
    invoices=query_db("SELECT i.*,cl.name client_name,tr.id tax_return_id FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id LEFT JOIN tax_returns tr ON tr.invoice_id=i.id ORDER BY i.status='Paid',i.id DESC LIMIT 300")
    rows=query_db("SELECT p.*,i.invoice_number,cl.name client_name FROM payments p LEFT JOIN invoices i ON i.id=p.invoice_id LEFT JOIN clients cl ON cl.id=p.client_id ORDER BY p.id DESC LIMIT 100")
    return render_template("payments.html",invoices=invoices,payments=rows,paid_total=query_db("SELECT COALESCE(SUM(amount),0) total FROM payments WHERE status='Paid'",one=True)["total"],unpaid_total=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'",one=True)["total"],paid_invoice_count=query_db("SELECT COUNT(*) c FROM invoices WHERE status='Paid'",one=True)["c"],unpaid_invoice_count=query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'",one=True)["c"])

@app.route("/documents", methods=["GET","POST"])
@login_required
def documents():
    if request.method=="POST":
        cid=request.form.get("client_id") if current_user.role=="admin" else current_user.client_id
        uploaded_by="Admin" if current_user.role=="admin" else "Client"
        file=request.files.get("file"); filename=""
        if file and file.filename and allowed_file(file.filename):
            filename=f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{cid}_{secure_filename(file.filename)}"; file.save(UPLOAD_DIR/filename)
        execute_db("INSERT INTO documents(client_id,name,filename,tax_year,status,notes,category,uploaded_by) VALUES (?,?,?,?,?,?,?,?)",(cid,request.form.get("name"),filename,request.form.get("tax_year"),request.form.get("status") or "Received",request.form.get("notes"),request.form.get("category") or "Tax Documents",uploaded_by))
        flash("Document saved.","success"); return redirect(url_for("documents") if current_user.role=="admin" else url_for("client_dashboard"))
    if current_user.role=="admin":
        rows=query_db("SELECT d.*,cl.name client_name FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id ORDER BY d.id DESC LIMIT 300")
        return render_template("documents.html",documents=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))
    return render_template("documents.html",documents=query_db("SELECT * FROM documents WHERE client_id=? ORDER BY id DESC LIMIT 300",(current_user.client_id,)),clients=[])

@app.route("/tax-returns", methods=["GET","POST"])
@app.route("/tax_returns", methods=["GET","POST"])
@login_required
def tax_returns():
    init_db()
    if current_user.role!="admin": return redirect(url_for("client_dashboard"))
    if request.method=="POST":
        cid=request.form.get("client_id"); year=request.form.get("tax_year"); service=request.form.get("service_type"); fee=money(request.form.get("fee")); due=request.form.get("due_date")
        inv=f"TR-{year}-{datetime.now().strftime('%H%M%S')}"
        invoice_id=execute_db("INSERT INTO invoices(client_id,invoice_number,issue_date,due_date,amount,status,description) VALUES (?,?,?,?,?,'Sent',?)",(cid,inv,datetime.now().strftime("%Y-%m-%d"),due,fee,f"Tax return service: {service or 'Tax Return'} for {year}"))
        execute_db("INSERT INTO tax_returns(client_id,tax_year,service_type,status,due_date,fee,notes,invoice_id) VALUES (?,?,?,?,?,?,?,?)",(cid,year,service,request.form.get("status") or "In Progress",due,fee,request.form.get("notes"),invoice_id))
        flash("Tax return saved and invoice auto-created.","success"); return redirect(url_for("tax_returns"))
    rows=query_db("SELECT tr.*,cl.name client_name,i.invoice_number,i.status invoice_status FROM tax_returns tr LEFT JOIN clients cl ON cl.id=tr.client_id LEFT JOIN invoices i ON i.id=tr.invoice_id ORDER BY tr.id DESC LIMIT 300")
    return render_template("tax_returns.html",returns=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/appointments", methods=["GET","POST"])
@login_required
def appointments():
    if current_user.role!="admin": return redirect(url_for("client_dashboard"))
    if request.method=="POST":
        execute_db("INSERT INTO appointments(client_id,title,start_at,end_at,location,meeting_link,status,notes) VALUES (?,?,?,?,?,?,?,?)",(request.form.get("client_id"),request.form.get("title"),request.form.get("start_at"),request.form.get("end_at"),request.form.get("location"),request.form.get("meeting_link"),request.form.get("status"),request.form.get("notes")))
        flash("Appointment saved.","success"); return redirect(url_for("appointments"))
    rows=query_db("SELECT a.*,cl.name client_name FROM appointments a LEFT JOIN clients cl ON cl.id=a.client_id ORDER BY a.id DESC LIMIT 300")
    return render_template("appointments.html",appointments=rows,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

@app.route("/crm", methods=["GET","POST"])
@login_required
@admin_required
def crm():
    if request.method=="POST":
        execute_db("INSERT INTO crm_leads(name,phone,email,status,source,follow_up_date,notes) VALUES (?,?,?,?,?,?,?)",(request.form.get("name"),request.form.get("phone"),request.form.get("email"),request.form.get("status"),request.form.get("source"),request.form.get("follow_up_date"),request.form.get("notes")))
        flash("Lead saved.","success"); return redirect(url_for("crm"))
    return render_template("crm.html",leads=query_db("SELECT * FROM crm_leads ORDER BY id DESC LIMIT 300"))

@app.route("/settings", methods=["GET","POST"])
@login_required
@admin_required
def settings():
    if request.method=="POST":
        email=request.form.get("email","").lower().strip()
        vals=(request.form.get("name"),generate_password_hash(request.form.get("password") or "Temp123!"),request.form.get("role") or "client",request.form.get("client_id") or None,email)
        if query_db("SELECT id FROM users WHERE lower(email)=?",(email,),one=True):
            execute_db("UPDATE users SET name=?,password_hash=?,role=?,client_id=?,is_active=1 WHERE lower(email)=?",vals); flash("Existing user updated.","success")
        else:
            execute_db("INSERT INTO users(name,email,password_hash,role,client_id,is_active) VALUES (?,?,?,?,?,1)",(request.form.get("name"),email,vals[1],vals[2],vals[3])); flash("User created.","success")
        return redirect(url_for("settings"))
    users=query_db("SELECT u.*,cl.name client_name FROM users u LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.id DESC")
    return render_template("settings.html",users=users,clients=query_db("SELECT id,name FROM clients ORDER BY name"))

def filters():
    return request.args.get("client_id") or "", request.args.get("year") or str(datetime.now().year), request.args.get("month") or ""

def where_for_transactions(alias="t"):
    cid,year,month=filters(); cond=[]; args=[]
    if cid: cond.append(f"{alias}.client_id=?"); args.append(cid)
    if year: cond.append(f"substr({alias}.date,1,4)=?"); args.append(year)
    if month: cond.append(f"substr({alias}.date,6,2)=?"); args.append(month.zfill(2))
    return (" WHERE "+" AND ".join(cond)) if cond else "", tuple(args)

@app.route("/reports")
@login_required
@admin_required
def reports():
    cid,year,month=filters(); where,args=where_for_transactions("t")
    income=query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions t {where + (' AND' if where else ' WHERE')} t.type='income'",args,one=True)["total"]
    expenses=query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions t {where + (' AND' if where else ' WHERE')} t.type='expense'",args,one=True)["total"]
    by_category=query_db(f"SELECT COALESCE(c.name,'Uncategorized') category,t.type,COALESCE(SUM(t.amount),0) total FROM transactions t LEFT JOIN categories c ON c.id=t.category_id {where} GROUP BY COALESCE(c.name,'Uncategorized'),t.type ORDER BY t.type,total DESC",args)
    transactions_rows=query_db(f"SELECT t.*,cl.name client_name,c.name category_name FROM transactions t LEFT JOIN clients cl ON cl.id=t.client_id LEFT JOIN categories c ON c.id=t.category_id {where} ORDER BY t.date DESC,t.id DESC LIMIT 500",args)
    client_revenue=query_db("SELECT cl.name client_name,COALESCE(SUM(p.amount),0) total FROM payments p LEFT JOIN clients cl ON cl.id=p.client_id GROUP BY cl.name ORDER BY total DESC LIMIT 25")
    return render_template("reports.html",clients=query_db("SELECT id,name FROM clients ORDER BY name"),selected_client=cid,selected_year=year,selected_month=month,income=income,expenses=expenses,profit=money(income)-money(expenses),by_category=by_category,transactions=transactions_rows,client_revenue=client_revenue,doc_counts=query_db("SELECT category,status,COUNT(*) count FROM documents GROUP BY category,status ORDER BY category,status"),tax_summary=query_db("SELECT status,COUNT(*) count,COALESCE(SUM(fee),0) total FROM tax_returns GROUP BY status ORDER BY status"))

@app.route("/reports/export/transactions.csv")
@login_required
@admin_required
def export_transactions():
    where,args=where_for_transactions("t")
    rows=query_db(f"SELECT t.date,cl.name client,t.description,t.type,c.name category,t.amount,t.notes FROM transactions t LEFT JOIN clients cl ON cl.id=t.client_id LEFT JOIN categories c ON c.id=t.category_id {where} ORDER BY t.date DESC,t.id DESC",args)
    output=io.StringIO(); w=csv.writer(output); w.writerow(["Date","Client","Description","Type","Category","Amount","Notes"])
    for r in rows: w.writerow([r["date"],r["client"],r["description"],r["type"],r["category"],r["amount"],r["notes"]])
    return Response(output.getvalue(),mimetype="text/csv",headers={"Content-Disposition":"attachment; filename=ppt_year_end_transactions.csv"})

if __name__ == "__main__":
    with app.app_context(): init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
