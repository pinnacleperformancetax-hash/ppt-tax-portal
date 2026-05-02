from __future__ import annotations

import csv
import io
import os
import sqlite3
from functools import wraps
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from urllib.parse import urlparse

from flask import Flask, g, render_template, request, redirect, url_for, flash, send_file, abort
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
DB_PATH = INSTANCE_DIR / "ppt_portal.db"
ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "xls", "xlsx", "csv"}

BRAND = {
    "app_name": "PPT Bookkeeping & Tax Portal Pro",
    "business_name": "Pinnacle Performance Tax and Accounting",
    "website": "www.pinnacleperformancetax.com",
    "email": "pinnacleperformancetax@gmail.com",
    "phone": "478-338-1632",
    "primary_color": "#11823b",
    "dark_color": "#0f172a",
    "payment_provider": "Clover",
}

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "ppt-change-this-in-production")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024

login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message_category = "warning"


def ensure_dirs() -> None:
    INSTANCE_DIR.mkdir(exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        ensure_dirs()
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(_error=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


@app.context_processor
def inject_brand():
    return {"brand": BRAND, "year": datetime.now().year}


class User(UserMixin):
    def __init__(self, row: sqlite3.Row):
        self.id = str(row["id"])
        self.name = row["name"]
        self.email = row["email"]
        self.role = row["role"]
        self.client_id = row["client_id"] if "client_id" in row.keys() else None
        self.is_active_flag = bool(row["is_active"]) if "is_active" in row.keys() else True

    @property
    def is_active(self):
        return self.is_active_flag


def query_db(query: str, args: tuple = (), one: bool = False):
    cur = get_db().execute(query, args)
    rows = cur.fetchall()
    cur.close()
    return (rows[0] if rows else None) if one else rows


def execute_db(query: str, args: tuple = ()) -> int:
    db = get_db()
    cur = db.execute(query, args)
    db.commit()
    return cur.lastrowid


@login_manager.user_loader
def load_user(user_id: str):
    row = query_db("SELECT * FROM users WHERE id = ?", (user_id,), one=True)
    return User(row) if row else None


def admin_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            abort(403)
        return view_func(*args, **kwargs)
    return wrapped


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def money(value) -> float:
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


def safe_payment_link(link: str) -> str:
    link = (link or "").strip()
    if not link:
        return ""
    parsed = urlparse(link)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return link



def normalize_payment_link(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith("http://"):
        value = "https://" + value[len("http://"):]
    if not value.startswith("https://"):
        return ""
    return value


def add_column_if_missing(db: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    try:
        existing = [row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()]
        if column not in existing:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError:
        pass



def clean_and_seed_categories(db: sqlite3.Connection) -> None:
    categories = [
        ("Tax Preparation Income", "income"),
        ("Bookkeeping Income", "income"),
        ("Consulting Income", "income"),
        ("Office Supplies", "expense"),
        ("Software & Subscriptions", "expense"),
        ("Advertising & Marketing", "expense"),
        ("Travel", "expense"),
        ("Meals", "expense"),
    ]

    # Move transactions pointing to duplicate category rows back to the first matching category.
    duplicate_groups = db.execute(
        """SELECT name, kind, MIN(id) AS keep_id
           FROM categories
           GROUP BY name, kind
           HAVING COUNT(*) > 1"""
    ).fetchall()

    for group in duplicate_groups:
        duplicate_ids = [
            row[0] for row in db.execute(
                "SELECT id FROM categories WHERE name=? AND kind=? AND id<>?",
                (group["name"], group["kind"], group["keep_id"]),
            ).fetchall()
        ]
        for duplicate_id in duplicate_ids:
            db.execute(
                "UPDATE transactions SET category_id=? WHERE category_id=?",
                (group["keep_id"], duplicate_id),
            )

    # Delete duplicate category rows but keep the first one.
    db.execute(
        """DELETE FROM categories
           WHERE id NOT IN (
               SELECT MIN(id)
               FROM categories
               GROUP BY name, kind
           )"""
    )

    # Add missing default categories without creating duplicates.
    for name, kind in categories:
        existing = db.execute(
            "SELECT id FROM categories WHERE name=? AND kind=? LIMIT 1",
            (name, kind),
        ).fetchone()
        if not existing:
            db.execute(
                "INSERT INTO categories(name, kind) VALUES (?, ?)",
                (name, kind),
            )


def init_db() -> None:
    ensure_dirs()
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    db.executescript("""
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        business_name TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        client_type TEXT,
        status TEXT DEFAULT 'Active',
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'client',
        client_id INTEGER,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        kind TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        description TEXT,
        type TEXT,
        category_id INTEGER,
        client_id INTEGER,
        amount REAL DEFAULT 0,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS tax_returns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        tax_year TEXT,
        service_type TEXT,
        status TEXT,
        due_date TEXT,
        fee REAL DEFAULT 0,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        document_name TEXT,
        original_filename TEXT,
        file_path TEXT,
        tax_year TEXT,
        status TEXT DEFAULT 'Received',
        notes TEXT,
        uploaded_by INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        invoice_number TEXT,
        issue_date TEXT,
        due_date TEXT,
        amount REAL DEFAULT 0,
        status TEXT DEFAULT 'Draft',
        description TEXT,
        payment_link TEXT,
        payment_provider TEXT DEFAULT 'Clover',
        clover_link TEXT,
        payment_notes TEXT,
        paid_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        title TEXT,
        start_at TEXT,
        end_at TEXT,
        location TEXT,
        meeting_link TEXT,
        notes TEXT,
        status TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS crm_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone TEXT,
        email TEXT,
        status TEXT DEFAULT 'New',
        source TEXT,
        follow_up_date TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER,
        amount REAL DEFAULT 0,
        provider TEXT DEFAULT 'Clover',
        payment_link TEXT,
        status TEXT DEFAULT 'Created',
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    for col, definition in [
        ("payment_provider", "TEXT DEFAULT 'Clover'"),
        ("clover_link", "TEXT"),
        ("payment_notes", "TEXT"),
        ("paid_at", "TEXT"),
    ]:
        add_column_if_missing(db, "invoices", col, definition)
    add_column_if_missing(db, "clients", "address", "TEXT")
    add_column_if_missing(db, "users", "is_active", "INTEGER DEFAULT 1")
    for col, definition in [
        ("payment_provider", "TEXT DEFAULT 'Clover'"),
        ("clover_link", "TEXT"),
        ("payment_notes", "TEXT"),
        ("paid_at", "TEXT"),
    ]:
        add_column_if_missing(db, "invoices", col, definition)


    clean_and_seed_categories(db)

    admin = db.execute("SELECT id FROM users WHERE lower(email)=?", ("admin@pinnacleperformancetax.com",)).fetchone()
    admin_hash = generate_password_hash(os.environ.get("ADMIN_PASSWORD", "ChangeMe123!"), method="pbkdf2:sha256")
    if admin:
        db.execute("UPDATE users SET name=?, password_hash=?, role=?, is_active=1 WHERE lower(email)=?",
                   ("PPT Admin", admin_hash, "admin", "admin@pinnacleperformancetax.com"))
    else:
        db.execute("INSERT INTO users (name, email, password_hash, role, is_active) VALUES (?, ?, ?, ?, ?)",
                   ("PPT Admin", "admin@pinnacleperformancetax.com", admin_hash, "admin", 1))

    if db.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 0:
        db.execute("""INSERT INTO clients (name, business_name, email, phone, client_type, status, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   ("Sample Client", "Sample Business LLC", "client@example.com", "478-555-0110", "Business", "Active", "Demo client record"))

    client = db.execute("SELECT id FROM clients WHERE lower(email)=?", ("client@example.com",)).fetchone()
    client_id = client["id"] if client else 1
    sample_user = db.execute("SELECT id FROM users WHERE lower(email)=?", ("client@example.com",)).fetchone()
    client_hash = generate_password_hash("Client123!", method="pbkdf2:sha256")
    if sample_user:
        db.execute("UPDATE users SET name=?, password_hash=?, role=?, client_id=?, is_active=1 WHERE lower(email)=?",
                   ("Sample Client", client_hash, "client", client_id, "client@example.com"))
    else:
        db.execute("INSERT INTO users (name, email, password_hash, role, client_id, is_active) VALUES (?, ?, ?, ?, ?, ?)",
                   ("Sample Client", "client@example.com", client_hash, "client", client_id, 1))

    if db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0:
        cat = db.execute("SELECT id FROM categories WHERE name='Bookkeeping Income'").fetchone()
        db.execute("""INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   ("2026-04-20", "Monthly bookkeeping retainer", "income", cat["id"] if cat else None, client_id, 450.00, "Sample transaction"))

    if db.execute("SELECT COUNT(*) FROM invoices").fetchone()[0] == 0:
        db.execute("""INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description, payment_provider)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                   (client_id, "PPT-1001", "2026-04-20", "2026-04-30", 375.00, "Draft", "Tax preparation deposit", "Clover"))

    if db.execute("SELECT COUNT(*) FROM tax_returns").fetchone()[0] == 0:
        db.execute("""INSERT INTO tax_returns(client_id, tax_year, service_type, status, due_date, fee, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (client_id, "2025", "Individual Tax Return", "In Progress", "2026-04-15", 0, "Sample tax return"))

    if db.execute("SELECT COUNT(*) FROM appointments").fetchone()[0] == 0:
        db.execute("""INSERT INTO appointments(client_id, title, start_at, end_at, location, meeting_link, notes, status)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                   (client_id, "Tax review call", "2026-04-20T14:00", "2026-04-20T14:30", "Phone", "", "Sample appointment", "Scheduled"))

    db.commit()
    db.close()


@app.before_request
def bootstrap_database():
    if request.endpoint != "static":
        init_db()


@app.route("/init")
def init():
    init_db()
    return "INIT COMPLETE"


@app.route("/reset-admin-2026")
def reset_admin_2026():
    init_db()
    return "ADMIN RESET COMPLETE - admin@pinnacleperformancetax.com / ChangeMe123!"


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        row = query_db("SELECT * FROM users WHERE lower(email)=?", (email,), one=True)
        if row and check_password_hash(row["password_hash"], password):
            login_user(User(row), remember=True)
            flash("Welcome back.", "success")
            return redirect(url_for("dashboard"))
        flash("Invalid login.", "danger")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been signed out.", "info")
    return redirect(url_for("login"))


@app.route("/")
@app.route("/dashboard")
@login_required
def dashboard():
    client_filter = None if current_user.role == "admin" else current_user.client_id
    where = "" if client_filter is None else "WHERE client_id = ?"
    args = () if client_filter is None else (client_filter,)

    income = query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions {where} AND type='income'" if where else "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", args if where else (), one=True)["total"]
    expenses = query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions {where} AND type='expense'" if where else "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", args if where else (), one=True)["total"]
    active_clients = query_db("SELECT COUNT(*) c FROM clients WHERE status='Active'", one=True)["c"]
    open_invoices = query_db(f"SELECT COUNT(*) c FROM invoices {where} AND status!='Paid'" if where else "SELECT COUNT(*) c FROM invoices WHERE status!='Paid'", args if where else (), one=True)["c"]
    unpaid_invoices = query_db(f"SELECT COALESCE(SUM(amount),0) total FROM invoices {where} AND status!='Paid'" if where else "SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", args if where else (), one=True)["total"]
    pending_docs = query_db(f"SELECT COUNT(*) c FROM documents {where} AND status='Received'" if where else "SELECT COUNT(*) c FROM documents WHERE status='Received'", args if where else (), one=True)["c"]
    tax_returns = query_db(f"SELECT COUNT(*) c FROM tax_returns {where}" if where else "SELECT COUNT(*) c FROM tax_returns", args if where else (), one=True)["c"]
    appointments_count = query_db(f"SELECT COUNT(*) c FROM appointments {where}" if where else "SELECT COUNT(*) c FROM appointments", args if where else (), one=True)["c"]
    crm_total = query_db("SELECT COUNT(*) c FROM crm_leads", one=True)["c"]
    crm_new = query_db("SELECT COUNT(*) c FROM crm_leads WHERE status='New'", one=True)["c"]
    crm_converted = query_db("SELECT COUNT(*) c FROM crm_leads WHERE status='Converted'", one=True)["c"]

    recent_leads = query_db("SELECT * FROM crm_leads ORDER BY created_at DESC, id DESC LIMIT 5")
    recent_transactions = query_db(
        f"""SELECT t.*, c.name AS category_name, cl.name AS client_name
            FROM transactions t
            LEFT JOIN categories c ON c.id=t.category_id
            LEFT JOIN clients cl ON cl.id=t.client_id
            {where}
            ORDER BY date DESC, id DESC LIMIT 8""",
        args if where else (),
    )
    upcoming_appointments = query_db(
        f"""SELECT a.*, cl.name AS client_name
            FROM appointments a LEFT JOIN clients cl ON cl.id=a.client_id
            {where}
            ORDER BY start_at ASC LIMIT 6""",
        args if where else (),
    )

    return render_template(
        "dashboard.html",
        income=income,
        expenses=expenses,
        balance=income - expenses,
        active_clients=active_clients,
        open_invoices=open_invoices,
        unpaid_invoices=unpaid_invoices,
        pending_docs=pending_docs,
        tax_returns=tax_returns,
        appointments=appointments_count,
        crm_total=crm_total,
        crm_new=crm_new,
        crm_converted=crm_converted,
        recent_leads=recent_leads,
        recent_transactions=recent_transactions,
        upcoming_appointments=upcoming_appointments,
    )



@app.route("/maintenance/clean-categories")
@login_required
@admin_required
def maintenance_clean_categories():
    db = get_db()
    clean_and_seed_categories(db)
    db.commit()
    flash("Bookkeeping categories cleaned.", "success")
    return redirect(url_for("transactions"))


@app.route("/clients", methods=["GET", "POST"])
@login_required
@admin_required
def clients():
    if request.method == "POST":
        execute_db("""INSERT INTO clients(name, business_name, email, phone, address, client_type, status, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("name"), request.form.get("business_name"), request.form.get("email"),
                    request.form.get("phone"), request.form.get("address"), request.form.get("client_type"),
                    request.form.get("status"), request.form.get("notes")))
        flash("Client added.", "success")
        return redirect(url_for("clients"))
    rows = query_db("SELECT * FROM clients ORDER BY created_at DESC, id DESC")
    return render_template("clients.html", clients=rows)


@app.route("/transactions", methods=["GET", "POST"])
@login_required
def transactions():
    if request.method == "POST" and current_user.role == "admin":
        execute_db("""INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("date"), request.form.get("description"), request.form.get("type"),
                    request.form.get("category_id") or None, request.form.get("client_id") or None,
                    money(request.form.get("amount")), request.form.get("notes")))
        flash("Transaction added.", "success")
        return redirect(url_for("transactions"))

    rows = query_db("""SELECT t.*, c.name AS category_name, cl.name AS client_name
                       FROM transactions t
                       LEFT JOIN categories c ON c.id=t.category_id
                       LEFT JOIN clients cl ON cl.id=t.client_id
                       WHERE (?='admin' OR t.client_id=?)
                       ORDER BY t.date DESC, t.id DESC""",
                    (current_user.role, current_user.client_id or -1))
    categories = query_db("SELECT MIN(id) AS id, name, kind FROM categories GROUP BY name, kind ORDER BY kind, name")
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("transactions.html", transactions=rows, categories=categories, clients=clients_rows)


@app.route("/tax_returns", methods=["GET", "POST"])
@app.route("/tax-returns", methods=["GET", "POST"])
@login_required
def tax_returns():
    if request.method == "POST" and current_user.role == "admin":
        execute_db("""INSERT INTO tax_returns(client_id, tax_year, service_type, status, due_date, fee, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id"), request.form.get("tax_year"), request.form.get("service_type"),
                    request.form.get("status"), request.form.get("due_date"), money(request.form.get("fee")),
                    request.form.get("notes")))
        flash("Tax return created.", "success")
        return redirect(url_for("tax_returns"))
    rows = query_db("""SELECT tr.*, cl.name AS client_name, cl.business_name
                       FROM tax_returns tr LEFT JOIN clients cl ON cl.id=tr.client_id
                       WHERE (?='admin' OR tr.client_id=?)
                       ORDER BY tr.tax_year DESC, tr.id DESC""",
                    (current_user.role, current_user.client_id or -1))
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("tax_returns.html", returns=rows, clients=clients_rows)


@app.route("/documents", methods=["GET", "POST"])
@login_required
def documents():
    if request.method == "POST":
        client_id = request.form.get("client_id") if current_user.role == "admin" else current_user.client_id
        uploaded = request.files.get("file")
        if not client_id:
            flash("Please select a client before saving a document.", "warning")
            return redirect(url_for("documents"))

        original_filename = None
        file_path = None
        if uploaded and uploaded.filename:
            if not allowed_file(uploaded.filename):
                flash("Unsupported file type.", "danger")
                return redirect(url_for("documents"))
            original_filename = secure_filename(uploaded.filename)
            saved_name = f"{uuid4().hex}_{original_filename}"
            uploaded.save(UPLOAD_DIR / saved_name)
            file_path = f"static/uploads/{saved_name}"

        execute_db("""INSERT INTO documents(client_id, document_name, original_filename, file_path, tax_year, status, notes, uploaded_by)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                   (client_id, request.form.get("document_name") or "Uploaded Document", original_filename, file_path,
                    request.form.get("tax_year"), request.form.get("status") or "Received", request.form.get("notes"),
                    current_user.id))
        flash("Document saved.", "success")
        return redirect(url_for("documents"))

    rows = query_db("""SELECT d.*, cl.name AS client_name
                       FROM documents d LEFT JOIN clients cl ON cl.id=d.client_id
                       WHERE (?='admin' OR d.client_id=?)
                       ORDER BY d.created_at DESC, d.id DESC""",
                    (current_user.role, current_user.client_id or -1))
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("documents.html", documents=rows, clients=clients_rows)


@app.route("/invoices", methods=["GET", "POST"])
@login_required
def invoices():
    if request.method == "POST" and current_user.role == "admin":
        execute_db("""INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description, payment_link, clover_link, payment_provider)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id"), request.form.get("invoice_number"), request.form.get("issue_date"),
                    request.form.get("due_date"), money(request.form.get("amount")), request.form.get("status") or "Draft",
                    request.form.get("description"), safe_payment_link(request.form.get("payment_link") or ""),
                    safe_payment_link(request.form.get("payment_link") or ""), "Clover"))
        flash("Invoice created.", "success")
        return redirect(url_for("invoices"))

    rows = query_db("""SELECT i.*, cl.name AS client_name FROM invoices i
                       LEFT JOIN clients cl ON cl.id=i.client_id
                       WHERE (?='admin' OR i.client_id=?)
                       ORDER BY i.issue_date DESC, i.id DESC""",
                    (current_user.role, current_user.client_id or -1))
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("invoices.html", invoices=rows, clients=clients_rows)



@app.route("/payments", methods=["GET", "POST"])
@login_required
@admin_required
def payments():
    if request.method == "POST":
        invoice_id = request.form.get("invoice_id")
        action = request.form.get("action") or "pay"

        if not invoice_id:
            flash("Please select an invoice.", "danger")
            return redirect(url_for("payments"))

        invoice = query_db("SELECT * FROM invoices WHERE id=?", (invoice_id,), one=True)
        if not invoice:
            flash("Invoice not found.", "danger")
            return redirect(url_for("payments"))

        if action == "save_link":
            clover_link = normalize_payment_link(request.form.get("clover_link"))
            if not clover_link:
                flash("Paste the correct Clover checkout link for this invoice.", "danger")
                return redirect(url_for("payments"))
            execute_db(
                """UPDATE invoices
                   SET payment_link=?, clover_link=?, payment_provider='Clover', payment_notes=?
                   WHERE id=?""",
                (clover_link, clover_link, "Invoice-specific Clover checkout link saved", invoice_id),
            )
            flash("Clover checkout link saved to this invoice.", "success")
            return redirect(url_for("payments"))

        if action == "pay":
            clover_link = normalize_payment_link(invoice["payment_link"] or invoice["clover_link"] or "")
            if not clover_link:
                flash("This invoice needs its own Clover checkout link first.", "danger")
                return redirect(url_for("payments"))
            execute_db(
                """INSERT INTO payments(invoice_id, amount, provider, payment_link, status, notes)
                   VALUES (?, ?, 'Clover', ?, 'Checkout Opened', ?)""",
                (invoice_id, invoice["amount"], clover_link, "Pay Invoice button opened Clover checkout"),
            )
            execute_db(
                "UPDATE invoices SET status='Sent', payment_provider='Clover' WHERE id=? AND status!='Paid'",
                (invoice_id,),
            )
            return redirect(clover_link)

        if action == "mark_paid":
            clover_link = normalize_payment_link(invoice["payment_link"] or invoice["clover_link"] or "")
            execute_db(
                "UPDATE invoices SET status='Paid', paid_at=CURRENT_TIMESTAMP, payment_provider='Clover' WHERE id=?",
                (invoice_id,),
            )
            existing = query_db(
                "SELECT id FROM payments WHERE invoice_id=? AND status='Paid' LIMIT 1",
                (invoice_id,),
                one=True,
            )
            if not existing:
                execute_db(
                    """INSERT INTO payments(invoice_id, amount, provider, payment_link, status, notes)
                       VALUES (?, ?, 'Clover', ?, 'Paid', ?)""",
                    (invoice_id, invoice["amount"], clover_link, "Marked paid after Clover checkout"),
                )

            income_category = query_db(
                "SELECT id FROM categories WHERE name='Tax Preparation Income' AND kind='income' LIMIT 1",
                one=True,
            )
            existing_transaction = query_db(
                "SELECT id FROM transactions WHERE notes=? LIMIT 1",
                (f"Auto-created from paid invoice #{invoice_id}",),
                one=True,
            )
            if not existing_transaction:
                execute_db(
                    """INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                       VALUES (?, ?, 'income', ?, ?, ?, ?)""",
                    (
                        datetime.now().strftime("%Y-%m-%d"),
                        f"Clover payment for invoice #{invoice['invoice_number'] or invoice_id}",
                        income_category["id"] if income_category else None,
                        invoice["client_id"],
                        invoice["amount"],
                        f"Auto-created from paid invoice #{invoice_id}",
                    ),
                )
            flash("Invoice marked paid and bookkeeping income recorded.", "success")
            return redirect(url_for("payments"))

        return redirect(url_for("payments"))

    invoices_rows = query_db(
        """SELECT i.id, i.invoice_number, i.amount, i.status, i.description, i.payment_link,
                  i.payment_provider, i.payment_notes, i.created_at, i.paid_at, i.clover_link,
                  cl.name AS client_name, cl.email AS client_email
           FROM invoices i
           LEFT JOIN clients cl ON cl.id=i.client_id
           ORDER BY i.created_at DESC, i.id DESC"""
    )
    payment_rows = query_db(
        """SELECT p.*, i.invoice_number, cl.name AS client_name
           FROM payments p
           LEFT JOIN invoices i ON i.id=p.invoice_id
           LEFT JOIN clients cl ON cl.id=i.client_id
           ORDER BY p.created_at DESC, p.id DESC
           LIMIT 50"""
    )
    paid_total = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status='Paid'", one=True)["total"]
    unpaid_total = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]
    needs_link = query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid' AND (payment_link IS NULL OR payment_link='')", one=True)["c"]

    return render_template(
        "payments.html",
        invoices=invoices_rows,
        payments=payment_rows,
        paid_total=paid_total,
        unpaid_total=unpaid_total,
        needs_link=needs_link,
    )



@app.route("/appointments", methods=["GET", "POST"])
@login_required
def appointments():
    if request.method == "POST" and current_user.role == "admin":
        execute_db("""INSERT INTO appointments(client_id, title, start_at, end_at, location, meeting_link, notes, status)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id") or None, request.form.get("title"), request.form.get("start_at"),
                    request.form.get("end_at"), request.form.get("location"), request.form.get("meeting_link"),
                    request.form.get("notes"), request.form.get("status")))
        flash("Appointment added.", "success")
        return redirect(url_for("appointments"))
    rows = query_db("""SELECT a.*, cl.name AS client_name FROM appointments a
                       LEFT JOIN clients cl ON cl.id=a.client_id
                       WHERE (?='admin' OR a.client_id=?)
                       ORDER BY a.start_at ASC, a.id DESC""",
                    (current_user.role, current_user.client_id or -1))
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("appointments.html", appointments=rows, clients=clients_rows)


@app.route("/crm", methods=["GET", "POST"])
@login_required
@admin_required
def crm():
    if request.method == "POST":
        execute_db("""INSERT INTO crm_leads(name, phone, email, status, source, follow_up_date, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("name"), request.form.get("phone"), request.form.get("email"),
                    request.form.get("status") or "New", request.form.get("source"),
                    request.form.get("follow_up_date"), request.form.get("notes")))
        flash("CRM lead saved.", "success")
        return redirect(url_for("crm"))
    stats = query_db("""SELECT COUNT(*) total,
                               SUM(CASE WHEN status='New' THEN 1 ELSE 0 END) new,
                               SUM(CASE WHEN status='Contacted' THEN 1 ELSE 0 END) contacted,
                               SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) converted
                        FROM crm_leads""", one=True)
    rows = query_db("SELECT * FROM crm_leads ORDER BY created_at DESC, id DESC")
    return render_template("crm.html", leads=rows, stats=stats)


@app.route("/crm/<int:lead_id>/update", methods=["POST"])
@login_required
@admin_required
def update_crm_lead(lead_id):
    execute_db("""UPDATE crm_leads SET status=?, follow_up_date=?, notes=?, updated_at=CURRENT_TIMESTAMP WHERE id=?""",
               (request.form.get("status") or "New", request.form.get("follow_up_date"), request.form.get("notes"), lead_id))
    flash("CRM lead updated.", "success")
    return redirect(url_for("crm"))


@app.route("/crm/<int:lead_id>/convert", methods=["POST"])
@login_required
@admin_required
def convert_lead(lead_id):
    lead = query_db("SELECT * FROM crm_leads WHERE id=?", (lead_id,), one=True)
    if not lead:
        flash("Lead not found.", "danger")
        return redirect(url_for("crm"))
    client_id = execute_db("""INSERT INTO clients(name, business_name, email, phone, client_type, status, notes)
                              VALUES (?, ?, ?, ?, ?, ?, ?)""",
                           (lead["name"], "", lead["email"], lead["phone"], "CRM Lead", "Active",
                            f"Converted from CRM. Source: {lead['source'] or ''}. Notes: {lead['notes'] or ''}"))
    execute_db("UPDATE crm_leads SET status='Converted', updated_at=CURRENT_TIMESTAMP WHERE id=?", (lead_id,))
    flash("Lead converted to client.", "success")
    return redirect(url_for("crm"))


@app.route("/settings", methods=["GET", "POST"])
@login_required
@admin_required
def settings():
    if request.method == "POST":
        password = request.form.get("password") or "Temp123!"
        execute_db("""INSERT INTO users(name, email, password_hash, role, client_id, is_active)
                      VALUES (?, ?, ?, ?, ?, ?)""",
                   (request.form.get("name"), request.form.get("email").lower(),
                    generate_password_hash(password, method="pbkdf2:sha256"),
                    request.form.get("role"), request.form.get("client_id") or None, 1))
        flash("User created.", "success")
        return redirect(url_for("settings"))
    users = query_db("""SELECT u.*, cl.name AS client_name FROM users u LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.created_at DESC""")
    clients_rows = query_db("SELECT * FROM clients ORDER BY name")
    return render_template("settings.html", users=users, clients=clients_rows)



@app.route("/reports")
@login_required
@admin_required
def reports():
    income = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", one=True)["total"]
    expenses = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", one=True)["total"]
    paid = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status='Paid'", one=True)["total"]
    unpaid = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]
    transaction_rows = query_db(
        """SELECT t.*, c.name AS category_name, cl.name AS client_name
           FROM transactions t
           LEFT JOIN categories c ON c.id=t.category_id
           LEFT JOIN clients cl ON cl.id=t.client_id
           ORDER BY t.date DESC, t.id DESC
           LIMIT 50"""
    )
    invoice_rows = query_db(
        """SELECT i.*, cl.name AS client_name
           FROM invoices i
           LEFT JOIN clients cl ON cl.id=i.client_id
           ORDER BY i.issue_date DESC, i.id DESC
           LIMIT 50"""
    )
    return render_template(
        "reports.html",
        income=income,
        expenses=expenses,
        profit=income - expenses,
        paid=paid,
        unpaid=unpaid,
        transactions=transaction_rows,
        invoices=invoice_rows,
    )


@app.route("/reports/export/transactions.csv")
@login_required
@admin_required
def export_transactions():
    rows = query_db("""SELECT t.date, t.description, t.type, c.name AS category, cl.name AS client, t.amount, t.notes
                       FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
                       LEFT JOIN clients cl ON cl.id=t.client_id
                       ORDER BY t.date DESC, t.id DESC""")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Description", "Type", "Category", "Client", "Amount", "Notes"])
    for row in rows:
        writer.writerow([row["date"], row["description"], row["type"], row["category"], row["client"], row["amount"], row["notes"]])
    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="ppt_transactions.csv")


@app.errorhandler(403)
def forbidden(_):
    return render_template("error.html", message="You do not have access to that page."), 403


@app.template_filter("currency")
def currency_filter(value):
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "$0.00"


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
