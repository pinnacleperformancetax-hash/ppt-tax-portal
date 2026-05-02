from __future__ import annotations

import csv
import io
import os
import sqlite3
from functools import wraps
from pathlib import Path
from uuid import uuid4
from datetime import datetime

from flask import (
    Flask, g, render_template, request, redirect, url_for,
    flash, send_file, abort
)
from flask_login import (
    LoginManager, UserMixin, current_user, login_required,
    login_user, logout_user
)
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


@login_manager.user_loader
def load_user(user_id: str):
    row = query_db("SELECT * FROM users WHERE id = ?", (user_id,), one=True)
    return User(row) if row else None


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


def add_column_if_missing(db: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    try:
        existing = [row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()]
        if column not in existing:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError:
        pass


def init_db() -> None:
    ensure_dirs()
    db = sqlite3.connect(DB_PATH)

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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id)
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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(category_id) REFERENCES categories(id),
        FOREIGN KEY(client_id) REFERENCES clients(id)
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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id)
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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id),
        FOREIGN KEY(uploaded_by) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        invoice_number TEXT,
        issue_date TEXT,
        due_date TEXT,
        amount REAL DEFAULT 0,
        status TEXT,
        description TEXT,
        payment_link TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id)
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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(client_id) REFERENCES clients(id)
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
    """)

    add_column_if_missing(db, "clients", "address", "TEXT")
    add_column_if_missing(db, "users", "is_active", "INTEGER DEFAULT 1")

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

    for name, kind in categories:
        db.execute("INSERT OR IGNORE INTO categories(name, kind) VALUES (?, ?)", (name, kind))

    admin = db.execute(
        "SELECT id FROM users WHERE lower(email)=?",
        ("admin@pinnacleperformancetax.com",),
    ).fetchone()

    if admin:
        db.execute(
            "UPDATE users SET name=?, password_hash=?, role=?, is_active=1 WHERE lower(email)=?",
            (
                "PPT Admin",
                generate_password_hash("ChangeMe123!", method="pbkdf2:sha256"),
                "admin",
                "admin@pinnacleperformancetax.com",
            ),
        )
    else:
        db.execute(
            "INSERT INTO users (name, email, password_hash, role, is_active) VALUES (?, ?, ?, ?, ?)",
            (
                "PPT Admin",
                "admin@pinnacleperformancetax.com",
                generate_password_hash("ChangeMe123!", method="pbkdf2:sha256"),
                "admin",
                1,
            ),
        )

    if db.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 0:
        db.execute(
            """INSERT INTO clients
            (name, business_name, email, phone, client_type, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("Sample Client", "Sample Business LLC", "client@example.com", "478-555-0110", "Business", "Active", "Demo client record"),
        )

    client = db.execute("SELECT id FROM clients WHERE lower(email)=?", ("client@example.com",)).fetchone()
    client_id = client[0] if client else 1
    sample_user = db.execute("SELECT id FROM users WHERE lower(email)=?", ("client@example.com",)).fetchone()
    if sample_user:
        db.execute(
            "UPDATE users SET name=?, password_hash=?, role=?, client_id=?, is_active=1 WHERE lower(email)=?",
            ("Sample Client", generate_password_hash("Client123!", method="pbkdf2:sha256"), "client", client_id, "client@example.com"),
        )
    else:
        db.execute(
            "INSERT INTO users (name, email, password_hash, role, client_id, is_active) VALUES (?, ?, ?, ?, ?, ?)",
            ("Sample Client", "client@example.com", generate_password_hash("Client123!", method="pbkdf2:sha256"), "client", client_id, 1),
        )

    db.commit()
    db.close()


# PERFORMANCE FIX:
# Initialize the database once when the app imports, instead of running init_db() on every page load.
# This fixes the slow Render behavior caused by @app.before_request.
init_db()


@app.route("/init")
def init():
    init_db()
    return "INIT COMPLETE - You can now log in"


@app.route("/reset-admin-2026")
def reset_admin_2026():
    init_db()
    return "ADMIN RESET COMPLETE - Login with admin@pinnacleperformancetax.com / ChangeMe123!"


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


@app.route("/dashboard")
@app.route("/")
@login_required
def dashboard():
    client_filter = None if current_user.role == "admin" else current_user.client_id
    where = "" if client_filter is None else "WHERE client_id = ?"
    args = () if client_filter is None else (client_filter,)

    income = query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions {where} AND type='income'" if where else "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", args if where else (), one=True)["total"]
    expenses = query_db(f"SELECT COALESCE(SUM(amount),0) total FROM transactions {where} AND type='expense'" if where else "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", args if where else (), one=True)["total"]
    open_invoices = query_db(f"SELECT COUNT(*) c FROM invoices {where} AND status!='Paid'" if where else "SELECT COUNT(*) c FROM invoices WHERE status!='Paid'", args if where else (), one=True)["c"]
    pending_docs = query_db(f"SELECT COUNT(*) c FROM documents {where} AND status='Received'" if where else "SELECT COUNT(*) c FROM documents WHERE status='Received'", args if where else (), one=True)["c"]
    tax_returns_count = query_db(f"SELECT COUNT(*) c FROM tax_returns {where}" if where else "SELECT COUNT(*) c FROM tax_returns", args if where else (), one=True)["c"]
    appointments_count = query_db(f"SELECT COUNT(*) c FROM appointments {where}" if where else "SELECT COUNT(*) c FROM appointments", args if where else (), one=True)["c"]

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

    active_clients = query_db("SELECT COUNT(*) c FROM clients WHERE status='Active'", one=True)["c"]
    crm_total = query_db("SELECT COUNT(*) c FROM crm_leads", one=True)["c"]
    crm_new = query_db("SELECT COUNT(*) c FROM crm_leads WHERE status='New'", one=True)["c"]
    crm_converted = query_db("SELECT COUNT(*) c FROM crm_leads WHERE status='Converted'", one=True)["c"]
    unpaid_invoice_total = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]

    recent_leads = query_db(
        """SELECT * FROM crm_leads
        ORDER BY created_at DESC, id DESC
        LIMIT 6"""
    )

    return render_template(
        "dashboard.html",
        income=income,
        expenses=expenses,
        balance=income - expenses,
        open_invoices=open_invoices,
        pending_docs=pending_docs,
        tax_returns=tax_returns_count,
        appointments=appointments_count,
        active_clients=active_clients,
        crm_total=crm_total,
        crm_new=crm_new,
        crm_converted=crm_converted,
        unpaid_invoice_total=unpaid_invoice_total,
        recent_transactions=recent_transactions,
        upcoming_appointments=upcoming_appointments,
        recent_leads=recent_leads,
    )


@app.route("/clients", methods=["GET", "POST"])
@login_required
@admin_required
def clients():
    if request.method == "POST":
        execute_db(
            """INSERT INTO clients(name, business_name, email, phone, address, client_type, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("name"),
                request.form.get("business_name"),
                request.form.get("email"),
                request.form.get("phone"),
                request.form.get("address"),
                request.form.get("client_type"),
                request.form.get("status"),
                request.form.get("notes"),
            ),
        )
        flash("Client added.", "success")
        return redirect(url_for("clients"))
    rows = query_db("SELECT * FROM clients ORDER BY created_at DESC, id DESC")
    return render_template("clients.html", clients=rows)


@app.route("/transactions", methods=["GET", "POST"])
@login_required
def transactions():
    if request.method == "POST" and current_user.role == "admin":
        execute_db(
            """INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("date"),
                request.form.get("description"),
                request.form.get("type"),
                request.form.get("category_id") or None,
                request.form.get("client_id") or None,
                money(request.form.get("amount")),
                request.form.get("notes"),
            ),
        )
        flash("Transaction added.", "success")
        return redirect(url_for("transactions"))

    rows = query_db(
        """SELECT t.*, c.name AS category_name, cl.name AS client_name
        FROM transactions t
        LEFT JOIN categories c ON c.id=t.category_id
        LEFT JOIN clients cl ON cl.id=t.client_id
        WHERE (?='admin' OR t.client_id=?)
        ORDER BY t.date DESC, t.id DESC""",
        (current_user.role, current_user.client_id or -1),
    )
    categories = query_db("SELECT * FROM categories ORDER BY kind, name")
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("transactions.html", transactions=rows, categories=categories, clients=clients_rows)


@app.route("/tax_returns", methods=["GET", "POST"])
@app.route("/tax-returns", methods=["GET", "POST"])
@login_required
def tax_returns():
    if request.method == "POST" and current_user.role == "admin":
        execute_db(
            """INSERT INTO tax_returns(client_id, tax_year, service_type, status, due_date, fee, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("client_id"),
                request.form.get("tax_year"),
                request.form.get("service_type"),
                request.form.get("status"),
                request.form.get("due_date"),
                money(request.form.get("fee")),
                request.form.get("notes"),
            ),
        )
        flash("Tax return created.", "success")
        return redirect(url_for("tax_returns"))

    rows = query_db(
        """SELECT tr.*, cl.name AS client_name, cl.business_name
        FROM tax_returns tr JOIN clients cl ON cl.id=tr.client_id
        WHERE (?='admin' OR tr.client_id=?)
        ORDER BY tr.tax_year DESC, tr.id DESC""",
        (current_user.role, current_user.client_id or -1),
    )
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

        file_path = None
        original_filename = None

        if uploaded and uploaded.filename:
            if not allowed_file(uploaded.filename):
                flash("Unsupported file type.", "danger")
                return redirect(url_for("documents"))
            original_filename = secure_filename(uploaded.filename)
            saved_name = f"{uuid4().hex}_{original_filename}"
            target = UPLOAD_DIR / saved_name
            uploaded.save(target)
            file_path = f"static/uploads/{saved_name}"

        execute_db(
            """INSERT INTO documents(client_id, document_name, original_filename, file_path, tax_year, status, notes, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                client_id,
                request.form.get("document_name") or "Uploaded Document",
                original_filename,
                file_path,
                request.form.get("tax_year"),
                request.form.get("status") or "Received",
                request.form.get("notes"),
                current_user.id,
            ),
        )
        flash("Document saved.", "success")
        return redirect(url_for("documents"))

    rows = query_db(
        """SELECT d.*, cl.name AS client_name
        FROM documents d JOIN clients cl ON cl.id=d.client_id
        WHERE (?='admin' OR d.client_id=?)
        ORDER BY d.created_at DESC, d.id DESC""",
        (current_user.role, current_user.client_id or -1),
    )
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("documents.html", documents=rows, clients=clients_rows)


@app.route("/crm", methods=["GET", "POST"])
@login_required
@admin_required
def crm():
    if request.method == "POST":
        execute_db(
            """INSERT INTO crm_leads
            (name, phone, email, status, source, follow_up_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("name"),
                request.form.get("phone"),
                request.form.get("email"),
                request.form.get("status") or "New",
                request.form.get("source"),
                request.form.get("follow_up_date"),
                request.form.get("notes"),
            ),
        )
        flash("CRM lead saved.", "success")
        return redirect(url_for("crm"))

    stats = query_db("""
        SELECT
            COUNT(*) as total,
            COALESCE(SUM(CASE WHEN status='New' THEN 1 ELSE 0 END), 0) as new,
            COALESCE(SUM(CASE WHEN status='Contacted' THEN 1 ELSE 0 END), 0) as contacted,
            COALESCE(SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END), 0) as converted
        FROM crm_leads
    """, one=True)

    search = request.args.get("search", "").strip()
    status_filter = request.args.get("status", "").strip()
    source_filter = request.args.get("source", "").strip()

    filters = []
    params = []

    if search:
        filters.append("(name LIKE ? OR phone LIKE ? OR email LIKE ? OR notes LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like, like])

    if status_filter:
        filters.append("status = ?")
        params.append(status_filter)

    if source_filter:
        filters.append("source LIKE ?")
        params.append(f"%{source_filter}%")

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    rows = query_db(
        f"""SELECT * FROM crm_leads
        {where_clause}
        ORDER BY created_at DESC, id DESC""",
        tuple(params),
    )

    return render_template(
        "crm.html",
        leads=rows,
        stats=stats,
        search=search,
        status_filter=status_filter,
        source_filter=source_filter,
    )


@app.route("/crm/<int:lead_id>/update", methods=["POST"])
@login_required
@admin_required
def update_crm_lead(lead_id):
    execute_db(
        """UPDATE crm_leads
        SET status=?, follow_up_date=?, notes=?, updated_at=CURRENT_TIMESTAMP
        WHERE id=?""",
        (
            request.form.get("status") or "New",
            request.form.get("follow_up_date"),
            request.form.get("notes"),
            lead_id,
        ),
    )
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

    existing_client = query_db(
        "SELECT id FROM clients WHERE lower(email)=?",
        ((lead["email"] or "").lower(),),
        one=True,
    ) if lead["email"] else None

    if existing_client:
        client_id = existing_client["id"]
    else:
        client_id = execute_db(
            """INSERT INTO clients(name, business_name, email, phone, client_type, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                lead["name"],
                "",
                lead["email"],
                lead["phone"],
                "CRM Lead",
                "Active",
                f"Converted from CRM. Source: {lead['source'] or ''}. Notes: {lead['notes'] or ''}",
            ),
        )

    execute_db(
        "UPDATE crm_leads SET status='Converted', updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (lead_id,),
    )
    flash("Lead converted to client.", "success")
    return redirect(url_for("crm"))


@app.route("/invoices", methods=["GET", "POST"])
@login_required
def invoices():
    if request.method == "POST" and current_user.role == "admin":
        execute_db(
            """INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description, payment_link)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("client_id"),
                request.form.get("invoice_number"),
                request.form.get("issue_date"),
                request.form.get("due_date"),
                money(request.form.get("amount")),
                request.form.get("status"),
                request.form.get("description"),
                request.form.get("payment_link"),
            ),
        )
        flash("Invoice created.", "success")
        return redirect(url_for("invoices"))

    rows = query_db(
        """SELECT i.*, cl.name AS client_name FROM invoices i
        JOIN clients cl ON cl.id=i.client_id
        WHERE (?='admin' OR i.client_id=?)
        ORDER BY i.issue_date DESC, i.id DESC""",
        (current_user.role, current_user.client_id or -1),
    )
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("invoices.html", invoices=rows, clients=clients_rows)


@app.route("/appointments", methods=["GET", "POST"])
@login_required
def appointments():
    if request.method == "POST" and current_user.role == "admin":
        execute_db(
            """INSERT INTO appointments(client_id, title, start_at, end_at, location, meeting_link, notes, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("client_id") or None,
                request.form.get("title"),
                request.form.get("start_at"),
                request.form.get("end_at"),
                request.form.get("location"),
                request.form.get("meeting_link"),
                request.form.get("notes"),
                request.form.get("status"),
            ),
        )
        flash("Appointment added.", "success")
        return redirect(url_for("appointments"))

    rows = query_db(
        """SELECT a.*, cl.name AS client_name FROM appointments a
        LEFT JOIN clients cl ON cl.id=a.client_id
        WHERE (?='admin' OR a.client_id=?)
        ORDER BY a.start_at ASC, a.id DESC""",
        (current_user.role, current_user.client_id or -1),
    )
    clients_rows = query_db("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("appointments.html", appointments=rows, clients=clients_rows)


@app.route("/reports/export/transactions.csv")
@login_required
@admin_required
def export_transactions():
    rows = query_db(
        """SELECT t.date, t.description, t.type, c.name AS category, cl.name AS client, t.amount, t.notes
        FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
        LEFT JOIN clients cl ON cl.id=t.client_id
        ORDER BY t.date DESC, t.id DESC"""
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Description", "Type", "Category", "Client", "Amount", "Notes"])
    for row in rows:
        writer.writerow([row["date"], row["description"], row["type"], row["category"], row["client"], row["amount"], row["notes"]])
    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="ppt_transactions.csv")


@app.route("/settings", methods=["GET", "POST"])
@login_required
@admin_required
def settings():
    if request.method == "POST":
        client_id = request.form.get("client_id") or None
        password = request.form.get("password") or "Temp123!"
        execute_db(
            """INSERT INTO users(name, email, password_hash, role, client_id, is_active) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                request.form.get("name"),
                request.form.get("email").lower(),
                generate_password_hash(password, method="pbkdf2:sha256"),
                request.form.get("role"),
                client_id,
                1,
            ),
        )
        flash("User created.", "success")
        return redirect(url_for("settings"))
    users = query_db("""SELECT u.*, cl.name AS client_name FROM users u LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.created_at DESC""")
    clients_rows = query_db("SELECT * FROM clients ORDER BY name")
    return render_template("settings.html", users=users, clients=clients_rows)


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
    app.run(debug=True)
