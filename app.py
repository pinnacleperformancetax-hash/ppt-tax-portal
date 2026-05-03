from __future__ import annotations

import csv
import io
import os
import sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

from flask import (
    Flask, Response, flash, g, redirect, render_template, request, url_for
)
from flask_login import (
    LoginManager, UserMixin, current_user, login_required, login_user, logout_user
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
INSTANCE_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = INSTANCE_DIR / "ppt_portal.db"

BRAND = {
    "app_name": "PPT Bookkeeping & Tax Portal Pro",
    "business_name": "Pinnacle Performance Tax and Accounting",
    "website": "www.pinnacleperformancetax.com",
    "email": "pinnacleperformancetax@gmail.com",
    "phone": "478-338-1632",
    "primary": "#11823b",
    "dark": "#0f172a",
}

ALLOWED_UPLOADS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "xls", "xlsx", "csv", "txt"}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-secret-key-in-render")
app.config["UPLOAD_FOLDER"] = str(UPLOAD_DIR)

login_manager = LoginManager(app)
login_manager.login_view = "login"


def db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc: Exception | None = None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def query(sql: str, args: tuple[Any, ...] = (), one: bool = False):
    cur = db().execute(sql, args)
    rows = cur.fetchall()
    return (rows[0] if rows else None) if one else rows


def execute(sql: str, args: tuple[Any, ...] = ()) -> int:
    cur = db().execute(sql, args)
    db().commit()
    return cur.lastrowid


def money(value: Any) -> float:
    try:
        return round(float(str(value or "0").replace("$", "").replace(",", "")), 2)
    except Exception:
        return 0.0


def normalize_link(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("http://"):
        value = "https://" + value[len("http://"):]
    if not value.startswith("https://"):
        return ""
    return value


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_UPLOADS


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            flash("Admin access required.", "danger")
            return redirect(url_for("dashboard"))
        return fn(*args, **kwargs)
    return wrapper


class User(UserMixin):
    def __init__(self, row: sqlite3.Row):
        self.id = str(row["id"])
        self.name = row["name"]
        self.email = row["email"]
        self.role = row["role"]
        self.client_id = row["client_id"]


@login_manager.user_loader
def load_user(user_id: str):
    row = query("SELECT * FROM users WHERE id=? AND is_active=1", (user_id,), one=True)
    return User(row) if row else None


@app.template_filter("currency")
def currency(v):
    return "${:,.2f}".format(money(v))


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db() -> None:
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        business_name TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        client_type TEXT DEFAULT 'Individual',
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
        name TEXT NOT NULL,
        kind TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        description TEXT NOT NULL,
        type TEXT NOT NULL,
        category_id INTEGER,
        client_id INTEGER,
        amount REAL NOT NULL,
        notes TEXT,
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
        payment_provider TEXT DEFAULT 'Clover',
        payment_link TEXT,
        clover_link TEXT,
        payment_notes TEXT,
        paid_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        name TEXT,
        filename TEXT,
        tax_year TEXT,
        status TEXT DEFAULT 'Received',
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS tax_returns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        tax_year TEXT,
        service_type TEXT,
        status TEXT DEFAULT 'In Progress',
        due_date TEXT,
        fee REAL DEFAULT 0,
        notes TEXT,
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
        status TEXT DEFAULT 'Scheduled',
        notes TEXT,
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
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Safe migrations for older DBs
    for col, definition in [
        ("payment_provider", "TEXT DEFAULT 'Clover'"),
        ("payment_link", "TEXT"),
        ("clover_link", "TEXT"),
        ("payment_notes", "TEXT"),
        ("paid_at", "TEXT"),
    ]:
        add_column_if_missing(conn, "invoices", col, definition)

    add_column_if_missing(conn, "users", "is_active", "INTEGER DEFAULT 1")
    add_column_if_missing(conn, "clients", "address", "TEXT")

    # Clean and seed categories once, no duplicates.
    desired = [
        ("Tax Preparation Income", "income"),
        ("Bookkeeping Income", "income"),
        ("Consulting Income", "income"),
        ("Office Supplies", "expense"),
        ("Software & Subscriptions", "expense"),
        ("Advertising & Marketing", "expense"),
        ("Travel", "expense"),
        ("Meals", "expense"),
    ]
    for name, kind in desired:
        conn.execute("INSERT INTO categories(name, kind) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM categories WHERE name=? AND kind=?)", (name, kind, name, kind))

    admin_email = "admin@pinnacleperformancetax.com"
    admin_password = os.environ.get("ADMIN_PASSWORD", "ChangeMe123")
    admin_hash = generate_password_hash(admin_password)
    if conn.execute("SELECT id FROM users WHERE lower(email)=?", (admin_email,)).fetchone():
        conn.execute("UPDATE users SET name=?, password_hash=?, role='admin', is_active=1 WHERE lower(email)=?", ("PPT Admin", admin_hash, admin_email))
    else:
        conn.execute("INSERT INTO users(name, email, password_hash, role, is_active) VALUES (?, ?, ?, 'admin', 1)", ("PPT Admin", admin_email, admin_hash))

    if conn.execute("SELECT COUNT(*) c FROM clients").fetchone()["c"] == 0:
        conn.execute("""INSERT INTO clients(name, business_name, email, phone, client_type, status, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                     ("Sample Client", "Sample Business LLC", "client@example.com", "478-555-0110", "Business", "Active", "Seed client"))
    client = conn.execute("SELECT id FROM clients WHERE email='client@example.com'").fetchone()
    if client and not conn.execute("SELECT id FROM users WHERE email='client@example.com'").fetchone():
        conn.execute("INSERT INTO users(name, email, password_hash, role, client_id, is_active) VALUES (?, ?, ?, 'client', ?, 1)",
                     ("Sample Client", "client@example.com", generate_password_hash("Client123!"), client["id"]))

    if conn.execute("SELECT COUNT(*) c FROM invoices").fetchone()["c"] == 0 and client:
        conn.execute("""INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                     (client["id"], "INV-0001", "2026-04-20", "2026-04-30", 375.00, "Draft", "Tax preparation service"))

    if conn.execute("SELECT COUNT(*) c FROM transactions").fetchone()["c"] == 0 and client:
        cat = conn.execute("SELECT id FROM categories WHERE name='Bookkeeping Income'").fetchone()
        conn.execute("""INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                        VALUES (?, ?, 'income', ?, ?, ?, ?)""",
                     ("2026-04-20", "Monthly bookkeeping retainer", cat["id"] if cat else None, client["id"], 450.00, "Sample transaction"))

    conn.commit()


@app.route("/init")
def init_route():
    init_db()
    return "INIT COMPLETE"


@app.route("/maintenance/clean-categories")
@login_required
@admin_required
def clean_categories():
    rows = query("SELECT id, name, kind FROM categories ORDER BY id")
    seen = {}
    for r in rows:
        key = (r["name"].strip().lower(), r["kind"].strip().lower())
        if key in seen:
            keep = seen[key]
            execute("UPDATE transactions SET category_id=? WHERE category_id=?", (keep, r["id"]))
            execute("DELETE FROM categories WHERE id=?", (r["id"],))
        else:
            seen[key] = r["id"]
    flash("Bookkeeping categories cleaned.", "success")
    return redirect(url_for("transactions"))


@app.route("/")
def home():
    return redirect(url_for("dashboard") if current_user.is_authenticated else url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    init_db()
    if request.method == "POST":
        email = request.form.get("email", "").lower().strip()
        password = request.form.get("password", "")
        row = query("SELECT * FROM users WHERE lower(email)=? AND is_active=1", (email,), one=True)
        if row and check_password_hash(row["password_hash"], password):
            login_user(User(row))
            flash("Welcome back.", "success")
            return redirect(url_for("dashboard"))
        flash("Invalid email or password.", "danger")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been signed out.", "success")
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    client_filter = "" if current_user.role == "admin" else "WHERE client_id=?"
    args = () if current_user.role == "admin" else (current_user.client_id,)
    income = query(f"SELECT COALESCE(SUM(amount),0) total FROM transactions {client_filter + (' AND' if client_filter else 'WHERE')} type='income'", args, one=True)["total"]
    expenses = query(f"SELECT COALESCE(SUM(amount),0) total FROM transactions {client_filter + (' AND' if client_filter else 'WHERE')} type='expense'", args, one=True)["total"]
    open_invoices = query(f"SELECT COUNT(*) c FROM invoices {client_filter + (' AND' if client_filter else 'WHERE')} status!='Paid'", args, one=True)["c"]
    unpaid = query(f"SELECT COALESCE(SUM(amount),0) total FROM invoices {client_filter + (' AND' if client_filter else 'WHERE')} status!='Paid'", args, one=True)["total"]
    counts = {
        "clients": query("SELECT COUNT(*) c FROM clients", one=True)["c"] if current_user.role == "admin" else 1,
        "documents": query(f"SELECT COUNT(*) c FROM documents {client_filter}", args, one=True)["c"],
        "tax_returns": query(f"SELECT COUNT(*) c FROM tax_returns {client_filter}", args, one=True)["c"],
        "appointments": query(f"SELECT COUNT(*) c FROM appointments {client_filter}", args, one=True)["c"],
        "crm": query("SELECT COUNT(*) c FROM crm_leads", one=True)["c"] if current_user.role == "admin" else 0,
    }
    recent_transactions = query("""SELECT t.*, c.name category_name, cl.name client_name
                                   FROM transactions t
                                   LEFT JOIN categories c ON c.id=t.category_id
                                   LEFT JOIN clients cl ON cl.id=t.client_id
                                   ORDER BY t.date DESC, t.id DESC LIMIT 5""")
    recent_invoices = query("""SELECT i.*, cl.name client_name
                               FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id
                               ORDER BY i.id DESC LIMIT 5""")
    return render_template("dashboard.html", income=income, expenses=expenses, balance=income-expenses,
                           unpaid=unpaid, open_invoices=open_invoices, counts=counts,
                           recent_transactions=recent_transactions, recent_invoices=recent_invoices)


@app.route("/clients", methods=["GET", "POST"])
@login_required
@admin_required
def clients():
    if request.method == "POST":
        execute("""INSERT INTO clients(name, business_name, email, phone, address, client_type, status, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (request.form.get("name"), request.form.get("business_name"), request.form.get("email"),
                 request.form.get("phone"), request.form.get("address"), request.form.get("client_type"),
                 request.form.get("status"), request.form.get("notes")))
        flash("Client saved.", "success")
        return redirect(url_for("clients"))
    rows = query("SELECT * FROM clients ORDER BY name")
    return render_template("clients.html", clients=rows)


@app.route("/transactions", methods=["GET", "POST"])
@login_required
def transactions():
    if request.method == "POST" and current_user.role == "admin":
        execute("""INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (request.form.get("date"), request.form.get("description"), request.form.get("type"),
                 request.form.get("category_id") or None, request.form.get("client_id") or None,
                 money(request.form.get("amount")), request.form.get("notes")))
        flash("Transaction added.", "success")
        return redirect(url_for("transactions"))

    where = "" if current_user.role == "admin" else "WHERE t.client_id=?"
    args = () if current_user.role == "admin" else (current_user.client_id,)
    rows = query(f"""SELECT t.*, c.name category_name, cl.name client_name
                    FROM transactions t
                    LEFT JOIN categories c ON c.id=t.category_id
                    LEFT JOIN clients cl ON cl.id=t.client_id
                    {where}
                    ORDER BY t.date DESC, t.id DESC""", args)
    categories = query("SELECT MIN(id) id, name, kind FROM categories GROUP BY name, kind ORDER BY kind, name")
    clients_rows = query("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("transactions.html", transactions=rows, categories=categories, clients=clients_rows)


@app.route("/invoices", methods=["GET", "POST"])
@login_required
def invoices():
    if request.method == "POST" and current_user.role == "admin":
        inv_number = request.form.get("invoice_number") or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        execute("""INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description, payment_link, clover_link)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (request.form.get("client_id") or None, inv_number, request.form.get("issue_date"),
                 request.form.get("due_date"), money(request.form.get("amount")), request.form.get("status"),
                 request.form.get("description"), normalize_link(request.form.get("payment_link")),
                 normalize_link(request.form.get("payment_link"))))
        flash("Invoice saved.", "success")
        return redirect(url_for("invoices"))

    where = "" if current_user.role == "admin" else "WHERE i.client_id=?"
    args = () if current_user.role == "admin" else (current_user.client_id,)
    rows = query(f"""SELECT i.*, cl.name client_name
                    FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id
                    {where}
                    ORDER BY i.created_at DESC, i.id DESC""", args)
    clients_rows = query("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("invoices.html", invoices=rows, clients=clients_rows)


@app.route("/payments", methods=["GET", "POST"])
@login_required
@admin_required
def payments():
    if request.method == "POST":
        invoice_id = request.form.get("invoice_id")
        action = request.form.get("action")
        if not invoice_id:
            flash("Please select an invoice.", "danger")
            return redirect(url_for("payments"))

        invoice = query("SELECT * FROM invoices WHERE id=?", (invoice_id,), one=True)
        if not invoice:
            flash("Invoice not found.", "danger")
            return redirect(url_for("payments"))

        if action == "save_link":
            clover_link = normalize_link(request.form.get("clover_link"))
            if not clover_link:
                flash("Paste the correct Clover checkout link for this invoice.", "danger")
                return redirect(url_for("payments"))
            execute("UPDATE invoices SET payment_link=?, clover_link=?, payment_provider='Clover', payment_notes=? WHERE id=?",
                    (clover_link, clover_link, "Invoice-specific Clover checkout link saved", invoice_id))
            flash("Clover link saved to this invoice.", "success")
            return redirect(url_for("payments"))

        if action == "pay":
            clover_link = normalize_link(invoice["payment_link"] or invoice["clover_link"] or "")
            if not clover_link:
                flash("Save this invoice's Clover checkout link first.", "danger")
                return redirect(url_for("payments"))
            execute("""INSERT INTO payments(invoice_id, amount, provider, payment_link, status, notes)
                       VALUES (?, ?, 'Clover', ?, 'Checkout Opened', ?)""",
                    (invoice_id, invoice["amount"], clover_link, "Pay Invoice opened Clover checkout"))
            execute("UPDATE invoices SET status='Sent', payment_provider='Clover' WHERE id=? AND status!='Paid'", (invoice_id,))
            return redirect(clover_link)

        if action == "mark_paid":
            clover_link = normalize_link(invoice["payment_link"] or invoice["clover_link"] or "")
            execute("UPDATE invoices SET status='Paid', paid_at=CURRENT_TIMESTAMP, payment_provider='Clover' WHERE id=?", (invoice_id,))
            if not query("SELECT id FROM payments WHERE invoice_id=? AND status='Paid' LIMIT 1", (invoice_id,), one=True):
                execute("""INSERT INTO payments(invoice_id, amount, provider, payment_link, status, notes)
                           VALUES (?, ?, 'Clover', ?, 'Paid', ?)""",
                        (invoice_id, invoice["amount"], clover_link, "Marked paid after Clover checkout"))
            cat = query("SELECT id FROM categories WHERE name='Tax Preparation Income' AND kind='income' LIMIT 1", one=True)
            note = f"Auto-created from paid invoice #{invoice_id}"
            if not query("SELECT id FROM transactions WHERE notes=? LIMIT 1", (note,), one=True):
                execute("""INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                           VALUES (?, ?, 'income', ?, ?, ?, ?)""",
                        (datetime.now().strftime("%Y-%m-%d"), f"Clover payment for invoice #{invoice['invoice_number'] or invoice_id}",
                         cat["id"] if cat else None, invoice["client_id"], invoice["amount"], note))
            flash("Invoice marked paid and bookkeeping income recorded.", "success")
            return redirect(url_for("payments"))

    invoices_rows = query("""SELECT i.*, cl.name client_name, cl.email client_email
                             FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id
                             ORDER BY i.created_at DESC, i.id DESC""")
    payment_rows = query("""SELECT p.*, i.invoice_number, cl.name client_name
                            FROM payments p
                            LEFT JOIN invoices i ON i.id=p.invoice_id
                            LEFT JOIN clients cl ON cl.id=i.client_id
                            ORDER BY p.created_at DESC, p.id DESC LIMIT 50""")
    paid_total = query("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status='Paid'", one=True)["total"]
    unpaid_total = query("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]
    needs_link = query("SELECT COUNT(*) c FROM invoices WHERE status!='Paid' AND (payment_link IS NULL OR payment_link='')", one=True)["c"]
    return render_template("payments.html", invoices=invoices_rows, payments=payment_rows,
                           paid_total=paid_total, unpaid_total=unpaid_total, needs_link=needs_link)


@app.route("/documents", methods=["GET", "POST"])
@login_required
def documents():
    if request.method == "POST":
        file = request.files.get("file")
        filename = ""
        if file and file.filename and allowed_file(file.filename):
            filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{secure_filename(file.filename)}"
            file.save(UPLOAD_DIR / filename)
        client_id = request.form.get("client_id") if current_user.role == "admin" else current_user.client_id
        execute("""INSERT INTO documents(client_id, name, filename, tax_year, status, notes)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (client_id, request.form.get("name"), filename, request.form.get("tax_year"),
                 request.form.get("status"), request.form.get("notes")))
        flash("Document saved.", "success")
        return redirect(url_for("documents"))

    where = "" if current_user.role == "admin" else "WHERE d.client_id=?"
    args = () if current_user.role == "admin" else (current_user.client_id,)
    rows = query(f"""SELECT d.*, cl.name client_name FROM documents d
                    LEFT JOIN clients cl ON cl.id=d.client_id {where}
                    ORDER BY d.created_at DESC""", args)
    clients_rows = query("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("documents.html", documents=rows, clients=clients_rows)


@app.route("/tax-returns", methods=["GET", "POST"])
@login_required
def tax_returns():
    if request.method == "POST" and current_user.role == "admin":
        execute("""INSERT INTO tax_returns(client_id, tax_year, service_type, status, due_date, fee, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (request.form.get("client_id"), request.form.get("tax_year"), request.form.get("service_type"),
                 request.form.get("status"), request.form.get("due_date"), money(request.form.get("fee")),
                 request.form.get("notes")))
        flash("Tax return saved.", "success")
        return redirect(url_for("tax_returns"))
    where = "" if current_user.role == "admin" else "WHERE tr.client_id=?"
    args = () if current_user.role == "admin" else (current_user.client_id,)
    rows = query(f"""SELECT tr.*, cl.name client_name FROM tax_returns tr
                    LEFT JOIN clients cl ON cl.id=tr.client_id {where}
                    ORDER BY tr.tax_year DESC, tr.id DESC""", args)
    clients_rows = query("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("tax_returns.html", returns=rows, clients=clients_rows)


@app.route("/appointments", methods=["GET", "POST"])
@login_required
def appointments():
    if request.method == "POST" and current_user.role == "admin":
        execute("""INSERT INTO appointments(client_id, title, start_at, end_at, location, meeting_link, status, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (request.form.get("client_id"), request.form.get("title"), request.form.get("start_at"),
                 request.form.get("end_at"), request.form.get("location"), request.form.get("meeting_link"),
                 request.form.get("status"), request.form.get("notes")))
        flash("Appointment saved.", "success")
        return redirect(url_for("appointments"))
    where = "" if current_user.role == "admin" else "WHERE a.client_id=?"
    args = () if current_user.role == "admin" else (current_user.client_id,)
    rows = query(f"""SELECT a.*, cl.name client_name FROM appointments a
                    LEFT JOIN clients cl ON cl.id=a.client_id {where}
                    ORDER BY a.start_at DESC""", args)
    clients_rows = query("SELECT * FROM clients ORDER BY name") if current_user.role == "admin" else []
    return render_template("appointments.html", appointments=rows, clients=clients_rows)


@app.route("/crm", methods=["GET", "POST"])
@login_required
@admin_required
def crm():
    if request.method == "POST":
        execute("""INSERT INTO crm_leads(name, phone, email, status, source, follow_up_date, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (request.form.get("name"), request.form.get("phone"), request.form.get("email"),
                 request.form.get("status") or "New", request.form.get("source"), request.form.get("follow_up_date"),
                 request.form.get("notes")))
        flash("CRM lead saved.", "success")
        return redirect(url_for("crm"))
    rows = query("SELECT * FROM crm_leads ORDER BY created_at DESC")
    return render_template("crm.html", leads=rows)


@app.route("/settings", methods=["GET", "POST"])
@login_required
@admin_required
def settings():
    clients_rows = query("SELECT * FROM clients ORDER BY name")
    if request.method == "POST":
        execute("""INSERT INTO users(name, email, password_hash, role, client_id, is_active)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (request.form.get("name"), request.form.get("email").lower().strip(),
                 generate_password_hash(request.form.get("password") or "Temp123!"),
                 request.form.get("role"), request.form.get("client_id") or None))
        flash("User created.", "success")
        return redirect(url_for("settings"))
    users = query("""SELECT u.*, cl.name client_name FROM users u
                    LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.name""")
    return render_template("settings.html", users=users, clients=clients_rows)


@app.route("/reports")
@login_required
@admin_required
def reports():
    income = query("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", one=True)["total"]
    expenses = query("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", one=True)["total"]
    unpaid = query("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]
    paid = query("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status='Paid'", one=True)["total"]
    transactions_rows = query("""SELECT t.*, c.name category_name, cl.name client_name
                                 FROM transactions t
                                 LEFT JOIN categories c ON c.id=t.category_id
                                 LEFT JOIN clients cl ON cl.id=t.client_id
                                 ORDER BY t.date DESC LIMIT 50""")
    invoices_rows = query("""SELECT i.*, cl.name client_name
                             FROM invoices i LEFT JOIN clients cl ON cl.id=i.client_id
                             ORDER BY i.id DESC LIMIT 50""")
    return render_template("reports.html", income=income, expenses=expenses, profit=income-expenses,
                           unpaid=unpaid, paid=paid, transactions=transactions_rows, invoices=invoices_rows)


@app.route("/reports/export/transactions.csv")
@login_required
@admin_required
def export_transactions():
    rows = query("""SELECT t.date, t.description, t.type, c.name category, cl.name client, t.amount, t.notes
                   FROM transactions t
                   LEFT JOIN categories c ON c.id=t.category_id
                   LEFT JOIN clients cl ON cl.id=t.client_id
                   ORDER BY t.date DESC""")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Description", "Type", "Category", "Client", "Amount", "Notes"])
    for r in rows:
        writer.writerow([r["date"], r["description"], r["type"], r["category"], r["client"], r["amount"], r["notes"]])
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=ppt_transactions.csv"})


if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
