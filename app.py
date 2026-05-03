from __future__ import annotations

import csv
import io
import os
import sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

from flask import Flask, Response, flash, g, redirect, render_template, request, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
INSTANCE_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = INSTANCE_DIR / "ppt_portal.db"
ALLOWED_UPLOADS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "xls", "xlsx", "csv", "txt"}

BRAND = {
    "business_name": "Pinnacle Performance Tax and Accounting",
    "website": "www.pinnacleperformancetax.com",
    "email": "pinnacleperformancetax@gmail.com",
    "phone": "478-338-1632",
}

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
        conn.execute("PRAGMA foreign_keys=ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(error=None):
    conn = g.pop("db", None)
    if conn:
        conn.close()


def query_db(sql: str, args: tuple[Any, ...] = (), one: bool = False):
    cur = get_db().execute(sql, args)
    rows = cur.fetchall()
    return (rows[0] if rows else None) if one else rows


def execute_db(sql: str, args: tuple[Any, ...] = ()) -> int:
    cur = get_db().execute(sql, args)
    get_db().commit()
    return cur.lastrowid


def money(value: Any) -> float:
    try:
        return round(float(str(value or "0").replace("$", "").replace(",", "")), 2)
    except Exception:
        return 0.0


@app.template_filter("currency")
def currency(value):
    return "${:,.2f}".format(money(value))


@app.context_processor
def inject_globals():
    return {"brand": BRAND}


class User(UserMixin):
    def __init__(self, row):
        self.id = str(row["id"])
        self.name = row["name"]
        self.email = row["email"]
        self.role = row["role"]
        self.client_id = row["client_id"]


@login_manager.user_loader
def load_user(user_id: str):
    row = query_db("SELECT * FROM users WHERE id=? AND is_active=1", (user_id,), one=True)
    return User(row) if row else None


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != "admin":
            flash("Admin access required.", "danger")
            return redirect(url_for("dashboard"))
        return fn(*args, **kwargs)
    return wrapper


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_UPLOADS


def init_db() -> None:
    db = get_db()
    db.executescript("""
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
        kind TEXT NOT NULL,
        UNIQUE(name, kind)
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
        paid_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER,
        client_id INTEGER,
        amount REAL DEFAULT 0,
        method TEXT DEFAULT 'Manual Entry',
        reference TEXT,
        status TEXT DEFAULT 'Paid',
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

    CREATE INDEX IF NOT EXISTS idx_transactions_client ON transactions(client_id);
    CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status);
    CREATE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id);
    """)

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

    admin_email = "admin@pinnacleperformancetax.com"
    admin_password = os.environ.get("ADMIN_PASSWORD", "ChangeMe123")
    admin_hash = generate_password_hash(admin_password)
    existing_admin = db.execute("SELECT id FROM users WHERE lower(email)=?", (admin_email,)).fetchone()
    if existing_admin:
        db.execute("UPDATE users SET name=?, password_hash=?, role='admin', is_active=1 WHERE lower(email)=?",
                   ("PPT Admin", admin_hash, admin_email))
    else:
        db.execute("INSERT INTO users(name, email, password_hash, role, is_active) VALUES (?, ?, ?, 'admin', 1)",
                   ("PPT Admin", admin_email, admin_hash))

    if db.execute("SELECT COUNT(*) c FROM clients").fetchone()["c"] == 0:
        db.execute("""INSERT INTO clients(name, business_name, email, phone, client_type, status, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   ("Sample Client", "Sample Business LLC", "client@example.com", "478-555-0110", "Business", "Active", "Demo client"))

    client = db.execute("SELECT id FROM clients WHERE email='client@example.com'").fetchone()
    if client and db.execute("SELECT COUNT(*) c FROM invoices").fetchone()["c"] == 0:
        today = datetime.now().strftime("%Y-%m-%d")
        db.execute("""INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (client["id"], "INV-0001", today, today, 375.00, "Draft", "Tax preparation service"))
    db.commit()


@app.route("/init")
def init_route():
    init_db()
    return "INIT COMPLETE"


@app.route("/")
def home():
    return redirect(url_for("dashboard") if current_user.is_authenticated else url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    init_db()
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        row = query_db("SELECT * FROM users WHERE lower(email)=? AND is_active=1", (email,), one=True)
        if row and check_password_hash(row["password_hash"], password):
            login_user(User(row))
            return redirect(url_for("dashboard"))
        flash("Invalid login.", "danger")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    income = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", one=True)["total"]
    expenses = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", one=True)["total"]
    unpaid = query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"]
    paid = query_db("SELECT COALESCE(SUM(amount),0) total FROM payments WHERE status='Paid'", one=True)["total"]
    counts = {
        "clients": query_db("SELECT COUNT(*) c FROM clients", one=True)["c"],
        "open_invoices": query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'", one=True)["c"],
        "paid_invoices": query_db("SELECT COUNT(*) c FROM invoices WHERE status='Paid'", one=True)["c"],
        "documents": query_db("SELECT COUNT(*) c FROM documents", one=True)["c"],
        "returns": query_db("SELECT COUNT(*) c FROM tax_returns", one=True)["c"],
        "appointments": query_db("SELECT COUNT(*) c FROM appointments", one=True)["c"],
        "leads": query_db("SELECT COUNT(*) c FROM crm_leads", one=True)["c"],
    }
    recent_transactions = query_db("""SELECT t.*, c.name category_name, cl.name client_name
                                      FROM transactions t
                                      LEFT JOIN categories c ON c.id=t.category_id
                                      LEFT JOIN clients cl ON cl.id=t.client_id
                                      ORDER BY t.id DESC LIMIT 5""")
    recent_invoices = query_db("""SELECT i.*, cl.name client_name FROM invoices i
                                  LEFT JOIN clients cl ON cl.id=i.client_id
                                  ORDER BY i.id DESC LIMIT 5""")
    return render_template("dashboard.html", income=income, expenses=expenses, balance=income-expenses,
                           unpaid=unpaid, paid=paid, counts=counts,
                           recent_transactions=recent_transactions, recent_invoices=recent_invoices)


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
        flash("Client saved.", "success")
        return redirect(url_for("clients"))
    return render_template("clients.html", clients=query_db("SELECT * FROM clients ORDER BY name"))


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

    rows = query_db("""SELECT t.*, c.name category_name, cl.name client_name
                       FROM transactions t
                       LEFT JOIN categories c ON c.id=t.category_id
                       LEFT JOIN clients cl ON cl.id=t.client_id
                       ORDER BY t.date DESC, t.id DESC LIMIT 300""")
    return render_template("transactions.html", transactions=rows,
                           categories=query_db("SELECT * FROM categories ORDER BY kind, name"),
                           clients=query_db("SELECT id, name FROM clients ORDER BY name"))


@app.route("/invoices", methods=["GET", "POST"])
@login_required
def invoices():
    if request.method == "POST" and current_user.role == "admin":
        inv_num = request.form.get("invoice_number") or f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        execute_db("""INSERT INTO invoices(client_id, invoice_number, issue_date, due_date, amount, status, description)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id"), inv_num, request.form.get("issue_date"),
                    request.form.get("due_date"), money(request.form.get("amount")),
                    request.form.get("status"), request.form.get("description")))
        flash("Invoice saved.", "success")
        return redirect(url_for("invoices"))

    rows = query_db("""SELECT i.*, cl.name client_name FROM invoices i
                       LEFT JOIN clients cl ON cl.id=i.client_id
                       ORDER BY i.id DESC LIMIT 300""")
    return render_template("invoices.html", invoices=rows, clients=query_db("SELECT id, name FROM clients ORDER BY name"))


@app.route("/payments", methods=["GET", "POST"])
@login_required
@admin_required
def payments():
    if request.method == "POST":
        invoice_id = request.form.get("invoice_id")
        invoice = query_db("SELECT * FROM invoices WHERE id=?", (invoice_id,), one=True)
        if not invoice:
            flash("Select a valid invoice.", "danger")
            return redirect(url_for("payments"))

        amount = money(request.form.get("amount")) or money(invoice["amount"])
        method = request.form.get("method") or "Manual Entry"
        reference = request.form.get("reference")
        notes = request.form.get("notes")

        execute_db("""INSERT INTO payments(invoice_id, client_id, amount, method, reference, status, notes)
                      VALUES (?, ?, ?, ?, ?, 'Paid', ?)""",
                   (invoice_id, invoice["client_id"], amount, method, reference, notes))
        execute_db("UPDATE invoices SET status='Paid', paid_at=CURRENT_TIMESTAMP WHERE id=?", (invoice_id,))

        cat = query_db("SELECT id FROM categories WHERE name='Tax Preparation Income' AND kind='income'", one=True)
        execute_db("""INSERT INTO transactions(date, description, type, category_id, client_id, amount, notes)
                      VALUES (?, ?, 'income', ?, ?, ?, ?)""",
                   (datetime.now().strftime("%Y-%m-%d"),
                    f"Payment received for invoice {invoice['invoice_number'] or invoice_id}",
                    cat["id"] if cat else None, invoice["client_id"], amount,
                    f"Payment record for invoice #{invoice_id}"))
        flash("Payment recorded. Invoice marked paid. Bookkeeping income added.", "success")
        return redirect(url_for("payments"))

    invoices = query_db("""SELECT i.*, cl.name client_name FROM invoices i
                           LEFT JOIN clients cl ON cl.id=i.client_id
                           ORDER BY i.status='Paid', i.id DESC LIMIT 300""")
    payments_rows = query_db("""SELECT p.*, i.invoice_number, cl.name client_name
                                FROM payments p
                                LEFT JOIN invoices i ON i.id=p.invoice_id
                                LEFT JOIN clients cl ON cl.id=p.client_id
                                ORDER BY p.id DESC LIMIT 100""")
    return render_template("payments.html",
                           invoices=invoices,
                           payments=payments_rows,
                           paid_total=query_db("SELECT COALESCE(SUM(amount),0) total FROM payments WHERE status='Paid'", one=True)["total"],
                           unpaid_total=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"],
                           paid_invoice_count=query_db("SELECT COUNT(*) c FROM invoices WHERE status='Paid'", one=True)["c"],
                           unpaid_invoice_count=query_db("SELECT COUNT(*) c FROM invoices WHERE status!='Paid'", one=True)["c"])


@app.route("/documents", methods=["GET", "POST"])
@login_required
def documents():
    if request.method == "POST":
        file = request.files.get("file")
        filename = ""
        if file and file.filename and allowed_file(file.filename):
            filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{secure_filename(file.filename)}"
            file.save(UPLOAD_DIR / filename)
        execute_db("""INSERT INTO documents(client_id, name, filename, tax_year, status, notes)
                      VALUES (?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id"), request.form.get("name"), filename,
                    request.form.get("tax_year"), request.form.get("status"), request.form.get("notes")))
        flash("Document saved.", "success")
        return redirect(url_for("documents"))
    rows = query_db("""SELECT d.*, cl.name client_name FROM documents d
                       LEFT JOIN clients cl ON cl.id=d.client_id
                       ORDER BY d.id DESC LIMIT 300""")
    return render_template("documents.html", documents=rows, clients=query_db("SELECT id, name FROM clients ORDER BY name"))


@app.route("/tax-returns", methods=["GET", "POST"])
@login_required
def tax_returns():
    if request.method == "POST" and current_user.role == "admin":
        execute_db("""INSERT INTO tax_returns(client_id, tax_year, service_type, status, due_date, fee, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id"), request.form.get("tax_year"), request.form.get("service_type"),
                    request.form.get("status"), request.form.get("due_date"), money(request.form.get("fee")),
                    request.form.get("notes")))
        flash("Tax return saved.", "success")
        return redirect(url_for("tax_returns"))
    rows = query_db("""SELECT tr.*, cl.name client_name FROM tax_returns tr
                       LEFT JOIN clients cl ON cl.id=tr.client_id
                       ORDER BY tr.id DESC LIMIT 300""")
    return render_template("tax_returns.html", returns=rows, clients=query_db("SELECT id, name FROM clients ORDER BY name"))


@app.route("/appointments", methods=["GET", "POST"])
@login_required
def appointments():
    if request.method == "POST" and current_user.role == "admin":
        execute_db("""INSERT INTO appointments(client_id, title, start_at, end_at, location, meeting_link, status, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("client_id"), request.form.get("title"), request.form.get("start_at"),
                    request.form.get("end_at"), request.form.get("location"), request.form.get("meeting_link"),
                    request.form.get("status"), request.form.get("notes")))
        flash("Appointment saved.", "success")
        return redirect(url_for("appointments"))
    rows = query_db("""SELECT a.*, cl.name client_name FROM appointments a
                       LEFT JOIN clients cl ON cl.id=a.client_id
                       ORDER BY a.id DESC LIMIT 300""")
    return render_template("appointments.html", appointments=rows, clients=query_db("SELECT id, name FROM clients ORDER BY name"))


@app.route("/crm", methods=["GET", "POST"])
@login_required
@admin_required
def crm():
    if request.method == "POST":
        execute_db("""INSERT INTO crm_leads(name, phone, email, status, source, follow_up_date, notes)
                      VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   (request.form.get("name"), request.form.get("phone"), request.form.get("email"),
                    request.form.get("status"), request.form.get("source"), request.form.get("follow_up_date"),
                    request.form.get("notes")))
        flash("Lead saved.", "success")
        return redirect(url_for("crm"))
    return render_template("crm.html", leads=query_db("SELECT * FROM crm_leads ORDER BY id DESC LIMIT 300"))


@app.route("/settings", methods=["GET", "POST"])
@login_required
@admin_required
def settings():
    if request.method == "POST":
        execute_db("""INSERT INTO users(name, email, password_hash, role, client_id, is_active)
                      VALUES (?, ?, ?, ?, ?, 1)""",
                   (request.form.get("name"), request.form.get("email").lower().strip(),
                    generate_password_hash(request.form.get("password") or "Temp123!"),
                    request.form.get("role"), request.form.get("client_id") or None))
        flash("User created.", "success")
        return redirect(url_for("settings"))
    users = query_db("""SELECT u.*, cl.name client_name FROM users u
                       LEFT JOIN clients cl ON cl.id=u.client_id ORDER BY u.id DESC""")
    return render_template("settings.html", users=users, clients=query_db("SELECT id, name FROM clients ORDER BY name"))


@app.route("/reports")
@login_required
@admin_required
def reports():
    income = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'", one=True)["total"]
    expenses = query_db("SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'", one=True)["total"]
    return render_template("reports.html",
                           income=income,
                           expenses=expenses,
                           profit=income-expenses,
                           paid=query_db("SELECT COALESCE(SUM(amount),0) total FROM payments WHERE status='Paid'", one=True)["total"],
                           unpaid=query_db("SELECT COALESCE(SUM(amount),0) total FROM invoices WHERE status!='Paid'", one=True)["total"],
                           transactions=query_db("""SELECT t.*, cl.name client_name FROM transactions t
                                                    LEFT JOIN clients cl ON cl.id=t.client_id
                                                    ORDER BY t.id DESC LIMIT 100"""),
                           invoices=query_db("""SELECT i.*, cl.name client_name FROM invoices i
                                                LEFT JOIN clients cl ON cl.id=i.client_id
                                                ORDER BY i.id DESC LIMIT 100"""))


@app.route("/reports/export/transactions.csv")
@login_required
@admin_required
def export_transactions():
    rows = query_db("""SELECT t.date, t.description, t.type, cl.name client, t.amount, t.notes
                      FROM transactions t LEFT JOIN clients cl ON cl.id=t.client_id
                      ORDER BY t.id DESC""")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Description", "Type", "Client", "Amount", "Notes"])
    for r in rows:
        writer.writerow([r["date"], r["description"], r["type"], r["client"], r["amount"], r["notes"]])
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=ppt_transactions.csv"})


if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
