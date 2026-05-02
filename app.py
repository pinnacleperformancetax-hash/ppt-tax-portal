# --- PERFORMANCE FIX: REMOVE SLOW BOOTSTRAP ---
# DELETE THIS FROM YOUR OLD FILE:
# @app.before_request
# def bootstrap_database():
#     if request.endpoint not in {"static"}:
#         init_db()


# --- ADD THIS INSTEAD (runs once safely) ---
@app.before_first_request
def startup():
    init_db()


# --- FIX DASHBOARD ROUTES ---
@app.route("/dashboard")
@app.route("/")
@login_required
def dashboard():
    client_filter = None if current_user.role == "admin" else current_user.client_id
    where = "" if client_filter is None else "WHERE client_id = ?"
    args = () if client_filter is None else (client_filter,)

    income = query_db(
        f"SELECT COALESCE(SUM(amount),0) total FROM transactions {where} AND type='income'"
        if where else "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='income'",
        args if where else (), one=True)["total"]

    expenses = query_db(
        f"SELECT COALESCE(SUM(amount),0) total FROM transactions {where} AND type='expense'"
        if where else "SELECT COALESCE(SUM(amount),0) total FROM transactions WHERE type='expense'",
        args if where else (), one=True)["total"]

    return render_template("dashboard.html", income=income, expenses=expenses)


# --- FIX TAX RETURN ROUTES ---
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
        return redirect("/tax-returns")

    rows = query_db(
        "SELECT * FROM tax_returns ORDER BY id DESC"
    )

    clients_rows = query_db("SELECT * FROM clients ORDER BY name")

    return render_template(
        "tax_returns.html",
        returns=rows,
        clients=clients_rows
    )
