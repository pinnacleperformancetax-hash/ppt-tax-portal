from flask import Flask, render_template, request, redirect

app = Flask(__name__)

invoices = [
    {"id": 1, "client": "Client A", "amount": 300, "status": "Unpaid"},
    {"id": 2, "client": "Client B", "amount": 450, "status": "Unpaid"}
]

payments = []

@app.route("/")
def home():
    return redirect("/payments")

@app.route("/payments", methods=["GET", "POST"])
def payments_page():
    global invoices, payments

    if request.method == "POST":
        invoice_id = int(request.form.get("invoice_id"))
        amount = float(request.form.get("amount"))

        for inv in invoices:
            if inv["id"] == invoice_id:
                inv["status"] = "Paid"
                payments.append({
                    "invoice": invoice_id,
                    "amount": amount
                })

        return redirect("/payments")

    paid_total = sum(p["amount"] for p in payments)
    unpaid_total = sum(i["amount"] for i in invoices if i["status"] != "Paid")

    paid_count = len([i for i in invoices if i["status"] == "Paid"])
    unpaid_count = len([i for i in invoices if i["status"] != "Paid"])

    return render_template(
        "payments.html",
        invoices=invoices,
        payments=payments,
        paid_total=paid_total,
        unpaid_total=unpaid_total,
        paid_count=paid_count,
        unpaid_count=unpaid_count
    )

app.run(host="0.0.0.0", port=5000)
