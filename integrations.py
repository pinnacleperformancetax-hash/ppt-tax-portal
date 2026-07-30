# ============================================================
# PPT INTEGRATIONS PACK
# Setmore Webhook | Google Voice Missed-Call Text-Back |
# SMS via Twilio Free | Appointment Auto-Confirm |
# Review Request Automation | Off-Season Campaign Sequences
# ============================================================
# PASTE THIS ENTIRE BLOCK INTO YOUR app.py
# Place it just before the final if __name__ == '__main__': block
# ============================================================


# ── ENVIRONMENT VARIABLES NEEDED (add to Render dashboard) ──
# TWILIO_ACCOUNT_SID  = your Twilio SID (free trial works)
# TWILIO_AUTH_TOKEN   = your Twilio auth token
# TWILIO_PHONE        = your Twilio phone number e.g. +14785550100
# BUSINESS_PHONE      = your real phone e.g. +14783381632
# SENDGRID_API_KEY    = already set
# GOOGLE_VOICE_EMAIL  = the Gmail that receives GV missed-call alerts
# GOOGLE_VOICE_APP_PW = Gmail App Password (not your regular password)
# ────────────────────────────────────────────────────────────


import json as _json
import threading
import smtplib
from email.mime.text import MIMEText


# ── HELPER: SEND SMS VIA TWILIO ──────────────────────────────

def send_sms(to_number, message):
    """
    Send an SMS via Twilio. Returns True on success.
    Free Twilio trial works fine — just verify the destination number first.
    """
    try:
        import urllib.request, urllib.parse, base64
        account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        auth_token  = os.environ.get("TWILIO_AUTH_TOKEN", "")
        from_number = os.environ.get("TWILIO_PHONE", "")
        if not all([account_sid, auth_token, from_number, to_number]):
            return False
        payload = urllib.parse.urlencode({
            "To":   to_number,
            "From": from_number,
            "Body": message,
        }).encode("utf-8")
        url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
        credentials = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Authorization": f"Basic {credentials}",
                     "Content-Type": "application/x-www-form-urlencoded"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[SMS] Error: {e}")
        return False


def notify_owner_sms(message):
    """Send an SMS alert to the business owner."""
    business_phone = os.environ.get("BUSINESS_PHONE", "")
    if business_phone:
        send_sms(business_phone, message)


# ── SETMORE WEBHOOK ──────────────────────────────────────────
# In Setmore: Apps & Integrations → Webhooks → New Webhook
# URL: https://ppt-tax-portal.onrender.com/webhook/setmore
# Events: booking_created, booking_updated, booking_cancelled

@app.route("/webhook/setmore", methods=["POST"])
def setmore_webhook():
    """
    Receives booking events from Setmore and:
    1. Creates or updates a CRM lead
    2. Creates an appointment record in the portal
    3. Sends a confirmation SMS to the client
    4. Sends an SMS alert to the business owner
    5. Sends a confirmation email to the client
    """
    try:
        data = request.json or {}

        # Setmore sends different structures — handle both v1 and v2
        event_type = data.get("event_type") or data.get("type") or "booking_created"

        # Extract client info (Setmore field names vary by plan)
        customer = data.get("customer") or data.get("client") or {}
        name  = (customer.get("name")
                 or customer.get("customer_name")
                 or data.get("customer_name", "")).strip()
        email = (customer.get("email")
                 or customer.get("customer_email")
                 or data.get("customer_email", "")).strip()
        phone = (customer.get("phone")
                 or customer.get("customer_phone")
                 or data.get("customer_phone", "")).strip()

        # Appointment details
        appt = data.get("appointment") or data
        service_name = (appt.get("service_name")
                        or appt.get("service")
                        or "Tax Appointment")
        start_time   = (appt.get("start_time")
                        or appt.get("start_datetime")
                        or appt.get("start", ""))
        end_time     = (appt.get("end_time")
                        or appt.get("end_datetime")
                        or appt.get("end", ""))
        staff_name   = (appt.get("staff_name")
                        or appt.get("staff", {}).get("name", "PPT Team"))
        location     = appt.get("location", "Pinnacle Performance Tax")
        notes        = (customer.get("comment")
                        or customer.get("notes")
                        or data.get("notes", ""))

        with app.app_context():
            # 1. Find or create CRM lead
            existing_lead = None
            if email:
                existing_lead = query_db(
                    "SELECT id FROM crm_leads WHERE LOWER(TRIM(email))=LOWER(TRIM(?))",
                    (email,), one=True
                )

            if event_type in ("booking_created", "new_booking") or not existing_lead:
                if not existing_lead and name:
                    execute_db(
                        "INSERT INTO crm_leads(name,phone,email,status,source,notes) "
                        "VALUES (?,?,?,?,?,?)",
                        (name, phone, email, "New",
                         "Setmore Booking",
                         f"Booked: {service_name} @ {start_time}. {notes}")
                    )

            # 2. Find or create client record
            client_id = None
            if email:
                existing_client = query_db(
                    "SELECT id FROM clients WHERE LOWER(TRIM(email))=LOWER(TRIM(?))",
                    (email,), one=True
                )
                if existing_client:
                    client_id = existing_client["id"]
                elif name:
                    client_id = execute_db(
                        "INSERT INTO clients(name,email,phone,status,notes) "
                        "VALUES (?,?,?,?,?)",
                        (name, email, phone, "New",
                         f"Auto-created from Setmore booking {start_time}")
                    )

            # 3. Create appointment record in portal
            if client_id:
                status_map = {
                    "booking_created":   "Scheduled",
                    "new_booking":       "Scheduled",
                    "booking_updated":   "Updated",
                    "booking_cancelled": "Cancelled",
                    "booking_deleted":   "Cancelled",
                }
                appt_status = status_map.get(event_type, "Scheduled")

                execute_db(
                    "INSERT INTO appointments(client_id,title,start_at,end_at,"
                    "location,status,notes) VALUES (?,?,?,?,?,?,?)",
                    (client_id, service_name, start_time, end_time,
                     location, appt_status,
                     f"Booked via Setmore. Staff: {staff_name}. {notes}")
                )

                # Push in-portal notification
                push_notification(
                    client_id, "appointment",
                    f"Appointment confirmed: {service_name} on {start_time}",
                    "/my/appointments"
                )

            # 4. Send client confirmation SMS
            if phone and event_type in ("booking_created", "new_booking"):
                send_sms(
                    phone,
                    f"Hi {name.split()[0] if name else 'there'}! Your appointment for "
                    f"{service_name} on {start_time} is confirmed. "
                    f"Questions? Call 478-338-1632. "
                    f"— Pinnacle Performance Tax"
                )
            elif phone and event_type == "booking_cancelled":
                send_sms(
                    phone,
                    f"Hi {name.split()[0] if name else 'there'}, your appointment on "
                    f"{start_time} has been cancelled. "
                    f"Call 478-338-1632 to reschedule. "
                    f"— Pinnacle Performance Tax"
                )

            # 5. Owner SMS alert
            notify_owner_sms(
                f"[PPT] New booking: {name} | {service_name} | {start_time} | "
                f"📞 {phone} | ✉️ {email}"
            )

            # 6. Confirmation email
            if email and event_type in ("booking_created", "new_booking"):
                send_email(
                    email,
                    f"Appointment Confirmed — {service_name}",
                    f"""<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
                    <div style="background:#11823b;padding:20px;border-radius:12px 12px 0 0">
                      <h2 style="color:white;margin:0">Appointment Confirmed ✅</h2>
                    </div>
                    <div style="background:#f9fafb;padding:24px;border-radius:0 0 12px 12px;
                                border:1px solid #e5e7eb">
                      <p>Hi {name},</p>
                      <p>Your appointment is confirmed!</p>
                      <div style="background:white;border-radius:10px;padding:16px;
                                  margin:16px 0;border:1px solid #e5e7eb">
                        <div style="margin-bottom:8px">
                          <span style="color:#6b7280">Service: </span>
                          <strong>{service_name}</strong>
                        </div>
                        <div style="margin-bottom:8px">
                          <span style="color:#6b7280">When: </span>
                          <strong>{start_time}</strong>
                        </div>
                        <div>
                          <span style="color:#6b7280">Location: </span>
                          <strong>{location}</strong>
                        </div>
                      </div>
                      <p>Please log in to your client portal to upload documents
                         before your appointment:</p>
                      <p><a href="https://ppt-tax-portal.onrender.com"
                            style="background:#11823b;color:white;padding:10px 20px;
                                   border-radius:8px;text-decoration:none;font-weight:900">
                        Access Client Portal →</a></p>
                      <p style="font-size:12px;color:#9ca3af">
                        Pinnacle Performance Tax and Accounting<br>
                        478-338-1632 | pinnacleperformancetax@gmail.com
                      </p>
                    </div></div>"""
                )

        return _json.dumps({"ok": True}), 200, {"Content-Type": "application/json"}

    except Exception as e:
        print(f"[Setmore Webhook] Error: {e}")
        return _json.dumps({"ok": False, "error": str(e)}), 200, \
               {"Content-Type": "application/json"}


# ── GOOGLE VOICE MISSED-CALL TEXT-BACK ──────────────────────
# Google Voice sends an email when you miss a call.
# This endpoint is called by a small Gmail polling script (below).
# You can also POST to it manually or via Zapier/Make free tier.
#
# OPTION A (Recommended — free):
#   Run poll_google_voice_emails() on a schedule using Render's
#   free background worker, OR call /poll-missed-calls from a
#   cron job or UptimeRobot (free) every 5 minutes.
#
# OPTION B: Use Zapier free tier:
#   Trigger: Gmail — new email matching "Missed call"
#   Action:  Webhook POST to /webhook/missed-call
#             Body: { "caller_name": "...", "caller_phone": "..." }

@app.route("/webhook/missed-call", methods=["POST"])
def missed_call_webhook():
    """
    Receives a missed-call notification and sends an automatic text-back.
    Can be triggered by Zapier, Make, or your own Gmail poller.
    """
    try:
        data = request.json or {}
        caller_name  = data.get("caller_name", "").strip()
        caller_phone = data.get("caller_phone", "").strip()
        caller_email = data.get("caller_email", "").strip()
        notes        = data.get("notes", "")

        if not caller_phone:
            return _json.dumps({"ok": False, "error": "No phone"}), 400, \
                   {"Content-Type": "application/json"}

        # Clean up phone number
        digits = "".join(c for c in caller_phone if c.isdigit())
        if len(digits) == 10:
            caller_phone = "+1" + digits
        elif len(digits) == 11 and digits.startswith("1"):
            caller_phone = "+" + digits

        with app.app_context():
            # 1. Send instant text-back
            first = caller_name.split()[0] if caller_name else "there"
            text_sent = send_sms(
                caller_phone,
                f"Hi {first}! Sorry we missed your call. "
                f"This is Pinnacle Performance Tax — we'll call you back shortly. "
                f"You can also book online: https://ppt-tax-portal.onrender.com "
                f"or reply to this message. 📞 478-338-1632"
            )

            # 2. Add to CRM as new lead
            execute_db(
                "INSERT INTO crm_leads(name,phone,email,status,source,notes) "
                "VALUES (?,?,?,?,?,?)",
                (caller_name or "Unknown Caller",
                 caller_phone,
                 caller_email,
                 "New",
                 "Missed Call — Google Voice",
                 f"Missed call auto-text sent: {text_sent}. {notes}")
            )

            # 3. Alert owner
            notify_owner_sms(
                f"[PPT] Missed call from {caller_name or 'unknown'} "
                f"{caller_phone}. Auto text-back {'sent ✅' if text_sent else 'FAILED ❌'}."
            )

            # 4. Dashboard notification (no client_id yet)
            execute_db(
                "INSERT INTO notifications(type,message,link) VALUES (?,?,?)",
                ("crm",
                 f"Missed call from {caller_name or caller_phone} — auto text sent",
                 "/crm")
            )

        return _json.dumps({"ok": True, "sms_sent": text_sent}), 200, \
               {"Content-Type": "application/json"}

    except Exception as e:
        print(f"[Missed Call Webhook] Error: {e}")
        return _json.dumps({"ok": False, "error": str(e)}), 500, \
               {"Content-Type": "application/json"}


# ── GMAIL POLLER FOR GOOGLE VOICE MISSED-CALL EMAILS ─────────
# This reads your Gmail inbox for Google Voice "Missed call" emails
# and automatically triggers the missed-call text-back.
# Call /poll-missed-calls every 5 minutes via UptimeRobot (free).

def poll_google_voice_emails():
    """
    Poll Gmail for Google Voice missed-call notification emails.
    Requires GOOGLE_VOICE_EMAIL and GOOGLE_VOICE_APP_PW env vars.
    Uses IMAP — no Google API needed.
    """
    import imaplib
    import email as _email
    import re

    gv_email  = os.environ.get("GOOGLE_VOICE_EMAIL", "")
    gv_app_pw = os.environ.get("GOOGLE_VOICE_APP_PW", "")
    if not gv_email or not gv_app_pw:
        return {"checked": False, "reason": "Missing GOOGLE_VOICE_EMAIL or GOOGLE_VOICE_APP_PW"}

    processed = 0
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(gv_email, gv_app_pw)
        mail.select("inbox")

        # Search for unread Google Voice missed-call emails
        _, ids = mail.search(None, '(UNSEEN FROM "voice-noreply@google.com" SUBJECT "Missed call")')
        email_ids = ids[0].split()

        for eid in email_ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            msg = _email.message_from_bytes(msg_data[0][1])

            # Extract body text
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        break
            else:
                body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")

            # Extract phone number from GV email body
            # GV format: "John Smith called at (478) 555-0100"
            phone_match = re.search(
                r'\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}', body
            )
            name_match = re.search(r'^(.+?) called', body, re.MULTILINE)

            caller_phone = phone_match.group(0) if phone_match else ""
            caller_name  = name_match.group(1).strip() if name_match else ""

            if caller_phone:
                # Trigger the missed-call handler internally
                with app.test_request_context(
                    "/webhook/missed-call",
                    method="POST",
                    json={"caller_name": caller_name,
                          "caller_phone": caller_phone}
                ):
                    missed_call_webhook()
                processed += 1

            # Mark as read so we don't process it again
            mail.store(eid, "+FLAGS", "\\Seen")

        mail.logout()
        return {"checked": True, "processed": processed}

    except Exception as e:
        return {"checked": False, "error": str(e)}


@app.route("/poll-missed-calls")
def poll_missed_calls_route():
    """
    Trigger Gmail polling for missed-call emails.
    Point UptimeRobot at this URL every 5 minutes (free tier).
    Or call it manually from admin.
    """
    # Basic security — only allow from known IPs or with a token
    token = request.args.get("token", "")
    expected = os.environ.get("POLL_TOKEN", "ppt2025")
    if token != expected:
        return "Unauthorized", 403

    result = poll_google_voice_emails()
    return _json.dumps(result), 200, {"Content-Type": "application/json"}


# ── REVIEW REQUEST AUTOMATION ────────────────────────────────
# Sends a Google Review request SMS + email after a tax return
# is marked Complete. Called automatically when status changes.

GOOGLE_REVIEW_LINK = "https://g.page/r/CRerC93nZyzREBM/review"

def send_review_request(client_id):
    """Send a review request to a client after completing their return."""
    try:
        with app.app_context():
            client = query_db("SELECT * FROM clients WHERE id=?", (client_id,), one=True)
            if not client:
                return
            first = (client["name"] or "").split()[0] or "there"

            # SMS review request
            if client.get("phone"):
                send_sms(
                    client["phone"],
                    f"Hi {first}! Thank you for choosing Pinnacle Performance Tax. "
                    f"Could you take 30 seconds to leave us a Google review? "
                    f"It helps us so much! {GOOGLE_REVIEW_LINK} "
                    f"— Timeeka at PPT 🙏"
                )

            # Email review request
            if client.get("email"):
                send_email(
                    client["email"],
                    "Thank you! Could you leave us a quick review?",
                    f"""<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
                    <div style="background:#11823b;padding:20px;border-radius:12px 12px 0 0;
                                text-align:center">
                      <h2 style="color:white;margin:0">Thank You, {first}! 🙏</h2>
                    </div>
                    <div style="background:#f9fafb;padding:28px;border-radius:0 0 12px 12px;
                                border:1px solid #e5e7eb;text-align:center">
                      <p style="font-size:16px">It was a pleasure serving you this tax season!</p>
                      <p>If you had a great experience, would you mind leaving us a
                         quick Google review? It only takes 30 seconds and helps us
                         reach more clients like you.</p>
                      <a href="{GOOGLE_REVIEW_LINK}"
                         style="display:inline-block;background:#11823b;color:white;
                                padding:14px 28px;border-radius:10px;font-weight:900;
                                font-size:16px;text-decoration:none;margin:16px 0">
                        ⭐ Leave a Google Review
                      </a>
                      <p style="font-size:13px;color:#475569;margin-top:20px">
                        Thank you for trusting us with your taxes.<br>
                        <strong>Pinnacle Performance Tax and Accounting</strong><br>
                        478-338-1632 | pinnacleperformancetax@gmail.com
                      </p>
                    </div></div>"""
                )

            # Log it
            push_notification(
                client_id, "message",
                "Review request sent — thank you for your business!",
                "/my/tax-returns"
            )
    except Exception as e:
        print(f"[Review Request] Error: {e}")


# Hook into tax return status updates to auto-send review requests
@app.route("/tax-returns/<int:return_id>/complete", methods=["POST"])
@login_required
@admin_required
def mark_return_complete(return_id):
    """Mark a tax return as complete and auto-send a review request."""
    row = query_db("SELECT * FROM tax_returns WHERE id=?", (return_id,), one=True)
    if not row:
        abort(404)
    execute_db(
        "UPDATE tax_returns SET status='Complete', workflow_stage='Complete', "
        "completed_at=CURRENT_TIMESTAMP WHERE id=?",
        (return_id,)
    )
    # Send review request in background so the page loads fast
    t = threading.Thread(target=send_review_request, args=(row["client_id"],))
    t.daemon = True
    t.start()
    flash("Return marked complete and review request sent to client! ⭐", "success")
    return redirect(request.referrer or url_for("tax_returns"))


# ── OFF-SEASON EMAIL CAMPAIGN SEQUENCES ──────────────────────
# Send targeted campaigns to past filers during the off-season.
# These keep PPT top-of-mind year-round.

def ensure_campaign_tables():
    db = get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS email_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        subject TEXT NOT NULL,
        body TEXT NOT NULL,
        campaign_type TEXT DEFAULT 'General',
        scheduled_date TEXT,
        status TEXT DEFAULT 'Draft',
        sent_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS campaign_recipients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER,
        client_id INTEGER,
        sent_at TEXT,
        status TEXT DEFAULT 'Pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    db.commit()


@app.route("/admin/campaigns", methods=["GET", "POST"])
@login_required
@admin_required
def admin_campaigns():
    """Off-season email campaign manager."""
    ensure_campaign_tables()
    clients = query_db("SELECT id,name,email FROM clients WHERE status='Active' AND email IS NOT NULL AND email!='' ORDER BY name")

    if request.method == "POST":
        action = request.form.get("action")

        if action == "create":
            execute_db(
                "INSERT INTO email_campaigns(name,subject,body,campaign_type,scheduled_date,status) "
                "VALUES (?,?,?,?,?,'Draft')",
                (request.form.get("name"),
                 request.form.get("subject"),
                 request.form.get("body"),
                 request.form.get("campaign_type") or "General",
                 request.form.get("scheduled_date"))
            )
            flash("Campaign saved as draft.", "success")

        elif action == "send_now":
            campaign_id = request.form.get("campaign_id")
            campaign = query_db("SELECT * FROM email_campaigns WHERE id=?", (campaign_id,), one=True)
            if not campaign:
                abort(404)
            selected_ids = request.form.getlist("client_ids")
            if not selected_ids:
                flash("No clients selected.", "danger")
                return redirect(url_for("admin_campaigns"))

            sent = 0
            for cid in selected_ids:
                client = query_db("SELECT * FROM clients WHERE id=?", (cid,), one=True)
                if not client or not client.get("email"):
                    continue
                # Personalize the email
                personalized_body = (
                    campaign["body"]
                    .replace("{{name}}", (client["name"] or "").split()[0])
                    .replace("{{full_name}}", client["name"] or "")
                    .replace("{{business}}", client["business_name"] or "")
                )
                if send_email(client["email"], campaign["subject"], personalized_body):
                    execute_db(
                        "INSERT INTO campaign_recipients(campaign_id,client_id,sent_at,status) "
                        "VALUES (?,?,CURRENT_TIMESTAMP,'Sent')",
                        (campaign_id, cid)
                    )
                    sent += 1

            execute_db(
                "UPDATE email_campaigns SET status='Sent', sent_count=sent_count+? WHERE id=?",
                (sent, campaign_id)
            )
            flash(f"Campaign sent to {sent} clients! 📧", "success")

        elif action == "delete":
            execute_db("DELETE FROM email_campaigns WHERE id=?", (request.form.get("campaign_id"),))
            flash("Campaign deleted.", "success")

        return redirect(url_for("admin_campaigns"))

    campaigns = query_db("SELECT * FROM email_campaigns ORDER BY id DESC")
    return render_template_string(CAMPAIGNS_HTML,
                                  campaigns=campaigns, clients=clients)


CAMPAIGNS_HTML = """{%extends"base.html"%}{%block content%}
<h1>📧 Email Campaigns</h1>
<p class="sub">Off-season campaigns to stay top-of-mind with past clients year-round.</p>

<div class="card">
<h2 style="margin-top:0">Quick-Start Templates</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px">

<form method="POST">
<input type="hidden" name="action" value="create">
<input type="hidden" name="name" value="Bookkeeping Offer — Off Season">
<input type="hidden" name="campaign_type" value="Bookkeeping">
<input type="hidden" name="subject" value="Stay organized year-round — bookkeeping help from PPT">
<input type="hidden" name="body" value="&lt;div style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto'&gt;&lt;div style='background:#11823b;padding:20px;border-radius:12px 12px 0 0'&gt;&lt;h2 style='color:white;margin:0'&gt;Year-Round Bookkeeping Support&lt;/h2&gt;&lt;/div&gt;&lt;div style='background:#f9fafb;padding:24px;border-radius:0 0 12px 12px;border:1px solid #e5e7eb'&gt;&lt;p&gt;Hi {{name}},&lt;/p&gt;&lt;p&gt;Tax season may be over, but staying organized year-round makes next year so much easier — and could save you thousands in deductions.&lt;/p&gt;&lt;p&gt;We're offering monthly bookkeeping services to help you:&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Track income and expenses automatically&lt;/li&gt;&lt;li&gt;Stay ready for tax season&lt;/li&gt;&lt;li&gt;Get monthly P&amp;L reports&lt;/li&gt;&lt;/ul&gt;&lt;p&gt;&lt;a href='https://ppt-tax-portal.onrender.com' style='background:#11823b;color:white;padding:10px 20px;border-radius:8px;text-decoration:none;font-weight:900'&gt;Get Started →&lt;/a&gt;&lt;/p&gt;&lt;p style='font-size:12px;color:#9ca3af'&gt;Pinnacle Performance Tax | 478-338-1632&lt;/p&gt;&lt;/div&gt;&lt;/div&gt;">
<button type="submit" style="width:100%;padding:12px;text-align:left;background:#f0fdf4;color:#0b5f2a;border:2px solid #bbf7d0;border-radius:12px;font-weight:900;cursor:pointer">
📚 Bookkeeping Offer
</button></form>

<form method="POST">
<input type="hidden" name="action" value="create">
<input type="hidden" name="name" value="Q2 Estimated Tax Reminder">
<input type="hidden" name="campaign_type" value="Quarterly">
<input type="hidden" name="subject" value="⏰ Reminder: Q2 estimated tax payment due June 15">
<input type="hidden" name="body" value="&lt;div style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto'&gt;&lt;div style='background:#f59e0b;padding:20px;border-radius:12px 12px 0 0'&gt;&lt;h2 style='color:white;margin:0'&gt;⏰ Q2 Estimated Tax Due June 15&lt;/h2&gt;&lt;/div&gt;&lt;div style='background:#f9fafb;padding:24px;border-radius:0 0 12px 12px;border:1px solid #e5e7eb'&gt;&lt;p&gt;Hi {{name}},&lt;/p&gt;&lt;p&gt;Just a friendly reminder that your &lt;strong&gt;Q2 estimated tax payment is due June 15&lt;/strong&gt;.&lt;/p&gt;&lt;p&gt;If you're self-employed or have significant non-W2 income, paying quarterly helps you avoid IRS penalties at year-end.&lt;/p&gt;&lt;p&gt;Log in to your portal to see your estimated tax amount:&lt;/p&gt;&lt;p&gt;&lt;a href='https://ppt-tax-portal.onrender.com/my/tax-estimate' style='background:#11823b;color:white;padding:10px 20px;border-radius:8px;text-decoration:none;font-weight:900'&gt;View My Tax Estimate →&lt;/a&gt;&lt;/p&gt;&lt;p&gt;Need help calculating your quarterly payment? Reply to this email or call 478-338-1632.&lt;/p&gt;&lt;p style='font-size:12px;color:#9ca3af'&gt;Pinnacle Performance Tax | 478-338-1632&lt;/p&gt;&lt;/div&gt;&lt;/div&gt;">
<button type="submit" style="width:100%;padding:12px;text-align:left;background:#fff7ed;color:#9a3412;border:2px solid #fde68a;border-radius:12px;font-weight:900;cursor:pointer">
⏰ Quarterly Tax Reminder
</button></form>

<form method="POST">
<input type="hidden" name="action" value="create">
<input type="hidden" name="name" value="Year-End Tax Planning Strategy Call">
<input type="hidden" name="campaign_type" value="Planning">
<input type="hidden" name="subject" value="💡 Reduce your 2025 tax bill — book a strategy call">
<input type="hidden" name="body" value="&lt;div style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto'&gt;&lt;div style='background:#11823b;padding:20px;border-radius:12px 12px 0 0'&gt;&lt;h2 style='color:white;margin:0'&gt;💡 Year-End Tax Planning&lt;/h2&gt;&lt;/div&gt;&lt;div style='background:#f9fafb;padding:24px;border-radius:0 0 12px 12px;border:1px solid #e5e7eb'&gt;&lt;p&gt;Hi {{name}},&lt;/p&gt;&lt;p&gt;The best time to reduce your tax bill is &lt;strong&gt;before December 31&lt;/strong&gt; — not in April.&lt;/p&gt;&lt;p&gt;Book a year-end tax planning strategy call with us and we'll help you:&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Maximize deductions before year-end&lt;/li&gt;&lt;li&gt;Decide on retirement contributions (SEP IRA, Solo 401k)&lt;/li&gt;&lt;li&gt;Plan for next year's estimated payments&lt;/li&gt;&lt;li&gt;Review your business structure for tax savings&lt;/li&gt;&lt;/ul&gt;&lt;p&gt;&lt;a href='https://ppt-tax-portal.onrender.com/my/book-appointment' style='background:#11823b;color:white;padding:10px 20px;border-radius:8px;text-decoration:none;font-weight:900'&gt;Book Strategy Call →&lt;/a&gt;&lt;/p&gt;&lt;p style='font-size:12px;color:#9ca3af'&gt;Pinnacle Performance Tax | 478-338-1632&lt;/p&gt;&lt;/div&gt;&lt;/div&gt;">
<button type="submit" style="width:100%;padding:12px;text-align:left;background:#f0f9ff;color:#0369a1;border:2px solid #bae6fd;border-radius:12px;font-weight:900;cursor:pointer">
💡 Strategy Call Invite
</button></form>

<form method="POST">
<input type="hidden" name="action" value="create">
<input type="hidden" name="name" value="Returning Client Early-Bird">
<input type="hidden" name="campaign_type" value="Retention">
<input type="hidden" name="subject" value="🎯 Book early for tax season — returning client priority">
<input type="hidden" name="body" value="&lt;div style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto'&gt;&lt;div style='background:#11823b;padding:20px;border-radius:12px 12px 0 0'&gt;&lt;h2 style='color:white;margin:0'&gt;🎯 Reserve Your Spot — Tax Season Is Coming&lt;/h2&gt;&lt;/div&gt;&lt;div style='background:#f9fafb;padding:24px;border-radius:0 0 12px 12px;border:1px solid #e5e7eb'&gt;&lt;p&gt;Hi {{name}},&lt;/p&gt;&lt;p&gt;As a returning client, you get &lt;strong&gt;first access&lt;/strong&gt; to our tax season appointment slots before we open to the public.&lt;/p&gt;&lt;p&gt;Tax season fills up fast — book now to guarantee your preferred time.&lt;/p&gt;&lt;p&gt;&lt;a href='https://ppt-tax-portal.onrender.com/my/book-appointment' style='background:#11823b;color:white;padding:14px 28px;border-radius:10px;text-decoration:none;font-weight:900;font-size:16px'&gt;Reserve My Appointment →&lt;/a&gt;&lt;/p&gt;&lt;p&gt;Log in to your portal to upload documents early and make the process even faster:&lt;/p&gt;&lt;p&gt;&lt;a href='https://ppt-tax-portal.onrender.com' style='color:#11823b'&gt;ppt-tax-portal.onrender.com&lt;/a&gt;&lt;/p&gt;&lt;p style='font-size:12px;color:#9ca3af'&gt;Pinnacle Performance Tax | 478-338-1632 | pinnacleperformancetax@gmail.com&lt;/p&gt;&lt;/div&gt;&lt;/div&gt;">
<button type="submit" style="width:100%;padding:12px;text-align:left;background:#faf5ff;color:#6b21a8;border:2px solid #e9d5ff;border-radius:12px;font-weight:900;cursor:pointer">
🎯 Early-Bird Invite
</button></form>

</div>
<p style="font-size:12px;color:#475569;margin-top:8px">Click any template to save it as a draft, then customize and send below.</p>
</div>

<div class="card">
<h2 style="margin-top:0">Create Custom Campaign</h2>
<form method="POST">
<input type="hidden" name="action" value="create">
<div class="grid grid-3">
<div><label>Campaign Name</label><input type="text" name="name" required placeholder="Summer Bookkeeping Push"></div>
<div><label>Type</label><select name="campaign_type"><option>General</option><option>Bookkeeping</option><option>Quarterly</option><option>Planning</option><option>Retention</option><option>Announcement</option></select></div>
<div><label>Schedule Date (optional)</label><input type="date" name="scheduled_date"></div>
<div style="grid-column:span 3"><label>Subject Line</label><input type="text" name="subject" required placeholder="Subject line your clients will see"></div>
<div style="grid-column:span 3"><label>Email Body (HTML or plain text — use {{name}} for first name)</label><textarea name="body" style="min-height:180px" placeholder="Hi {{name}}, ..."></textarea></div>
<div><button type="submit">Save as Draft</button></div>
</div>
</form>
</div>

{%if campaigns%}
<div class="card">
<h2 style="margin-top:0">{{campaigns|length}} Campaign{{"s"if campaigns|length!=1}}</h2>
{%for c in campaigns%}
<div style="border:1px solid #e5e7eb;border-radius:16px;padding:16px;margin-bottom:14px">
<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:12px">
<div>
<strong style="font-size:15px">{{c.name}}</strong>
<div style="font-size:12px;color:#475569;margin-top:2px">
{{c.campaign_type}} · Created {{c.created_at[:10]}} · Sent to {{c.sent_count}} clients
</div>
<div style="font-size:13px;color:#374151;margin-top:4px">📧 {{c.subject}}</div>
</div>
<div style="display:flex;gap:6px;align-items:center">
<span class="pill{%if c.status=="Sent"%}{%else%} warn{%endif%}">{{c.status}}</span>
<form method="POST" onsubmit="return confirm('Delete campaign?')" style="display:inline">
<input type="hidden" name="action" value="delete">
<input type="hidden" name="campaign_id" value="{{c.id}}">
<button style="padding:4px 10px;font-size:12px;background:#fef2f2;color:#b91c1c;border:0;border-radius:8px;cursor:pointer">Delete</button>
</form>
</div>
</div>
<form method="POST">
<input type="hidden" name="action" value="send_now">
<input type="hidden" name="campaign_id" value="{{c.id}}">
<div style="margin-bottom:10px">
<div style="display:flex;gap:8px;margin-bottom:8px">
<button type="button" onclick="this.closest('form').querySelectorAll('input[name=client_ids]').forEach(x=>x.checked=true)" style="padding:5px 10px;font-size:12px;background:#e8f5ec;color:#0b5f2a;border:0;border-radius:8px;cursor:pointer">Select All</button>
<button type="button" onclick="this.closest('form').querySelectorAll('input[name=client_ids]').forEach(x=>x.checked=false)" style="padding:5px 10px;font-size:12px;background:#f1f5f9;color:#0f172a;border:0;border-radius:8px;cursor:pointer">Clear</button>
</div>
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:6px;max-height:160px;overflow-y:auto;border:1px solid #e5e7eb;border-radius:10px;padding:10px">
{%for cl in clients%}
<label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer">
<input type="checkbox" name="client_ids" value="{{cl.id}}" style="width:auto;margin:0">
<span>{{cl.name}}</span>
</label>
{%endfor%}
</div>
</div>
<button type="submit" style="background:#0b5f2a;padding:10px 20px;font-size:14px">
📧 Send Campaign Now
</button>
</form>
</div>
{%endfor%}
</div>
{%endif%}

{%endblock%}"""


# ── APPOINTMENT REMINDER SMS ──────────────────────────────────
# Send a reminder SMS 24 hours before an appointment.
# Call /send-appointment-reminders daily via UptimeRobot.

@app.route("/send-appointment-reminders")
def send_appointment_reminders():
    """
    Send SMS reminders for appointments happening tomorrow.
    Set up UptimeRobot to hit this URL daily at 9am.
    """
    token = request.args.get("token", "")
    expected = os.environ.get("POLL_TOKEN", "ppt2025")
    if token != expected:
        return "Unauthorized", 403

    from datetime import timedelta
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    appointments = query_db(
        """SELECT a.*,c.name client_name,c.phone client_phone,c.email client_email
           FROM appointments a LEFT JOIN clients c ON c.id=a.client_id
           WHERE a.status IN ('Scheduled','Approved','Confirmed')
           AND substr(a.start_at,1,10)=?""",
        (tomorrow,)
    )

    sent = 0
    for appt in appointments:
        phone = appt["client_phone"]
        email = appt["client_email"]
        name  = (appt["client_name"] or "").split()[0] or "there"
        time  = appt["start_at"]
        title = appt["title"] or "Tax Appointment"

        if phone:
            send_sms(
                phone,
                f"Hi {name}! Reminder: you have a {title} "
                f"tomorrow at {time}. "
                f"Questions? Call 478-338-1632. "
                f"— Pinnacle Performance Tax"
            )
            sent += 1

        if email:
            send_email(
                email,
                f"Appointment Reminder — {title} Tomorrow",
                f"""<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
                <div style="background:#11823b;padding:20px;border-radius:12px 12px 0 0">
                  <h2 style="color:white;margin:0">📅 Appointment Reminder</h2>
                </div>
                <div style="background:#f9fafb;padding:24px;border-radius:0 0 12px 12px;
                            border:1px solid #e5e7eb">
                  <p>Hi {name},</p>
                  <p>Just a reminder about your appointment <strong>tomorrow</strong>:</p>
                  <div style="background:white;border-radius:10px;padding:16px;margin:16px 0;
                              border:1px solid #e5e7eb">
                    <strong>{title}</strong><br>
                    🕐 {time}<br>
                    📍 {appt['location'] or 'Pinnacle Performance Tax'}
                  </div>
                  <p>Please have your documents ready. You can upload them now in your portal:</p>
                  <p><a href="https://ppt-tax-portal.onrender.com/my/documents"
                        style="background:#11823b;color:white;padding:10px 20px;
                               border-radius:8px;text-decoration:none;font-weight:900">
                    Upload Documents →</a></p>
                  <p style="font-size:12px;color:#9ca3af">
                    Pinnacle Performance Tax | 478-338-1632
                  </p>
                </div></div>"""
            )

    return _json.dumps({"sent": sent, "date": tomorrow}), 200, \
           {"Content-Type": "application/json"}


# ── TEST ENDPOINTS (ADMIN ONLY) ───────────────────────────────

@app.route("/admin/test-sms", methods=["GET", "POST"])
@login_required
@admin_required
def test_sms():
    """Test your SMS integration."""
    sent = None
    if request.method == "POST":
        phone = request.form.get("phone", "")
        msg   = request.form.get("message", "Test from Pinnacle Performance Tax PPT Portal!")
        sent  = send_sms(phone, msg)
        flash(f"SMS {'sent successfully ✅' if sent else 'FAILED ❌ — check TWILIO env vars'}", 
              "success" if sent else "danger")
    return render_template_string("""{%extends"base.html"%}{%block content%}
<h1>📱 Test SMS</h1>
<div class="card" style="max-width:500px">
<form method="POST">
<div class="grid">
<div><label>Phone Number</label>
<input type="tel" name="phone" placeholder="+14785551234" required></div>
<div><label>Message</label>
<textarea name="message">Test from Pinnacle Performance Tax!</textarea></div>
<div><button type="submit">Send Test SMS</button></div>
</div>
</form>
<div style="margin-top:20px;font-size:13px;color:#475569">
<strong>Required env vars in Render:</strong><br>
• TWILIO_ACCOUNT_SID<br>
• TWILIO_AUTH_TOKEN<br>
• TWILIO_PHONE (your Twilio number e.g. +14785550100)<br>
• BUSINESS_PHONE (your real number e.g. +14783381632)<br>
• POLL_TOKEN (secret token for cron jobs, e.g. ppt2025)
</div>
</div>{%endblock%}""")


@app.route("/admin/test-missed-call")
@login_required
@admin_required
def test_missed_call():
    """Simulate a missed call to test the text-back system."""
    with app.test_request_context(
        "/webhook/missed-call", method="POST",
        json={
            "caller_name":  "Test Caller",
            "caller_phone": os.environ.get("BUSINESS_PHONE", ""),
            "notes":        "This is a test missed call"
        }
    ):
        result = missed_call_webhook()
    flash("Missed-call test triggered! Check your phone for the text-back.", "success")
    return redirect(url_for("dashboard"))


# ── ADD CAMPAIGN LINK TO SIDEBAR ──────────────────────────────
# In your base.html sidebar, add this link under the Tools section:
#   <a href="/admin/campaigns">📧 Email Campaigns</a>
#   <a href="/admin/test-sms">📱 Test SMS</a>
#   <a href="/send-appointment-reminders?token=ppt2025">⏰ Send Reminders</a>

# ── RENDER ENVIRONMENT VARIABLES SETUP ───────────────────────
# Go to your Render dashboard → PPT Portal service → Environment
# Add these variables:
#
# TWILIO_ACCOUNT_SID   → from twilio.com/console (free account)
# TWILIO_AUTH_TOKEN    → from twilio.com/console
# TWILIO_PHONE         → your Twilio number e.g. +14785550100
# BUSINESS_PHONE       → +14783381632
# SENDGRID_API_KEY     → already set
# ADMIN_EMAIL          → pinnacleperformancetax@gmail.com
# POLL_TOKEN           → ppt2025 (or any secret word you choose)
# GOOGLE_VOICE_EMAIL   → your Gmail address (optional, for auto-polling)
# GOOGLE_VOICE_APP_PW  → Gmail App Password (optional)
#
# ── UPTIMEROBOT FREE SETUP (replaces a paid cron service) ────
# Go to uptimerobot.com → Create monitors:
#
# Monitor 1: Appointment Reminders (daily)
#   URL: https://ppt-tax-portal.onrender.com/send-appointment-reminders?token=ppt2025
#   Interval: Every 24 hours
#
# Monitor 2: Missed-Call Polling (every 5 min, optional)
#   URL: https://ppt-tax-portal.onrender.com/poll-missed-calls?token=ppt2025
#   Interval: Every 5 minutes
#
# ── SETMORE WEBHOOK SETUP ────────────────────────────────────
# 1. Log into Setmore
# 2. Go to Apps & Integrations → Webhooks
# 3. Click Add New Webhook
# 4. URL: https://ppt-tax-portal.onrender.com/webhook/setmore
# 5. Events: booking_created, booking_updated, booking_cancelled
# 6. Save
#
# ── GOOGLE VOICE MISSED-CALL SETUP (Option B — Zapier free) ──
# 1. Go to zapier.com → Create Zap
# 2. Trigger: Gmail → New Email Matching Search
#    Search: from:voice-noreply@google.com subject:"Missed call"
# 3. Action: Webhooks by Zapier → POST
#    URL: https://ppt-tax-portal.onrender.com/webhook/missed-call
#    Payload type: JSON
#    Data: { "caller_name": "<parsed from email>",
#            "caller_phone": "<parsed from email>" }
# Free Zapier gives you 100 tasks/month which is plenty.
# ────────────────────────────────────────────────────────────
