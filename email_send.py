import os
import ssl
import smtplib
import psycopg
from email.message import EmailMessage
from decimal import Decimal, InvalidOperation

# ---------- Required config from GitHub Actions secrets ----------
DSN = (os.getenv("SUPABASE_DB_URL") or "").strip()
SMTP_HOST = (os.getenv("EMAIL_SMTP_HOST") or "").strip()           # e.g., smtp.gmail.com or SES host
SMTP_PORT = int((os.getenv("EMAIL_SMTP_PORT") or "587").strip())   # 587 for STARTTLS is typical
SMTP_USER = (os.getenv("EMAIL_USERNAME") or "").strip()
SMTP_PASS = (os.getenv("EMAIL_PASSWORD") or "").strip()
FROM_EMAIL = (os.getenv("EMAIL_FROM") or "").strip()
FROM_NAME  = (os.getenv("EMAIL_FROM_NAME") or "EIS Campaign Updates").strip()

TO_LIST = [e.strip() for e in (os.getenv("EMAIL_TO") or "").split(",") if e.strip()]
CC_LIST = [e.strip() for e in (os.getenv("EMAIL_CC") or "").split(",") if e.strip()]

# ---------- Threshold parsing with safety ----------
def parse_int_env(name: str, default_value: int) -> int:
    raw = os.getenv(name, "")
    if not raw or not raw.strip():
        return default_value
    s = raw.strip()
    try:
        # Accept "250" or "250.0" or "250 leads"
        val = int(Decimal(s))
        return val if val >= 0 else default_value
    except (InvalidOperation, ValueError):
        digits = "".join(ch for ch in s if ch.isdigit())
        if digits:
            try:
                val = int(digits)
                return val if val >= 0 else default_value
            except Exception:
                pass
        print(f"Invalid {name}='{raw}', falling back to {default_value}")
        return default_value

LOW_LEADS_THRESHOLD = parse_int_env("LOW_LEADS_THRESHOLD", 250)           # matches n8n
LOW_REPLY_COUNT_THRESHOLD = parse_int_env("LOW_REPLY_COUNT_THRESHOLD", 5)  # matches n8n

if not (DSN and SMTP_HOST and SMTP_USER and SMTP_PASS and FROM_EMAIL and TO_LIST):
    raise SystemExit("Missing one or more required env vars: SUPABASE_DB_URL, EMAIL_SMTP_HOST, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_FROM, EMAIL_TO")

print(f"Using thresholds: LOW_LEADS_THRESHOLD={LOW_LEADS_THRESHOLD}, LOW_REPLY_COUNT_THRESHOLD={LOW_REPLY_COUNT_THRESHOLD}")

# ---------- SQL: IST-yesterday, ACTIVE only (sent > 0), group per client ----------
SQL = """
WITH ist_yesterday AS (
  SELECT ((now() AT TIME ZONE 'Asia/Kolkata')::date - 1) AS d
),
base AS (
  SELECT
    cr.client_name,
    cr.total_sent,
    cr.new_leads_reached,
    cr.replies_count,
    cr.positive_reply,
    (now() AT TIME ZONE 'Asia/Kolkata')::date - 1 AS ist_yday
  FROM public.campaign_reporting cr
  JOIN ist_yesterday y ON cr.start_date = y.d
)
SELECT
  ist_yday,
  client_name,
  SUM(total_sent)        AS sent,
  SUM(new_leads_reached) AS leads,
  SUM(replies_count)     AS replies,
  SUM(positive_reply)    AS positives
FROM base
GROUP BY ist_yday, client_name
HAVING SUM(total_sent) > 0         -- ACTIVE only
ORDER BY client_name;
"""

def fetch_rows():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    return cols, [dict(zip(cols, r)) for r in rows]

def build_html(rows):
    if not rows:
        return "<p>No data for IST yesterday.</p>", "Daily Campaign Alerts"

    ist_date = rows[0]["ist_yday"]

    total_sent = sum(r["sent"] for r in rows)
    total_leads = sum(r["leads"] for r in rows)
    total_replies = sum(r["replies"] for r in rows)
    total_pos = sum(r["positives"] for r in rows)

    # n8n parity: Reply Rate in email summary = positives / sent
    if total_sent:
        reply_rate_pct = f"{(Decimal(total_pos) / Decimal(total_sent) * 100).quantize(Decimal('0.1'))}%"
    else:
        reply_rate_pct = "0.0%"

    # Alert sections as per n8n
    leads_alerts = sorted(
        [(r["client_name"], r["leads"]) for r in rows if r["client_name"] and r["leads"] < LOW_LEADS_THRESHOLD],
        key=lambda x: (x[1], x[0])
    )
    replies_alerts = sorted(
        [(r["client_name"], r["replies"]) for r in rows if r["client_name"] and r["replies"] <= LOW_REPLY_COUNT_THRESHOLD],
        key=lambda x: (x[1], x[0])
    )
    positives_alerts = sorted(
        [(r["client_name"], r["positives"]) for r in rows if r["client_name"] and r["positives"] == 0],
        key=lambda x: x[0]
    )

    def render_list(items, label):
        if not items:
            return '<p style="font-size:14px;margin:0 0 8px;">✅ None</p>'
        li = "\n".join([f"<li><strong>{name}</strong>: {val} {label}</li>" for name, val in items])
        return f'<ul style="padding-left:20px;margin:0;font-size:14px;line-height:1.6;">{li}</ul>'

    html = f"""\
<div style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:0 auto;line-height:1.6;">
  <div style="background:#0052CC;color:#fff;padding:16px;border-radius:4px;">
    <h2 style="margin:0;font-size:20px;">⚠️ Daily Campaign Alerts</h2>
    <p style="margin:4px 0 0;font-size:14px;"><strong>Date:</strong> {ist_date}</p>
  </div>

  <div style="padding:16px;background:#f0f4ff;border-radius:4px;margin-top:8px;">
    <h3 style="margin:0 0 8px;font-size:16px;">📊 Yesterday’s Summary</h3>
    <ul style="padding-left:20px;margin:0;font-size:14px;">
      <li>Total Emails Sent: {total_sent}</li>
      <li>New Leads: {total_leads}</li>
      <li>Replies: {total_replies}</li>
      <li>Positive Replies: {total_pos}</li>
      <li>Reply Rate: {reply_rate_pct}</li>
    </ul>
  </div>

  <div style="padding:16px;background:#fff3cd;border-radius:4px;margin-top:12px;">
    <h3 style="margin:0 0 8px;font-size:16px;color:#856404;">📉 Accounts with &lt; {LOW_LEADS_THRESHOLD} New Leads Contacted</h3>
    {render_list(leads_alerts, "Leads")}
  </div>

  <div style="padding:16px;background:#f1dfd1;border-radius:4px;margin-top:12px;">
    <h3 style="margin:0 0 8px;font-size:16px;color:#602f0c;">🗨️ Accounts with ≤ {LOW_REPLY_COUNT_THRESHOLD} Replies</h3>
    {render_list(replies_alerts, "Replies")}
  </div>

  <div style="padding:16px;background:#f8d7da;border-radius:4px;margin-top:12px;">
    <h3 style="margin:0 0 8px;font-size:16px;color:#721c24;">🚨 Accounts with 0 Positive Replies</h3>
    {render_list(positives_alerts, "Positive Replies")}
  </div>

  <p style="font-size:12px;color:#666;margin-top:12px;">
    Automated notification, please do not reply.
  </p>
</div>
"""
    subject = f"⚠️ Daily Campaign Alerts: {ist_date}"
    return html, subject

def build_plaintext():
    return "Daily Campaign Alerts. Open in an HTML-capable mail client."

def smtp_connect():
    ctx = ssl.create_default_context()
    # Try STARTTLS on configured port first
    try:
        s = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        s.ehlo()
        s.starttls(context=ctx)
        s.ehlo()
        s.login(SMTP_USER, SMTP_PASS)
        print(f"SMTP connected with STARTTLS on {SMTP_HOST}:{SMTP_PORT}")
        return s
    except Exception as e1:
        print(f"STARTTLS failed on {SMTP_HOST}:{SMTP_PORT}: {e1}")
        # Try SSL on 465
        try:
            s = smtplib.SMTP_SSL(SMTP_HOST, 465, timeout=30, context=ctx)
            s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            print(f"SMTP connected with SSL on {SMTP_HOST}:465")
            return s
        except Exception as e2:
            raise SystemExit(f"SMTP connection failed. Tried STARTTLS {SMTP_PORT} then SSL 465. Errors: {e1} | {e2}")

def send_email(subject, html_body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = ", ".join(TO_LIST)
    if CC_LIST:
        msg["Cc"] = ", ".join(CC_LIST)

    msg.set_content(build_plaintext())
    msg.add_alternative(html_body, subtype="html")

    recipients = TO_LIST + CC_LIST
    with smtp_connect() as s:
        s.send_message(msg, from_addr=FROM_EMAIL, to_addrs=recipients)
        print(f"Sent email to {len(recipients)} recipients.")

def main():
    _, rows = fetch_rows()
    html, subject = build_html(rows)
    send_email(subject, html)

if __name__ == "__main__":
    main()
