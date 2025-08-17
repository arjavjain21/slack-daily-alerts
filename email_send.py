import os
import smtplib
import psycopg
from email.message import EmailMessage
from decimal import Decimal

# ---------- Config ----------
DSN = os.getenv("SUPABASE_DB_URL")
SMTP_HOST = os.getenv("EMAIL_SMTP_HOST")
SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
SMTP_USER = os.getenv("EMAIL_USERNAME")
SMTP_PASS = os.getenv("EMAIL_PASSWORD")
FROM_EMAIL = os.getenv("EMAIL_FROM")
FROM_NAME  = os.getenv("EMAIL_FROM_NAME", "EIS Campaign Updates")
TO_LIST    = [e.strip() for e in os.getenv("EMAIL_TO", "").split(",") if e.strip()]
CC_LIST    = [e.strip() for e in os.getenv("EMAIL_CC", "").split(",") if e.strip()]

LOW_LEADS_THRESHOLD = int(os.getenv("LOW_LEADS_THRESHOLD", "250"))
LOW_REPLY_COUNT_THRESHOLD = int(os.getenv("LOW_REPLY_COUNT_THRESHOLD", "5"))

if not (DSN and SMTP_HOST and SMTP_USER and SMTP_PASS and FROM_EMAIL and TO_LIST):
    raise SystemExit("Missing one or more required env vars: SUPABASE_DB_URL, EMAIL_SMTP_HOST, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_FROM, EMAIL_TO")

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
HAVING SUM(total_sent) > 0           -- ACTIVE only
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

    # Match n8n: Reply Rate = positives / sent
    reply_rate_pct = f"{(Decimal(total_pos) / total_sent * 100).quantize(Decimal('0.1'))}%" if total_sent else "0.0%"

    # Lists to match n8n HTML
    leads_alerts   = sorted([(r["client_name"], r["leads"]) for r in rows if r["leads"] < LOW_LEADS_THRESHOLD], key=lambda x: (x[1], x[0]))
    replies_alerts = sorted([(r["client_name"], r["replies"]) for r in rows if r["replies"] <= LOW_REPLY_COUNT_THRESHOLD and r["client_name"]], key=lambda x: (x[1], x[0]))
    positives_alerts = sorted([(r["client_name"], r["positives"]) for r in rows if r["positives"] == 0 and r["client_name"]], key=lambda x: x[0])

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

def build_plaintext(html):
    # Simple fallback body
    return "Daily Campaign Alerts. Open in an HTML-capable mail client."

def send_email(subject, html_body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = ", ".join(TO_LIST)
    if CC_LIST:
        msg["Cc"] = ", ".join(CC_LIST)
    msg.set_content(build_plaintext(html_body))
    msg.add_alternative(html_body, subtype="html")

    recipients = TO_LIST + CC_LIST
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg, from_addr=FROM_EMAIL, to_addrs=recipients)
        print(f"Sent email to {len(recipients)} recipients.")

def main():
    _, rows = fetch_rows()
    html, subject = build_html(rows)
    send_email(subject, html)

if __name__ == "__main__":
    main()
