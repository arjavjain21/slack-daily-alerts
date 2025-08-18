import os
import psycopg
import requests
from decimal import Decimal

DSN = os.getenv("SUPABASE_DB_URL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")

if not (DSN and SLACK_TOKEN and SLACK_CHANNEL):
    raise SystemExit("Missing env vars: SUPABASE_DB_URL / SLACK_BOT_TOKEN / SLACK_CHANNEL_ID")

def parse_int_env(name: str, default_value: int) -> int:
    raw = os.getenv(name, "")
    if not raw or not raw.strip():
        return default_value
    raw = raw.strip()
    # tolerate "250", "250.0", "250 leads"
    try:
        val = int(Decimal(raw))
        return val if val >= 0 else default_value
    except Exception:
        digits = "".join(ch for ch in raw if ch.isdigit())
        try:
            return int(digits) if digits else default_value
        except Exception:
            return default_value

LOW_LEADS_THRESHOLD = parse_int_env("LOW_LEADS_THRESHOLD", 250)
REPLIES_ALERT_THRESHOLD = parse_int_env("REPLIES_ALERT_THRESHOLD", 5)

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
    cr.bounce_count,
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
  SUM(positive_reply)    AS positives,
  SUM(bounce_count)      AS bounces
FROM base
GROUP BY ist_yday, client_name
HAVING SUM(total_sent) > 0
ORDER BY client_name;
"""

def fetch_rows():
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def safe_div(num: int, den: int) -> Decimal:
    if not den:
        return Decimal(0)
    return Decimal(num) / Decimal(den)

def d2pct(x: Decimal) -> str:
    try:
        return f"{(x * 100).quantize(Decimal('0.1'))}%"
    except Exception:
        return "0.0%"

def build_message(rows):
    if not rows:
        return [
            {"type": "section", "text": {"type": "mrkdwn", "text": "*❗ Daily Campaign Alerts: no data for IST yesterday*"}}
        ]

    ist_date = rows[0]["ist_yday"]
    total_sent = sum(r["sent"] for r in rows)
    total_leads = sum(r["leads"] for r in rows)
    total_replies = sum(r["replies"] for r in rows)
    total_positives = sum(r["positives"] for r in rows)
    overall_reply_rate = safe_div(total_positives, total_sent)  # n8n parity

    low_leads = sorted([(r["client_name"], r["leads"]) for r in rows if r["leads"] < LOW_LEADS_THRESHOLD],
                       key=lambda x: (x[1], x[0]))
    low_replies = sorted([(r["client_name"], r["replies"]) for r in rows if r["replies"] <= REPLIES_ALERT_THRESHOLD],
                         key=lambda x: (x[1], x[0]))
    zero_positive = sorted([(r["client_name"], r["positives"]) for r in rows if r["positives"] == 0],
                           key=lambda x: x[0])

    header = {
      "type": "section",
      "text": {"type": "mrkdwn", "text": f"*❗ Daily Campaign Alerts: {ist_date}*"}
    }

    summary_lines = [
        ":bar_chart: Yesterday’s Summary",
        f"• New Leads: {total_leads}",
        f"• Replies: {total_replies}",
        f"• Positive Replies: {total_positives}",
        f"• Overall Reply Rate: {d2pct(overall_reply_rate)}",
    ]
    summary_block = {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(summary_lines)}}

    def bullets(items, fmt):
        if not items:
            return "• None"
        return "\n".join(fmt(it) for it in items)

    low_leads_block = {
      "type": "section",
      "text": {"type": "mrkdwn",
               "text": f":chart_with_downwards_trend: Accounts with < {LOW_LEADS_THRESHOLD} New Leads Contacted Yesterday\n"
                       + bullets(low_leads, lambda x: f"• {x[0]}: {x[1]} Leads")}
    }

    low_replies_block = {
      "type": "section",
      "text": {"type": "mrkdwn",
               "text": f"🗨️ Accounts with ≤ {REPLIES_ALERT_THRESHOLD} Replies\n"
                       + bullets(low_replies, lambda x: f"• {x[0]}: {x[1]} Replies")}
    }

    zero_pos_block = {
      "type": "section",
      "text": {"type": "mrkdwn",
               "text": ":rotating_light: Accounts with 0 Positive Replies from Yesterday\n"
                       + bullets(zero_positive, lambda x: f"• {x[0]}: 0 Positive Replies")}
    }

    return [header, summary_block, low_leads_block, low_replies_block, zero_pos_block]

def post_to_slack(blocks):
    headers = {
      "Authorization": f"Bearer {SLACK_TOKEN}",
      "Content-Type": "application/json; charset=utf-8"
    }
    payload = {"channel": SLACK_CHANNEL, "blocks": blocks, "text": "Daily Campaign Alerts"}
    resp = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload, timeout=30)
    print("Slack response:", resp.status_code, resp.text)
    resp.raise_for_status()

def main():
    rows = fetch_rows()
    blocks = build_message(rows)
    post_to_slack(blocks)

if __name__ == "__main__":
    main()
